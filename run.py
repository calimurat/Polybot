"""
RUN.PY — MAIN ENTRY POINT & ORCHESTRATOR
Initializes all modules, runs bot lifecycle, handles errors & shutdown

Production-grade: full integration, logging, error recovery, cleanup
"""

import logging
import re
from logging.handlers import RotatingFileHandler
import sys
import os
import signal
import subprocess
import time
import json
import threading
import fcntl
from typing import Optional
from datetime import datetime

# Import all modules
from config import Config
from polymarket_production_bot import PolymarketBot, BotConfig
from external_connections import ExternalDataCoordinator, ExternalConfig, ResolutionWatcher
from feed_lag import BINANCE_FEED_LAG, POLYMARKET_FEED_LAG
from wallet_manager import WalletManager, WalletConfig
from database import Database, MarketSession
from position_manager import PositionManager
from shadow import ShadowRecorder
from perp_shadow import PerpMomentumShadow
from signal_engine import (
    Signal, MultiFactorSignalEngine, MultiFactorConfig,
)
from market_loader import MarketLoader


def _fmt(value: Optional[float], decimals: int = 3) -> str:
    """Format an Optional[float] for log lines — 'n/a' instead of blowing
    up on None (Phase 2 diagnostics: several of these values legitimately
    aren't available yet, e.g. no trades seen, TWAP not flowing)."""
    return f"{value:.{decimals}f}" if value is not None else "n/a"

# Ranks a cycle's rejection reason by how far it got before being
# rejected, so the market_sessions row keeps the single MOST INFORMATIVE
# reason seen during the cycle rather than whichever tick happened to run
# last. Without the ranking, a cycle's row almost always ends up saying
# "outside trading window" — that is simply what every tick reports once
# seconds_remaining drops below the direction's minimum, and those are the
# last ticks of the window.
#
# Deepest first. The stages, in order of how much has to go right to
# reach them:
#
#   RISK_PAUSED     a circuit breaker stopped the cycle before any signal
#                   was even evaluated. Ranked top because it makes the
#                   whole cycle non-tradeable: the row must not read as
#                   "the market offered nothing" when the truth is the bot
#                   refused to trade.
#   execution:      a signal PASSED every gate and then died on the way to
#                   an order — slippage, book depth, execution latency.
#                   This is the stage that hid the single worst bug in
#                   this bot: 16,946 signals were rejected on an
#                   unreachable slippage limit over one session while the
#                   database recorded those cycles as plain NO_SIGNAL,
#                   which is why nobody noticed for weeks. Anything
#                   recorded here is a near-miss and the most actionable
#                   thing a cycle can report.
#   edge/ask/...    MultiFactorSignalEngine.evaluate()'s own gates, in
#                   pipeline order (see that method).
#
# "spread" used to sit at the top of this tuple; that gate was deleted
# from the engine on 2026-09-22, so it could never match again. Replaced
# by the execution-stage marker, which is what actually occupies that
# position now.
_REASON_DEPTH_ORDER = (
    "RISK_PAUSED",
    "execution:",
    "edge",
    "ask",
    "model_prob",
    "insufficient book depth",
    "no valid book",
    "outside trading window",
    "no directional signal",
    "market status",
    # Shallowest on purpose. When the opening price is missing the whole
    # cycle reports this and nothing competes with it, so it still gets
    # recorded; but when only the occasional tick lacks a current price,
    # any real gate reason from the ticks that DID have one is the more
    # useful thing to keep.
    "no BTC price",
)


def _reason_depth(reason: Optional[str]) -> int:
    """Higher = got further before being rejected, i.e. more informative."""
    if not reason:
        return -1
    for i, marker in enumerate(_REASON_DEPTH_ORDER):
        if marker in reason:
            return len(_REASON_DEPTH_ORDER) - i
    return 0

# ============================================================================
# LOGGING SETUP
# ============================================================================

class _ConsoleNoiseFilter(logging.Filter):
    """
    Keeps the SCREEN readable. bot.log still gets every record.

    Measured on 2026-09-30, one busy 5-minute window: 643 lines, two of them
    the [CYCLE] banner. The rest was the dashboard's HTTP access lines (198,
    one per poll), "[WS] Live ... failed verification" (186) and the same
    "[LOOP] ...: fill=" / "... skipping trade" pair on every tick while a
    signal sat behind a feed-lag guard (196). At two lines a second the
    banner scrolled off within a minute, less once a storm started.

    Dashboard access lines stay off the screen. The other repeats show at
    most once per `interval` seconds per distinct message (digits ignored,
    so 1958ms and 6496ms are one message). ERROR and above always show.
    """

    _REPEATS = re.compile(r"failed verification|skipping trade|\[LOOP\] \w+: fill=")
    _NUMBERS = re.compile(r"-?\d+(?:\.\d+)?")

    def __init__(self, interval: float = 30.0, clock=time.monotonic):
        super().__init__()
        self.interval = interval
        self.clock = clock
        self._shown = {}

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR:
            return True
        if record.name == "Dashboard" and record.levelno <= logging.INFO:
            return False
        message = record.getMessage()
        if not self._REPEATS.search(message):
            return True
        key = self._NUMBERS.sub("#", message)
        now = self.clock()
        last = self._shown.get(key)
        if last is not None and now - last < self.interval:
            return False
        if len(self._shown) > 500:
            self._shown.clear()
        self._shown[key] = now
        return True


def setup_logging(config: Config):
    """
    Set up logging for all modules, with a SIZE-BOUNDED log file.

    This used to be a plain FileHandler, i.e. a file that grows forever.
    Measured on the 2026-09-22 session: 5.11 MB over 4.08 hours — about
    1.3 MB/hour, 30 MB/day — while the bot opened zero positions. This
    runs continuously by design (one 5-minute market after another,
    forever), so "forever" is the operative word: nothing ever truncated
    or rotated it.

    Rotation caps total log usage at log_max_bytes * (log_backup_count + 1)
    — roughly 110 MB by default, which at the measured rate is several
    days of history. Old segments are dropped oldest-first rather than the
    file being allowed to fill the disk out from under a running bot.
    """

    file_handler = RotatingFileHandler(
        config.log_file,
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )

    # force=True: polymarket_production_bot configures the root logger
    # itself at import time (a plain FileHandler on bot.log), and run.py
    # imports it at the top, so without force this call was a no-op — the
    # rotating handler was never attached, bot.log grew past 33 MB, and
    # config.log_level was ignored.
    console = logging.StreamHandler()
    console.addFilter(_ConsoleNoiseFilter())

    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format=config.log_format,
        handlers=[
            file_handler,
            console
        ],
        force=True,
    )

    # websocket-client logs "<error> - goodbye" whenever run_forever()
    # ends on an error — right after handing that same error to the app's
    # on_error, which every WebSocketApp here already logs with context.
    # Those duplicates were ~20,000 of bot.log's lines. Its other messages
    # (notably "error from callback", i.e. a bug in one of our handlers)
    # still come through.
    logging.getLogger("websocket").addFilter(
        lambda record: not record.getMessage().endswith(" - goodbye")
    )

    logger = logging.getLogger("Bot")
    logger.info("="*80)
    logger.info("POLYMARKET REAL TRADING BOT — MAIN ENTRY POINT")
    logger.info("="*80)
    
    return logger

# ============================================================================
# BOT CONTROLLER
# ============================================================================

class BotController:
    """
    Main bot orchestrator
    
    Manages:
    - Module initialization
    - Trading loop
    - Error handling
    - Graceful shutdown
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("BotController")
        
        # Modules (will be initialized in run())
        self.polymarket = None
        self.external = None
        self.wallet = None
        self.db = None
        self.positions = None
        self.btc_signal_engine = None
        self.market_loader = None
        self.resolution_watcher = None
        self.shadow = None  # ShadowRecorder, see shadow.py
        self.perp_shadow = None  # PerpMomentumShadow, see perp_shadow.py
        self.dashboard_server = None
        # market_recorder.py child process (Config.record_market_tape), and
        # whether "another recorder is already running" has been logged
        # (said once; the bot keeps retrying quietly so it takes over the
        # tape as soon as that other recorder stops).
        self.market_recorder: Optional[subprocess.Popen] = None
        self._market_recorder_elsewhere = False
        # Background thread that looks up official resolutions for
        # positions a previous run left open — see
        # _settle_and_recover_orphans(). Tracked so _cleanup() can stop it:
        # it makes HTTP calls and writes to the database, so it must not
        # outlive the database it writes to.
        self._orphan_recovery_thread: Optional[threading.Thread] = None
        self._orphan_recovery_stop = threading.Event()

        # State
        self.running = False
        self.market_open = False
        self.market_data = None
        # Why the current market must not be traded, or None — see
        # _market_trade_blocker(). Set whenever market_data is.
        self._market_blocker: Optional[str] = None
        self.session_id = None
        self.btc_opening_price: Optional[float] = None

        # Pre-warming: the next cycle's market + PolymarketBot, connected
        # and subscribed BEFORE the current cycle ends, so there's live
        # data flowing the instant we roll over instead of a ~3.5s reactive
        # discover-then-connect gap every single cycle.
        self.next_market_data = None
        self.next_polymarket = None
        self._prewarm_attempted = False
        self._late_pickup = False
        # True when self.polymarket still needs start_market() called on it
        # (a freshly-created, not-yet-started bot); False when it was
        # promoted from next_polymarket, which is already running.
        self._polymarket_needs_start = True

        # Event-driven tick dispatch (replaces the old fixed 1s poll) — see
        # _run_trading_loop()/_evaluator_loop()/_on_market_tick(). A single
        # dedicated evaluator thread wakes on this Event, which
        # BinanceSpot.on_price_update / PolymarketBot.on_book_update set
        # from their own WS callback threads. Event.set() coalesces any
        # number of ticks that arrive while the evaluator is still busy
        # with the previous one into a single extra evaluation right
        # after — real signal evaluation is never run more than once at a
        # time, however fast the underlying feeds burst.
        self._tick_ready = threading.Event()
        self._loop_active = False
        self._evaluator_thread: Optional[threading.Thread] = None
        self._loop_start: Optional[float] = None
        self._loop_end: Optional[float] = None
        # Bumped once per _run_trading_loop() call — see that method and
        # _evaluator_loop()/_on_market_tick() for why a stale evaluator
        # thread (one whose join() timed out) needs this, not just
        # _loop_active, to reliably stop itself.
        self._active_generation = 0

        # Wall-clock throttles for log lines that used to key off
        # int(elapsed) % 30 == 0 in the old once-a-second loop — that
        # trick assumed at most one tick per second, which no longer
        # holds now that a tick can fire hundreds of times a second
        # during a book-event burst. Timestamp-based throttling keeps the
        # same "about every 30s" cadence regardless of tick rate.
        self._last_nosignal_log_time = 0.0
        self._last_progress_log_time = 0.0

        # Statistics
        self.session_stats = {
            "signals_detected": 0,
            "trades_executed": 0,
            "positions_closed": 0,
            "total_pnl": 0.0,
            "start_time": None,
            "end_time": None
        }
        
        # Setup signal handlers (graceful shutdown)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("[BOT] Controller initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.warning("[BOT] Shutdown signal received")
        self.running = False
    
    # ========================================================================
    # INITIALIZATION
    # ========================================================================
    
    def _init_modules(self) -> bool:
        """Initialize all modules"""
        
        try:
            self.logger.info("[BOT] Initializing modules...")
            
            # Database
            self.db = Database(self.config.db_file)
            self.logger.info("[BOT] ✓ Database")
            
            # External connections (Binance)
            external_config = ExternalConfig()
            self.external = ExternalDataCoordinator(external_config)
            self.external.start()
            self.logger.info("[BOT] ✓ External connections")
            
            # Wallet
            wallet_config = WalletConfig(
                private_key=self.config.private_key,
                wallet_type="metamask" if self.config.signature_type == 0 else "magic_link",
                signature_type=self.config.signature_type,
                chain_id=self.config.chain_id
            )
            self.wallet = WalletManager(wallet_config)
            self.logger.info(f"[BOT] ✓ Wallet ({self.wallet.get_address()[:10]}...)")
            
            # Polymarket CLOB
            # NOTE: We do NOT create the PolymarketBot instance here.
            # A new one is created for each 5-minute market cycle in
            # _load_market(), because the WS subscription needs the
            # fresh token_yes_id/token_no_id for that specific window.
            self.polymarket = None
            self.logger.info("[BOT] ✓ Polymarket CLOB (created per cycle)")
            
            # Position manager
            self.positions = PositionManager(self.db)
            self.logger.info("[BOT] ✓ Position manager")
            
            # NOTE: SignalEngine (the older mean-reversion-on-token-price
            # strategy) is deliberately NOT created here. It was still
            # being constructed and reported as "✓ Signal engine" at
            # startup, and its stats were printed at shutdown, but nothing
            # ever called calculate_signal() on it — so those stats were
            # always zeros and the startup line implied a component that
            # was doing nothing. MultiFactorSignalEngine below is the only
            # signal path; the class itself was removed from
            # signal_engine.py on 2026-09-29.

            # Multi-factor signal — price-to-beat distance (real BTC/USD vs.
            # the window's opening price, volatility-normalized) combined
            # with momentum, TWAP structure, order-book imbalance, trade
            # flow, and market-quality/time gates into one scored decision.
            # This is what actually drives trade decisions now. Rule-based
            # throughout — no probabilistic model, no training data
            # pipeline; weights in MultiFactorConfig are a starting point
            # to tune against bot_trades.db history.
            multi_config = MultiFactorConfig()

            # The taker fee rate lives in two places — the signal engine
            # subtracts it when deciding whether an edge is big enough, and
            # PositionManager charges it when a position actually opens.
            # Their comments say they must match; nothing made them. If
            # they drift, the bot trades on an edge computed against one
            # fee and books P&L against another, and neither number is
            # wrong enough to look wrong. Fail at startup instead.
            if multi_config.taker_fee_rate != PositionManager.TAKER_FEE_RATE_CRYPTO:
                raise ValueError(
                    f"Taker fee rate mismatch: MultiFactorConfig.taker_fee_rate="
                    f"{multi_config.taker_fee_rate} but "
                    f"PositionManager.TAKER_FEE_RATE_CRYPTO="
                    f"{PositionManager.TAKER_FEE_RATE_CRYPTO}. The edge gate and "
                    f"the actual charge must use the same rate — see the note on "
                    f"TAKER_FEE_RATE_CRYPTO."
                )

            self.btc_signal_engine = MultiFactorSignalEngine(
                multi_config,
                trade_size=self.config.trade_size
            )
            # The gates in force, in the log at every start — the calibration
            # protocol needs to know which settings produced which trades.
            mc = multi_config
            down = ("DOWN real trades OFF" if mc.min_edge >= 1.0 else
                    f"DOWN edge>={mc.min_edge:g} model_prob>={mc.min_model_prob_down:g} "
                    f"ask {mc.min_ask:g}-{mc.max_ask_down:g} "
                    f"{mc.min_seconds_to_resolution_down:g}-{mc.max_seconds_to_resolution_down:g}s")
            self.logger.info(
                f"[BOT] ✓ Multi-factor signal engine "
                f"(taker fee {mc.taker_fee_rate}; UP edge>={mc.min_edge_up:g} "
                f"model_prob>={mc.min_model_prob_up:g} ask {mc.min_ask_up:g}-{mc.max_ask:g} "
                f"{mc.min_seconds_to_resolution_up:g}-{mc.max_seconds_to_resolution_up:g}s; {down})"
            )

            # Market loader
            self.market_loader = MarketLoader()
            self.logger.info("[BOT] ✓ Market loader")

            # Resolution watcher — audit trail only, cross-checks our own
            # computed UP/DOWN against Polymarket's real market_resolved
            # event once it lands (arrives minutes late, so it never blocks
            # or feeds into actual position settlement). One persistent
            # connection for the whole bot run, independent of each 5-minute
            # cycle's own PolymarketBot/WS.
            self.resolution_watcher = ResolutionWatcher(
                db=self.db,
                # Feed the official result back into settlement instead of
                # only logging the disagreement — see
                # PositionManager.resettle_for_official_resolution().
                on_official_resolution=self.positions.resettle_for_official_resolution,
            )
            self.resolution_watcher.start()
            self.logger.info("[BOT] ✓ Resolution watcher")

            # Positions left OPEN by a previous run belong to markets that
            # ended long ago. Close them now — before the first cycle can
            # sweep them up — and try to recover each one's real outcome in
            # the background. Must run after the watcher exists, since that
            # is what performs the lookup.
            self._settle_and_recover_orphans()

            # Dashboard — same process, own daemon thread. Imported lazily
            # (not at module top) so its logging.basicConfig() call (which
            # is a no-op once the root logger already has handlers) runs
            # AFTER setup_logging() in main() has already configured this
            # process's real handlers (file + stream); importing it at
            # module load time would race that and silently drop file
            # logging for the whole bot. It only ever opens its own
            # short-lived read-only DB connections (see dashboard_server.py
            # get_db()), never touching self.db's connection, so it's safe
            # to run alongside the bot's own DB writes without any locking
            # changes here.
            self._start_dashboard()

            # Shadow trades in small-edge bands (shadow.py): recorded
            # beside the real paper trades, never instead of them.
            self.shadow = ShadowRecorder(
                self, self.config.shadow_edge_bands,
                fee_rate=PositionManager.TAKER_FEE_RATE_CRYPTO,
            )

            # Perp momentum signal, recorded only (perp_shadow.py) — like the
            # shadow above, it never touches the real paper trades.
            if self.config.perp_shadow_enabled:
                try:
                    self.perp_shadow = PerpMomentumShadow(
                        self,
                        variants=self.config.perp_momentum_variants,
                        min_remaining_s=self.config.perp_shadow_min_remaining_s,
                        max_remaining_s=self.config.perp_shadow_max_remaining_s,
                        size=self.config.trade_size,
                        fee_rate=PositionManager.TAKER_FEE_RATE_CRYPTO,
                    ).start()
                except Exception as e:
                    self.perp_shadow = None
                    self.logger.warning(f"[BOT] Perp momentum shadow did not start: {e}")

            # 100 ms market tape (market_recorder.py): a separate process,
            # beside trading, never part of it.
            self._ensure_market_recorder()

            self.logger.info("[BOT] All modules initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"[BOT] Module initialization failed: {e}", exc_info=True)
            return False

    def _settle_and_recover_orphans(self):
        """
        Close any position carried over from a previous run, then look up
        what its market actually resolved to and correct the settlement.

        The close happens synchronously (it must finish before the first
        cycle starts), the recovery does not: it is one HTTP call per
        orphaned market against Polymarket's resolutions endpoint, and the
        bot has 5-minute markets to get on with. A failed or unavailable
        lookup just leaves the position VOID — accurate about the fee that
        was paid, silent about an outcome we could not confirm.
        """
        try:
            condition_ids = self.positions.settle_orphaned_positions()
        except Exception as e:
            self.logger.error(f"[BOT] Orphan settlement failed: {e}", exc_info=True)
            return

        if not condition_ids or not self.resolution_watcher:
            return

        def recover():
            for condition_id in condition_ids:
                # Checked between markets, not just at the start: this
                # does one HTTP round trip per orphaned market, so a
                # shutdown arriving partway through should not have to
                # wait for the rest of the list.
                if self._orphan_recovery_stop.is_set():
                    self.logger.info("[BOT] Orphan recovery stopped before finishing")
                    return
                try:
                    official = self.resolution_watcher.fetch_official_resolution(condition_id)
                    if not official:
                        self.logger.info(
                            f"[BOT] No official resolution available yet for orphaned "
                            f"market {condition_id[:16]}... — leaving it VOID"
                        )
                        continue
                    corrected = self.positions.resettle_for_official_resolution(
                        condition_id, official
                    )
                    self.logger.info(
                        f"[BOT] Orphaned market {condition_id[:16]}... resolved "
                        f"{official} — {corrected} trade(s) corrected"
                    )
                except Exception as e:
                    self.logger.warning(f"[BOT] Orphan recovery failed for {condition_id}: {e}")

        self._orphan_recovery_stop.clear()
        self._orphan_recovery_thread = threading.Thread(
            target=recover, name="OrphanRecovery", daemon=True
        )
        self._orphan_recovery_thread.start()

    def _ensure_market_recorder(self):
        """
        Start market_recorder.py as a child process, or restart it if it has
        died; called at startup and at every cycle start, so a crash costs at
        most one window of tape. Best-effort: the recorder is a data
        collector, never load-bearing for trading.

        Its own session (start_new_session): a Ctrl+C in the terminal reaches
        only the bot, which then stops the recorder in _cleanup() after
        everything that trades. If the bot dies without cleanup, the recorder
        notices its parent is gone (--parent-pid) and stops by itself.
        """
        if not self.config.record_market_tape:
            return
        proc = self.market_recorder
        quiet = False
        if proc is not None:
            code = proc.poll()
            if code is None:
                if self._market_recorder_elsewhere:
                    self._market_recorder_elsewhere = False
                    self.logger.info(f"[BOT] ✓ Market recorder took over the tape (PID {proc.pid})")
                return
            if code == 3:  # market_recorder.EXIT_ALREADY_RUNNING
                if not self._market_recorder_elsewhere:
                    self._market_recorder_elsewhere = True
                    self.logger.info("[BOT] Market recorder: another recorder is already "
                                     "running — retrying each cycle, quietly")
                quiet = True
            else:
                self.logger.warning(f"[BOT] Market recorder exited (code {code}) — restarting")
        here = os.path.dirname(os.path.abspath(__file__))
        try:
            with open(os.path.join(here, "recorder_stderr.log"), "ab") as err:
                self.market_recorder = subprocess.Popen(
                    [sys.executable, os.path.join(here, "market_recorder.py"),
                     "--db", self.config.market_tape_db, "--parent-pid", str(os.getpid()),
                     "--bot-db", self.config.db_file,
                     "--parquet-dir", self.config.parquet_dir if self.config.parquet_sync_enabled else "",
                     "--retention-days", str(self.config.sqlite_retention_days
                                             if self.config.parquet_sync_enabled else 0)],
                    cwd=here, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=err, start_new_session=True,
                )
            if not quiet:
                self.logger.info(f"[BOT] ✓ Market recorder (PID {self.market_recorder.pid}, "
                                 f"100 ms tape → {self.config.market_tape_db}, log recorder.log)")
        except Exception as e:
            self.market_recorder = None
            self.logger.warning(f"[BOT] Market recorder did not start: {e}")

    def _stop_market_recorder(self):
        proc = self.market_recorder
        if proc is None or proc.poll() is not None:
            return
        proc.terminate()
        try:
            # 20 s: feeds close in ~1 s, then the recorder copies its last
            # rows to Parquet (parquet_sync.py) before exiting.
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            raise TimeoutError("did not stop within 20s, killed")

    def _start_dashboard(self):
        """
        Run dashboard_server's read-only HTTP API in a daemon thread of
        this same process, instead of as a separate `python3
        dashboard_server.py` process. Best-effort: if the port's already
        taken (e.g. an old dashboard process is still up) or anything else
        goes wrong, log it and keep trading — the dashboard is a viewer,
        never load-bearing for the bot's own operation.
        """
        try:
            import dashboard_server

            # Listening address from the environment (DASHBOARD_HOST):
            # 127.0.0.1 here, 0.0.0.0 in Docker — see dashboard_server.HOST.
            server = dashboard_server.make_server()
            thread = threading.Thread(
                target=server.serve_forever, name="DashboardServer", daemon=True
            )
            thread.start()
            self.dashboard_server = server
            dashboard_url = f"http://127.0.0.1:{dashboard_server.PORT}"
            self.logger.info(
                f"[BOT] ✓ Dashboard (listening on {dashboard_server.HOST}:{dashboard_server.PORT}, same process)"
            )
            if not dashboard_server.LOOPBACK or os.environ.get("POLYBOT_NO_BROWSER"):
                return              # a server or container has no browser to open

            # Best-effort convenience — a headless box (no GUI/no default
            # browser registered) would raise or just fail silently here;
            # either way the dashboard itself is already up regardless of
            # whether a tab actually opened, so this must never affect
            # startup success.
            try:
                import webbrowser
                webbrowser.open(dashboard_url)
            except Exception as e:
                self.logger.debug(f"[BOT] Could not auto-open browser: {e}")
        except Exception as e:
            self.dashboard_server = None
            self.logger.warning(f"[BOT] Dashboard did not start: {e}")

    # ========================================================================
    # MARKET LIFECYCLE
    # ========================================================================
    
    def _load_market(self) -> bool:
        """Load market data before trading"""

        try:
            # Fast path: a pre-warmed next market is already sitting there
            # connected and subscribed (see _prewarm_next_market(), fired
            # near the end of the previous cycle's trading loop) — promote
            # it instead of reactively discovering + connecting from
            # scratch, which is what caused the ~3.5s gap every cycle.
            if self.next_market_data and self.next_polymarket:
                self.logger.info(
                    f"[MARKET] Using pre-warmed market: {self.next_market_data.question}"
                )
                self.market_data = self.next_market_data
                self.polymarket = self.next_polymarket
                self.config.token_yes_id = self.market_data.token_yes_id
                self.config.token_no_id = self.market_data.token_no_id
                self.config.market_id = self.market_data.id
                self.config.condition_id = self.market_data.condition_id

                # Now that it's the active bot, bring up the threads
                # _prewarm_next_market() deferred (see
                # PolymarketBot.start_market()'s full_threads docstring).
                self.polymarket.start_remaining_threads()

                self.next_market_data = None
                self.next_polymarket = None
                self._polymarket_needs_start = False  # already start_market()'d during prewarm

                self._check_market_tradable()
                return True

            self.logger.info("[MARKET] Loading market data...")

            # Load the currently-live 5-minute BTC Up/Down market.
            # NOTE: We use load_live_5m_btc_market() instead of
            # load_market_by_name() because Polymarket's regular
            # /markets?active=true listing does not include these
            # rolling 5-minute markets at all.
            market = self.market_loader.load_live_5m_btc_market()

            if not market:
                self.logger.error("[MARKET] Failed to load market")
                return False

            if not market.is_active():
                self.logger.error(f"[MARKET] Market is not active: {market.status}")
                return False

            # Check liquidity
            if not self.market_loader.is_sufficient_liquidity(market):
                self.logger.error(f"[MARKET] Insufficient liquidity: ${market.liquidity}")
                return False

            # Store market data
            self.market_data = market
            self.config.token_yes_id = market.token_yes_id
            self.config.token_no_id = market.token_no_id
            self.config.market_id = market.id
            self.config.condition_id = market.condition_id

            self.logger.info(
                f"[MARKET] Loaded: {market.question}\n"
                f"  YES token: {market.token_yes_id[:20]}...\n"
                f"  NO token: {market.token_no_id[:20]}...\n"
                f"  Opening price: {market.opening_price}\n"
                f"  Liquidity: ${market.liquidity:.2f}\n"
                # Informational only, not gated on — see MarketData's field
                # comment: these two don't reliably reflect tradability for
                # this market type (verified live 2026-09-14).
                f"  acceptingOrders: {market.accepting_orders}, enableOrderBook: {market.enable_order_book}\n"
                f"  tick {market.tick_size}, min size {market.min_order_size}, negRisk {market.neg_risk}, "
                f"fees {market.fees_enabled} (rate {market.fee_rate}, exponent {market.fee_exponent}, "
                f"taker only {market.fee_taker_only})"
            )
            self._check_market_tradable()

            # Create a fresh PolymarketBot for THIS market window, using
            # the token IDs we just loaded. Each 5-minute market has its
            # own YES/NO token IDs, so the WS subscription must be redone
            # per cycle rather than reused from a previous window.
            bot_config = self.config.get_polymarket_config()
            self.polymarket = PolymarketBot(bot_config)
            self._polymarket_needs_start = True

            return True

        except Exception as e:
            self.logger.error(f"[MARKET] Load failed: {e}", exc_info=True)
            return False

    def _market_trade_blocker(self, market) -> Optional[str]:
        """
        Why the bot must not trade `market`, or None if it may.

        Compares the market's own trading parameters (read from Gamma, see
        market_loader.MarketData) with what the bot assumes: the edge gate
        subtracts, and PositionManager books, a taker fee of
        MultiFactorConfig.taker_fee_rate * p * (1 - p) — i.e. exponent 1,
        takers only — on an order of config.trade_size shares. If the
        market charges something else, both the edge and the P&L would be
        computed against a fee it doesn't charge, and neither number would
        look wrong. A value Gamma didn't send is treated the same way: not
        known to match.
        """
        if market is None:
            return "no market data"
        rate = self.btc_signal_engine.config.taker_fee_rate
        if market.fees_enabled is not True:
            return f"fees not confirmed enabled (feesEnabled={market.fees_enabled})"
        if (market.fee_rate is None or abs(market.fee_rate - rate) > 1e-9
                or market.fee_exponent != 1 or market.fee_taker_only is not True):
            return (f"fee schedule rate={market.fee_rate} exponent={market.fee_exponent} "
                    f"takerOnly={market.fee_taker_only} differs from the bot's "
                    f"(rate={rate}, exponent=1, takers only)")
        if market.min_order_size is None:
            return "minimum order size unknown"
        if self.config.trade_size < market.min_order_size:
            return (f"trade size {self.config.trade_size} is below the market's "
                    f"minimum order size {market.min_order_size}")
        return None

    def _check_market_tradable(self):
        """Set self._market_blocker for the market just loaded; say so
        loudly if it can't be traded (every signal on it will be skipped)."""
        self._market_blocker = self._market_trade_blocker(self.market_data)
        if self._market_blocker:
            self.logger.error(f"[MARKET] Not trading this market: {self._market_blocker}")

    def _prewarm_next_market(self):
        """
        Find and connect to the NEXT 5-minute market's WS while the current
        one is still trading, so the cycle transition has data flowing
        immediately instead of a multi-second discover-then-connect gap.
        Called once near the end of each cycle's trading loop. Failure here
        just means _load_market() falls back to its normal reactive path —
        never fatal.
        """
        try:
            if not self.market_data:
                return

            # Defensive: if a previous prewarm's bot is still sitting here
            # unconsumed (_load_market()'s fast path normally clears both
            # attributes together every time it promotes one — see there —
            # so this shouldn't be reachable in the current control flow,
            # but overwriting the reference below without stopping it
            # first would otherwise leak a WSReader thread forever
            # reconnecting to a market nothing else references).
            if self.next_polymarket is not None:
                self.logger.warning(
                    "[PREWARM] Replacing an unconsumed prewarmed bot — stopping its threads first"
                )
                try:
                    self.next_polymarket.end_market()
                except Exception as e:
                    self.logger.debug(f"[PREWARM] Cleanup of stale prewarmed bot failed: {e}")

            next_market = self.market_loader.load_next_5m_btc_market(self.market_data)
            if not next_market:
                self.logger.debug("[PREWARM] Next market not available yet, will load reactively")
                return

            # Same two checks the reactive path in _load_market() applies.
            # They were missing here, and since the pre-warmed path is the
            # one actually taken — 43 of 51 cycles, 84%, in the 2026-09-22
            # session — neither check was really running.
            #
            # They are NOT a safety net against bad trades: both have live
            # per-tick equivalents that run whichever path loaded the
            # market. MultiFactorSignalEngine.evaluate() gates on
            # market_status every tick, and vwap_fill_price() checks real
            # book depth for the full trade size every tick — a direct
            # measure of tradability that a stale aggregate "liquidity"
            # figure cannot improve on. What these buy is failing fast:
            # not spending a whole five-minute window on a market that was
            # never going to trade.
            #
            # Deliberately fail-SOFT. This data was fetched ~20s before the
            # window opens, and it is not verified how Gamma reports
            # `active` and `liquidity` for a market that has not started —
            # liquidity in particular may build only once it goes live. So
            # a rejection here just skips pre-warming and lets
            # _load_market() take its reactive path, which re-fetches and
            # re-checks against fresh data once the window is actually
            # open. The cost of being wrong is ~3.5s of latency for that
            # cycle, not a lost cycle. Logged at WARNING with the values
            # so a systematic false rejection is visible in the first run
            # rather than silently costing the optimisation every cycle.
            if not next_market.is_active():
                self.logger.warning(
                    f"[PREWARM] Next market reports status={next_market.status!r} "
                    f"~20s before its window opens — skipping pre-warm, will load "
                    f"reactively. (If this happens every cycle, Gamma simply does not "
                    f"mark a not-yet-started market active and this check belongs only "
                    f"on the reactive path.)"
                )
                return

            if not self.market_loader.is_sufficient_liquidity(next_market):
                self.logger.warning(
                    f"[PREWARM] Next market liquidity ${next_market.liquidity:.2f} is below "
                    f"the ${self.market_loader.config.min_liquidity:.2f} minimum ~20s before "
                    f"its window opens — skipping pre-warm, will load reactively. "
                    f"(If this happens every cycle, liquidity is simply reported low "
                    f"before a window goes live and this check belongs only on the "
                    f"reactive path.)"
                )
                return

            next_bot_config = BotConfig(
                market_duration_seconds=self.config.market_duration_seconds,
                market_name=self.config.market_name,
                token_yes_id=next_market.token_yes_id,
                token_no_id=next_market.token_no_id,
                paper_trade_size=self.config.trade_size
            )
            next_bot = PolymarketBot(next_bot_config)
            # Only the WS reader — this bot isn't active yet, so its
            # heartbeat/REST-fallback/stats threads have nothing to do
            # until _load_market() promotes it (start_remaining_threads()
            # there). Halves the thread overlap during this ~20s window
            # against the current cycle's own 4 threads — see
            # PolymarketBot.start_market()'s full_threads docstring.
            next_bot.start_market(full_threads=False)

            self.next_market_data = next_market
            self.next_polymarket = next_bot

            self.logger.info(f"[PREWARM] Connected ahead of time: {next_market.question}")

        except Exception as e:
            self.logger.warning(f"[PREWARM] Failed (will fall back to reactive load): {e}")

    def _window_bounds(self):
        """
        (start, end) of the current Polymarket window in unix seconds —
        end from market_data.expires_at (the real endDate), start that minus
        the market duration — or (None, None) when unavailable.
        """
        try:
            end = datetime.fromisoformat(
                self.market_data.expires_at.replace("Z", "+00:00")
            ).timestamp()
        except (AttributeError, TypeError, ValueError):
            return None, None
        return end - self.config.market_duration_seconds, end

    def _book_recorder_loop(self):
        """
        Once a second, while a market is open, record both tokens' order
        book state to book_ticks (see config.record_order_book): best
        bid/ask, the fill price for the bot's full trade size, top-5 depth
        per side, book age/source, whether the live book was verified, BTC,
        and how far behind Polymarket the feed was. Own SQLite connection (WAL, so it never blocks the bot's
        writes); batched commits every 10s. Never raises.
        """
        import sqlite3
        size = self.config.trade_size
        try:
            conn = sqlite3.connect(self.config.db_file, timeout=10)
            conn.execute("PRAGMA journal_mode=WAL")
            # id: a stable key for parquet_sync.py's export and cleanup
            # (sqlite_keys.py); inserts below name their columns.
            conn.execute("""CREATE TABLE IF NOT EXISTS book_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL, window_end INTEGER, token TEXT,
                best_bid REAL, best_ask REAL, fill_price REAL,
                bid_depth5 REAL, ask_depth5 REAL,
                book_age REAL, source TEXT, live_verified INTEGER, btc REAL,
                feed_behind REAL)""")
            # feed_behind (seconds; PolymarketBot.feed_behind_s) was added
            # 2026-09-28. book_age only says when a book arrived; this says
            # how far behind the exchange it was, so research on these
            # rows can drop the ones priced from a backed-up feed.
            cols = {row[1] for row in conn.execute("PRAGMA table_info(book_ticks)")}
            if "feed_behind" not in cols:
                conn.execute("ALTER TABLE book_ticks ADD COLUMN feed_behind REAL")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_book_ticks_window ON book_ticks(window_end)")
            conn.commit()
            # Older databases get the key once; a failure (e.g. a lock)
            # only postpones it — recording continues either way.
            try:
                from sqlite_keys import ensure_autoincrement_key
                ensure_autoincrement_key(conn, "book_ticks")
            except Exception as e:
                self.logger.warning(f"[BOOK-REC] book_ticks id key not added yet ({e}); retried at next start")
        except Exception as e:
            self.logger.warning(f"[BOOK-REC] Recording disabled: {e}")
            return

        def depth5(levels, best_first_high):
            try:
                prices = sorted(((float(l["price"]), float(l["size"])) for l in levels),
                                reverse=best_first_high)
                return sum(s for _, s in prices[:5])
            except (KeyError, TypeError, ValueError):
                return None

        BOOK_TICKS_INSERT = ("INSERT INTO book_ticks (ts, window_end, token, best_bid, best_ask, "
                             "fill_price, bid_depth5, ask_depth5, book_age, source, live_verified, "
                             "btc, feed_behind) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
        rows, last_commit, next_tick = [], time.time(), time.time()
        self.logger.info("[BOOK-REC] Recording order books to book_ticks every 1s")
        while self.running:
            next_tick += 1.0
            try:
                pm, window_end = self.polymarket, getattr(self, "_loop_end", None)
                if pm is not None and self.market_open and window_end:
                    now = time.time()
                    cached = self.external.binance.price_cache
                    btc = cached.btc_usd if cached and now - cached.timestamp < 5 else None
                    behind = pm.feed_behind_s(now)
                    for token, book in (("yes", pm.orderbook_cache_yes), ("no", pm.orderbook_cache_no)):
                        if not book or not (book.bids or book.asks):
                            continue
                        rows.append((now, int(window_end), token, book.best_bid, book.best_ask,
                                     book.vwap_fill_price(size, "buy"),
                                     depth5(book.bids, True), depth5(book.asks, False),
                                     (now - book.timestamp) if book.timestamp else None, book.source,
                                     int(bool(getattr(pm, "_live_ok", {}).get(token))), btc, behind))
                if rows and time.time() - last_commit >= 10:
                    conn.executemany(BOOK_TICKS_INSERT, rows)
                    conn.commit()
                    rows.clear()
                    last_commit = time.time()
            except Exception as e:
                self.logger.debug(f"[BOOK-REC] {e}")
            time.sleep(max(0.0, next_tick - time.time()))
        try:
            if rows:
                conn.executemany(BOOK_TICKS_INSERT, rows)
                conn.commit()
            conn.close()
        except Exception:
            pass

    def _settlement_price(self, ts):
        """
        (price, source) at window boundary `ts` on the rule Polymarket
        settles these markets by — the 60s TWAP ending at the boundary
        (Chainlink btc-usd-twap-60s; see MultiFactorConfig's docstring) —
        or (None, None).

        The 1s candles first: they are defined in a quiet market and after
        a mid-window restart, where the WS tick history (tried second) may
        not reach back far enough. If neither gives a TWAP, the spot price
        at the boundary is a degraded fallback: its source says so in the
        log, since the signal's thresholds were chosen against the TWAP.
        """
        if ts is None:
            return None, None
        binance = self.external.binance
        twap_s = int(self.btc_signal_engine.config.settlement_twap_seconds)
        price = binance.twap_before_rest(ts, twap_s)
        if price is not None:
            return price, f"{twap_s}s TWAP from 1s candles"
        price = binance.twap_before(ts, twap_s)
        if price is not None:
            return price, f"{twap_s}s TWAP from WS ticks"
        price = binance.price_before_rest(ts)
        if price is not None:
            return price, "SPOT 1s candle — TWAP unavailable"
        price = binance.price_at(ts)
        if price is not None:
            return price, "SPOT WS tick — TWAP unavailable"
        return None, None

    def _start_market(self):
        """Start trading session"""
        
        try:
            self.logger.info("[MARKET] Starting trading session...")

            self.session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.market_open = True
            # NOT self.running = True here — run() already sets that once
            # before its loop starts. Resetting it here would silently
            # revert a shutdown signal that arrived while _load_market()
            # (a network call) was still in flight: _signal_handler sets
            # self.running=False, _load_market() finishes and returns
            # True, and without this, _start_market() would flip it back
            # to True and run a full extra cycle before anything checked
            # self.running again.

            self.session_stats["start_time"] = datetime.now().timestamp()

            # New cycle — allow prewarm to fire again near its end.
            self._prewarm_attempted = False

            # New cycle — don't let a throttled log from the tail end of
            # the previous cycle suppress this cycle's first one.
            self._last_nosignal_log_time = 0.0
            self._last_progress_log_time = 0.0

            # Per-cycle signal/decision snapshot for the market_sessions
            # row _end_market() saves regardless of whether a trade fired
            # — see _run_trading_loop()'s NO_SIGNAL branch and its
            # trade-open block for where these get filled in.
            self._cycle_signal_type = None
            self._cycle_score = None
            self._cycle_rejected_reason = None
            self._cycle_signal_features_json = None
            self._cycle_position_id = None
            # -1 so the very first NO_SIGNAL tick (depth >= 0) always wins —
            # see _reason_depth()'s docstring for why "deepest wins" instead
            # of "latest wins".
            self._cycle_best_depth = -1

            # Capture the real BTC/USD price at window open. This is what
            # UP/DOWN resolution must compare against at market end — it is
            # NOT the same thing as market.opening_price (that's Polymarket's
            # own YES-token probability, e.g. 0.315, a completely different
            # scale from a BTC dollar price). Captured here (the true start
            # of the window) regardless of whether this bot was just
            # created or promoted from a prewarmed connection.
            #
            # Binance specifically (user-specified 2026-09-20) — and used
            # consistently for
            # open/tick/close (see _run_trading_loop() and _end_market())
            # rather than mixing sources, so a signal computed against one
            # feed is never resolved against a different one. This can
            # disagree with Polymarket's own official (Chainlink-based)
            # settlement; ResolutionWatcher already audits our resolution
            # against theirs for exactly this kind of drift, independent of
            # whichever feed drives it.
            # RESOLUTION_MAX_AGE, not the default: this reading is one of
            # the two numbers the whole window's UP/DOWN outcome is
            # computed from, so a price from even a few seconds before the
            # window opened measures the wrong span. get_price() returns
            # None rather than something stale, and None settles the cycle
            # VOID — the correct outcome for "we cannot honestly say".
            #
            # Preferred: the 60s TWAP ending at the window's real start
            # (_settlement_price) — the reference Polymarket itself settles
            # against, and what the 2026-09-27 settings were chosen on.
            # Falls back to the freshest cached price only when no boundary
            # price can be had at all.
            window_start, _ = self._window_bounds()
            aligned_open, open_src = self._settlement_price(window_start)
            if aligned_open is not None:
                self.btc_opening_price = aligned_open
                self.logger.info(
                    f"[MARKET] BTC opening price: ${aligned_open:.2f} "
                    f"(window start, {open_src})"
                )
            else:
                btc_price_data = self.external.binance.get_price(
                    max_age_seconds=self.external.binance.RESOLUTION_MAX_AGE,
                    # Twice per cycle, and it decides whether the cycle can
                    # trade — see get_price()'s bypass_throttle docstring.
                    bypass_throttle=True,
                )
                self.btc_opening_price = btc_price_data.btc_usd if btc_price_data else None
                if self.btc_opening_price:
                    self.logger.info(
                        f"[MARKET] BTC opening price: ${self.btc_opening_price:.2f} "
                        f"(age {time.time() - btc_price_data.timestamp:.2f}s, "
                        f"src {btc_price_data.source}; window-start trade unavailable)"
                    )
                else:
                    self.logger.error(
                        "[MARKET] No sufficiently fresh BTC opening price "
                        f"(needed < {self.external.binance.RESOLUTION_MAX_AGE:.0f}s old) "
                        "— market will settle as VOID"
                    )

            # Refresh the 1-minute candles behind the signal's volatility
            # estimate (MinuteVolatility), and the 1-second ones behind the
            # log-only ShortVolatility. In a thread and after the opening
            # price on purpose: a slow REST call must delay neither.
            if self.btc_signal_engine is not None:
                engine, binance = self.btc_signal_engine, self.external.binance

                def refresh_vol():
                    engine.vol.load_klines(binance.get_minute_closes())
                    cfg = engine.config
                    engine.short_vol.load_second_closes(binance.get_second_closes(
                        cfg.short_vol_window_seconds + 3 * cfg.short_vol_step_seconds))

                threading.Thread(target=refresh_vol, daemon=True, name="VolRefresh").start()

            # Start Polymarket — unless this bot was already start_market()'d
            # during prewarm (its WS is already connected and subscribed).
            if self._polymarket_needs_start:
                self.polymarket.start_market()
            else:
                self.logger.info("[MARKET] Prewarmed connection already live, skipping start_market()")

            self.logger.info(
                f"[MARKET] Session started: {self.session_id}\n"
                f"  Duration: {self.config.market_duration_seconds}s (5 min)"
            )

            # Last, so a restart (one fork) never delays the opening price
            # or the market connection above.
            self._ensure_market_recorder()

        except Exception as e:
            self.logger.error(f"[MARKET] Start failed: {e}", exc_info=True)
            self.market_open = False
    
    def _end_market(self):
        """End trading session and settle positions"""
        
        try:
            self.logger.info("[MARKET] Ending trading session...")
            
            self.market_open = False
            self.session_stats["end_time"] = datetime.now().timestamp()

            # Stop this cycle's PolymarketBot cleanly (closes WS, stops
            # its background threads) before we load the next 5-minute
            # market and create a new PolymarketBot for it.
            if self.polymarket:
                try:
                    self.polymarket.should_run = False
                    self.polymarket.end_market()
                except Exception as e:
                    self.logger.warning(f"[MARKET] PolymarketBot shutdown warning: {e}")
            
            # Get closing BTC/USD price — Binance, same reasoning as the
            # opening price capture above (consistency with the source the
            # whole cycle's signal ticks used), including preferring the
            # 60s TWAP ending at the window's real end (_loop_end) — the
            # same rule as the opening price, so this bot's own UP/DOWN call
            # matches Polymarket's official one far more often (98% vs 84%
            # on 820 past windows).
            window_end = getattr(self, "_loop_end", None)
            closing_price, close_src = self._settlement_price(window_end)
            if closing_price is not None:
                self.logger.info(
                    f"[MARKET] BTC closing price: ${closing_price:.2f} "
                    f"(window end, {close_src})"
                )
            else:
                closing_price_data = self.external.binance.get_price(
                    max_age_seconds=self.external.binance.RESOLUTION_MAX_AGE,
                    bypass_throttle=True,
                )
                closing_price = closing_price_data.btc_usd if closing_price_data else None
                if closing_price_data is not None:
                    self.logger.info(
                        f"[MARKET] BTC closing price: ${closing_price:.2f} "
                        f"(age {time.time() - closing_price_data.timestamp:.2f}s, "
                        f"src {closing_price_data.source}; window-end trade unavailable)"
                    )
                else:
                    self.logger.error(
                        "[MARKET] No sufficiently fresh BTC closing price "
                        f"(needed < {self.external.binance.RESOLUTION_MAX_AGE:.0f}s old) "
                        "— settling VOID rather than resolving on a stale reading"
                    )

            # Resolve market (UP/DOWN) by comparing real BTC/USD price at
            # close vs at open — NOT market_data.opening_price, which is
            # Polymarket's own YES-token probability (0-1 scale) and isn't
            # comparable to a BTC dollar price.
            if closing_price and self.btc_opening_price:
                resolution = self.external.resolve_market(self.btc_opening_price, closing_price=closing_price)
                reliability_note = (
                    " [UNRELIABLE: picked up too late — opening/closing prices are "
                    "seconds apart, not the real ~5min window, see _run_trading_loop]"
                    if self._late_pickup else ""
                )
                self.logger.info(
                    f"[MARKET] Session ended: {self.session_id}\n"
                    f"  BTC opening: ${self.btc_opening_price:.2f}\n"
                    f"  BTC closing: ${closing_price:.2f}\n"
                    f"  Resolution: {resolution}{reliability_note}"
                )

                # Hand off to the resolution watcher: it'll log a match/
                # mismatch once Polymarket's real market_resolved event
                # lands (a few minutes from now) — pure audit trail, does
                # not affect the position settlement below.
                if self.resolution_watcher and self.market_data:
                    self.resolution_watcher.track_session(
                        token_yes_id=self.market_data.token_yes_id,
                        token_no_id=self.market_data.token_no_id,
                        condition_id=self.market_data.condition_id,
                        question=self.market_data.question,
                        our_resolution=resolution
                    )
            else:
                resolution = "UNKNOWN"
                self.logger.error(
                    "[MARKET] Could not resolve UP/DOWN (missing BTC open/close price) "
                    "— settling open positions as VOID"
                )

            # Close all positions. Each position settles on whether its own
            # signal_type (BUY_UP/BUY_DOWN) called the resolution correctly
            # (binary payout: 1.0 win / 0.0 loss) rather than all positions
            # sharing one flat price — a BUY_UP and a BUY_DOWN position in
            # the same window do NOT have the same outcome.
            # only_condition_id: a resolution belongs to ONE window. Without
            # this, any position still open from another market — notably one
            # carried over from a previous run — got paid out 1.0/0.0 on an
            # outcome it had nothing to do with. See
            # PositionManager.close_all_positions().
            closed_this_cycle = self.positions.close_all_positions(
                resolution=resolution,
                only_condition_id=(
                    self.market_data.condition_id if self.market_data else None
                ),
            )

            # THIS CYCLE's numbers, derived from the trades this cycle
            # actually closed — not from PositionManager's running totals.
            #
            # The market_sessions row means "what happened in this
            # 5-minute window", but it used to be filled from
            # get_stats(), which counts everything since the process
            # started. Every row therefore carried the run-to-date totals,
            # so cycle 40 reported 39 earlier cycles' trades as its own and
            # per-cycle analysis was impossible.
            #
            # Taking a snapshot at cycle start and diffing at cycle end
            # would not work either, now that trades can be re-settled
            # asynchronously: a correction landing mid-cycle for an OLDER
            # market shifts those same running totals (see
            # PositionManager.resettle_for_official_resolution), and the
            # diff would silently attribute it to this cycle. Reading the
            # closed-position results directly is exact and immune to that.
            cycle_trades = len(closed_this_cycle)
            cycle_pnl = sum(r["pnl"] for r in closed_this_cycle)
            cycle_wins = sum(1 for r in closed_this_cycle if r["status"] == "win")
            cycle_losses = sum(1 for r in closed_this_cycle if r["status"] == "loss")
            cycle_win_rate = (cycle_wins / cycle_trades * 100) if cycle_trades else 0.0

            # session_stats is run-wide (see its own comment) — it keeps
            # the cumulative figure on purpose.
            final_stats = self.positions.get_stats()
            self.session_stats["total_pnl"] = final_stats["total_pnl"]

            # Persist this cycle regardless of outcome — one row per
            # 5-minute window, traded or not, so a NO_SIGNAL cycle carries
            # the same score/rejected_reason/signal_features detail a
            # traded one gets (see _start_market()'s _cycle_* reset and
            # _run_trading_loop()'s NO_SIGNAL/trade-open blocks for where
            # these get filled in). condition_id is used as the row's key
            # (not the internal session_id) so ResolutionWatcher's async
            # official-resolution update — which only knows condition_id —
            # can find this same row later.
            try:
                cycle_session_id = (
                    self.market_data.condition_id
                    if self.market_data and self.market_data.condition_id
                    else self.session_id
                )
                self.db.save_market_session(MarketSession(
                    session_id=cycle_session_id,
                    open_time=self.session_stats.get("start_time"),
                    close_time=self.session_stats.get("end_time"),
                    opening_price=self.btc_opening_price,
                    closing_price=closing_price,
                    resolution=resolution,
                    total_trades=cycle_trades,
                    total_pnl=cycle_pnl,
                    win_rate=cycle_win_rate,
                    cycle_number=getattr(self, "_current_cycle_number", None),
                    signal_type=self._cycle_signal_type,
                    trade_opened=1 if self._cycle_position_id else 0,
                    position_id=self._cycle_position_id,
                    rejected_reason=self._cycle_rejected_reason,
                    score=self._cycle_score,
                    signal_features=self._cycle_signal_features_json,
                ))
            except Exception as e:
                self.logger.error(f"[DB] Failed to persist cycle record: {e}")

            self.logger.info(
                f"[CYCLE] This window: {cycle_trades} trade(s), "
                f"{cycle_wins}W/{cycle_losses}L, PnL ${cycle_pnl:.2f}"
            )

            self.logger.info(
                f"[SESSION] Run totals (all cycles since start):\n"
                f"  Total trades: {final_stats['total_trades']}\n"
                f"  Wins: {final_stats['wins']}\n"
                f"  Losses: {final_stats['losses']}\n"
                f"  Win rate: {final_stats['win_rate']:.1f}%\n"
                f"  Total PnL: ${final_stats['total_pnl']:.2f}"
            )
        
        except Exception as e:
            self.logger.error(f"[MARKET] End failed: {e}", exc_info=True)
    

    # ========================================================================
    # TRADING LOOP
    # ========================================================================

    def _run_trading_loop(self):
        """
        Main trading loop for this ~5 minute market window — event-driven.

        A signal is evaluated the instant fresh BTC price (Binance WS) or
        order book (Polymarket WS) data arrives, via _request_tick() ->
        _evaluator_loop() -> _on_market_tick(), instead of on a fixed
        polling interval. This method wires that up, then runs a
        lightweight watchdog loop for the handful of things that are
        genuinely time-based rather than data-triggered: detecting the
        window's real close and firing the next cycle's prewarm.
        """

        try:
            self._loop_start = time.time()
            loop_start = self._loop_start

            # Anchor to Polymarket's own real window end (market_data.expires_at,
            # the actual endDate) instead of loop_start + 300s. loop_start is
            # whenever OUR code happened to begin this tick — even with
            # prewarming that's ~1s after the real window began — so a
            # duration-based end would drift by that same ~1s every single
            # cycle. The real endDate is exact, so anchoring to it directly
            # makes "remaining" (and everything derived from it: the BTC
            # signal's threshold scaling, the prewarm trigger) track
            # Polymarket's actual clock instead of our own approximation.
            loop_end = None
            if self.market_data and self.market_data.expires_at:
                try:
                    loop_end = datetime.fromisoformat(
                        self.market_data.expires_at.replace("Z", "+00:00")
                    ).timestamp()
                except (ValueError, AttributeError):
                    loop_end = None

            if loop_end is None:
                self.logger.warning("[LOOP] No real market end time available, falling back to duration-based timing")
                loop_end = loop_start + self.config.market_duration_seconds

            self._loop_end = loop_end

            self.logger.info(
                f"[LOOP] Starting trading loop... "
                f"({loop_end - loop_start:.1f}s until Polymarket's real window close)"
            )

            # A market picked up this close to its own end (e.g. catching
            # up after a network outage — observed live 2026-09-16) can
            # never actually trade anyway: MultiFactorSignalEngine's own
            # time-window gate already requires remaining >= 60s, so
            # nothing would fire regardless. But self.btc_opening_price was
            # still just captured fresh in _start_market() at effectively
            # the same
            # moment as the eventual closing price — comparing two BTC
            # reads seconds apart instead of the market's real ~5-minute
            # span makes the UP/DOWN call pure noise, not a signal. Verified
            # live: this produced a resolution that disagreed with
            # Polymarket's own official one (our=DOWN, official=UP on a
            # $2.38 gap). No position can open here, so this only affects
            # the resolution logged in _end_market() — flagged there via
            # this attribute so it reads as "unreliable," not as a bug in
            # the resolve logic itself.
            self._late_pickup = (loop_end - loop_start) < 60
            if self._late_pickup:
                self.logger.warning(
                    f"[LOOP] Picked up with only {loop_end - loop_start:.1f}s left in the real "
                    f"window — too late to trade or to trust the opening-price reference. "
                    f"Skipping this cycle's loop."
                )
                return

            # Generation token for this cycle's loop — _evaluator_loop()
            # and _on_market_tick() both check their captured generation
            # against self._active_generation before doing anything.
            # Needed because self._evaluator_thread.join() below has a
            # timeout: if a tick is ever stuck long enough for the join to
            # time out, the OLD thread is still alive when this method
            # returns and the NEXT cycle starts a second evaluator thread.
            # A plain boolean (_loop_active) can't tell those two threads
            # apart — both would see it as True once the next cycle sets
            # it again. The generation counter can: the stale thread's
            # captured number never matches self._active_generation again
            # once a newer cycle bumps it, so it quietly stops itself
            # (and refuses to run a tick) instead of processing the next
            # cycle's data too.
            self._active_generation += 1
            my_generation = self._active_generation

            # Wrapping from here (not just the watchdog loop below) so
            # ANY exception during setup — including self._evaluator_thread.start()
            # itself failing, e.g. under the thread pressure a prewarm
            # overlap can create — still runs the teardown in `finally`
            # instead of leaking this cycle's callbacks/active state into
            # the next one.
            try:
                self._loop_active = True
                self._tick_ready.clear()
                # Wire up event-driven evaluation for THIS cycle's feeds:
                # the Binance client is shared across cycles (persistent),
                # so its callback is (re)pointed here every cycle;
                # self.polymarket is a fresh instance per cycle, so it
                # never carries a stale callback from a torn-down
                # previous one.
                self.external.binance.on_price_update = self._request_tick
                self.polymarket.on_book_update = self._request_tick

                self._evaluator_thread = threading.Thread(
                    target=self._evaluator_loop, args=(my_generation,),
                    daemon=True, name="SignalEvaluator"
                )
                self._evaluator_thread.start()
                # Evaluate once immediately against whatever's already
                # cached (e.g. from prewarm) instead of waiting for the
                # first fresh WS tick, which could still be a moment away
                # during a quiet market.
                self._request_tick()

                # Also stop if the real market resolves before our own
                # timer does — verified live 2026-09-14: our internal
                # clock doesn't necessarily match the real window's
                # remaining time (e.g. if we pick up a market close to its
                # own end), and without this check the loop kept running
                # for minutes against an already-dead market: repeated WS
                # reconnects and REST 404s on tokens whose book no longer
                # exists. Sleeps briefly between checks — this thread does
                # NOT evaluate signals (that's _evaluator_loop, woken
                # instantly by each new tick), so a short poll interval
                # here only affects how quickly window-close/prewarm are
                # noticed, not trading latency.
                while (self.running and time.time() < loop_end
                       and self.polymarket and self.polymarket.market_open):
                    remaining = loop_end - time.time()

                    # Pre-warm the next cycle's market + WS connection once,
                    # with enough runway left (20s) for the Gamma API call
                    # and WS connect+subscribe to finish well before this
                    # cycle actually ends.
                    if remaining <= 20 and not self._prewarm_attempted:
                        self._prewarm_attempted = True
                        self._prewarm_next_market()

                    time.sleep(0.5)
            finally:
                # Tear down before this cycle's PolymarketBot gets stopped
                # (_end_market(), right after this method returns) — a
                # stray late callback into a torn-down bot's state would
                # otherwise be possible.
                self._loop_active = False
                self.external.binance.on_price_update = None
                if self.polymarket:
                    self.polymarket.on_book_update = None
                self._tick_ready.set()  # wake the evaluator so it can exit
                if self._evaluator_thread:
                    self._evaluator_thread.join(timeout=5)
                    if self._evaluator_thread.is_alive():
                        self.logger.error(
                            "[LOOP] SignalEvaluator thread did not stop within 5s "
                            "(a tick is stuck) — leaving it running; the generation "
                            "check in _evaluator_loop()/_on_market_tick() keeps it "
                            "from acting on the next cycle's data."
                        )
                    self._evaluator_thread = None

            self.logger.info("[LOOP] Trading loop ended")

        except Exception as e:
            self.logger.error(f"[LOOP] Fatal error: {e}", exc_info=True)

    def _record_cycle_reason(self, signal_type: str, reason: Optional[str],
                             evaluation=None, extra: Optional[dict] = None):
        """
        Record why this cycle did not (yet) produce a trade, keeping only
        the most informative reason seen — see _REASON_DEPTH_ORDER.

        Every non-trading outcome funnels through here: a circuit-breaker
        pause, an engine gate rejection, and — the part that was missing —
        a signal that passed every gate and then failed on the way to an
        order. Before this, those last ones updated nothing at all, so a
        cycle where a real signal fired and died on slippage was stored as
        an ordinary NO_SIGNAL with whatever gate reason some other tick
        had reported. That is precisely how an unreachable slippage limit
        went unnoticed while rejecting 16,946 signals in one session.

        Frozen once a trade opens (via _cycle_position_id) so a later tick
        can never overwrite the record of the trade that actually fired.
        """
        if self._cycle_position_id is not None:
            return

        depth = _reason_depth(reason)
        score = evaluation.score if evaluation is not None else None
        # Ties broken by score, so among equally-deep rejections the
        # closest call is the one that gets remembered.
        is_better = depth > self._cycle_best_depth or (
            depth == self._cycle_best_depth
            and (score or 0.0) > (self._cycle_score or 0.0)
        )
        if not is_better:
            return

        self._cycle_best_depth = depth
        self._cycle_signal_type = signal_type
        self._cycle_rejected_reason = reason
        self._cycle_score = score

        features = dict(extra or {})
        if evaluation is not None:
            features.update({
                "candidate_direction": evaluation.candidate_direction,
                "score": evaluation.score,
                "score_breakdown": evaluation.breakdown,
                "distance_z": evaluation.distance_z,
                "returns": evaluation.returns,
                "edge": evaluation.edge,
                "model_prob": evaluation.model_prob,
                "vol_per_sqrt_s": evaluation.vol_per_sqrt_s,
                # log-only, see signal_engine.ShortVolatility
                "short_vol_per_sqrt_s": evaluation.short_vol_per_sqrt_s,
                "model_prob_short_vol": evaluation.model_prob_short_vol,
                "edge_short_vol": evaluation.edge_short_vol,
                "candidate_ask": evaluation.candidate_ask,
                "obi_1": (evaluation.obis or {}).get("obi_1"),
                "obi_5": (evaluation.obis or {}).get("obi_5"),
                "obi_10": (evaluation.obis or {}).get("obi_10"),
                "trade_flow": evaluation.trade_flow,
            })
        try:
            self._cycle_signal_features_json = json.dumps(features) if features else None
        except Exception:
            self._cycle_signal_features_json = None

    def _request_tick(self, *_args):
        """
        Called from a WS callback thread — BinanceSpot.on_price_update
        (new BTC price) or PolymarketBot.on_book_update (new order book
        snapshot) — whenever fresh market data lands. Only flags that new
        data is available; O(1) and never blocks the WS thread. Idempotent:
        however many ticks arrive while the evaluator is still busy with
        the previous one (order-book bursts of 700-800/s have been seen
        live during high BTC volatility) collapse into a single extra
        evaluation, in _evaluator_loop (no sooner than
        Config.evaluator_min_interval_ms after the previous one started)
        — never a queue of redundant work.
        """
        self._tick_ready.set()

    def _evaluator_loop(self, generation: int):
        """
        Dedicated single-threaded consumer that turns _tick_ready signals
        into actual signal evaluations, one at a time. Running this on its
        own thread (rather than directly on a WS callback thread) means a
        slow evaluation can never stall the WS reader and risk it being
        seen as a "slow consumer" by the server.

        `generation` is this thread's own cycle's generation token (see
        _run_trading_loop()) — checked against the CURRENT
        self._active_generation, not just self._loop_active, so a thread
        that outlives its own cycle's teardown (its join() timed out)
        cleanly stops itself once a newer cycle bumps the active
        generation, instead of going on to service that next cycle's
        ticks too.
        """
        min_gap = max(0.0, self.config.evaluator_min_interval_ms / 1000.0)
        last_start = float("-inf")
        evals = throttled = 0
        cpu_total = cpu_max = 0.0
        started = time.monotonic()
        try:
            while self._loop_active and self._active_generation == generation:
                self._tick_ready.wait()
                # See Config.evaluator_min_interval_ms. Sleeping BEFORE the
                # clear() lets every tick that lands meanwhile fold into
                # this one evaluation instead of queueing another.
                wait = last_start + min_gap - time.monotonic()
                if wait > 0:
                    throttled += 1
                    time.sleep(wait)
                self._tick_ready.clear()
                if not self._loop_active or self._active_generation != generation:
                    break
                last_start = time.monotonic()
                # CPU time, not wall time: only the time this thread spends
                # running (holding the GIL) competes with the WS readers.
                cpu_before = time.thread_time()
                try:
                    self._on_market_tick(generation)
                except Exception as e:
                    self.logger.error(f"[LOOP] Tick evaluation error: {e}", exc_info=True)
                cpu = time.thread_time() - cpu_before
                evals += 1
                cpu_total += cpu
                cpu_max = max(cpu_max, cpu)
        finally:
            wall = time.monotonic() - started
            if evals and wall > 0:
                self.logger.info(
                    f"[LOOP] Evaluator: {evals} evaluations ({evals / wall:.1f}/s), "
                    f"{throttled} delayed by the {min_gap * 1000:.0f} ms minimum interval; "
                    f"CPU avg {cpu_total / evals * 1000:.2f} ms, max {cpu_max * 1000:.2f} ms, "
                    f"{cpu_total / wall * 100:.1f}% of the cycle"
                )
            # This thread is recreated every 5-minute cycle, and the risk
            # circuit breaker it runs opens a SQLite connection of its own
            # (Database._get_connection is thread-local). Nothing else can
            # close it: sqlite3 connections are thread-affine, so
            # Database.close() on the main thread cannot reach it.
            #
            # Hygiene, not a leak fix — measured, the garbage collector
            # reclaims a dead thread's connection either way. This just
            # makes the release deterministic and keeps ownership where it
            # belongs: the thread that opened it closes it.
            try:
                if self.db:
                    self.db.close()
            except Exception as e:
                self.logger.debug(f"[LOOP] Evaluator DB close: {e}")

    def _on_market_tick(self, generation: int):
        """
        Evaluate one signal tick and act on it — the event-driven
        replacement for one iteration of the old fixed-interval trading
        loop. Runs only on the evaluator thread (see _evaluator_loop), so
        no lock is needed here even though it's triggered from two
        different WS callback threads.

        `generation` must match self._active_generation — see
        _run_trading_loop()'s docstring comment on the generation token:
        this is what keeps a stale evaluator thread (one whose join()
        timed out during teardown) from acting on a NEWER cycle's data
        after finally finishing whatever tick left it stuck.
        """
        if not self._loop_active or self._active_generation != generation or not self.polymarket:
            return

        elapsed = time.time() - self._loop_start
        remaining = self._loop_end - time.time()

        # Signal: MultiFactorSignalEngine computes a BTC price-
        # to-beat z-score (volatility-normalized deviation from
        # window open) into a model probability, then applies
        # the user's direction-specific model-probability/ask/
        # edge/spread/time-window thresholds — see
        # MultiFactorConfig's docstring for the 2026-09-20
        # parameter set. Always called (even outside the
        # tradeable window) so its underlying BTC price history
        # stays continuous — real volatility doesn't reset just
        # because a new Polymarket window started.
        #
        # Binance specifically (user-specified 2026-09-20) —
        # see _start_market()'s btc_price_data capture for why,
        # and for keeping this consistent with the open/close
        # prices the same cycle is resolved against.
        current_btc = self.external.binance.get_price()
        yes_book_for_signal = self.polymarket.orderbook_cache_yes
        no_book_for_signal = self.polymarket.orderbook_cache_no
        evaluation = self.btc_signal_engine.evaluate(
            current_btc=current_btc.btc_usd if current_btc else None,
            # The feed's own timestamp for that price, not now(): it files
            # the price under the minute it was actually seen in when
            # MinuteVolatility builds its 1-minute closes.
            current_btc_ts=current_btc.timestamp if current_btc else None,
            opening_btc=self.btc_opening_price,
            seconds_remaining=remaining,
            elapsed=elapsed,
            market_status=self.market_data.status if self.market_data else None,
            yes_book=yes_book_for_signal, no_book=no_book_for_signal,
            trade_flow_yes=self.polymarket.trade_flow_ratio("yes"),
            trade_flow_no=self.polymarket.trade_flow_ratio("no"),
        )
        signal = evaluation.signal

        # Shadow measurement (shadow.py) sees every evaluation; it returns
        # at once unless the edge sits in a band not yet taken, and must
        # never disturb the real decision below.
        if self.shadow is not None:
            try:
                self.shadow.consider(evaluation, current_btc, time.time())
            except Exception as e:
                self.logger.debug(f"[SHADOW] consider failed: {e}")

        if signal == Signal.NO_SIGNAL:
            # Keep the single MOST INFORMATIVE rejected
            # evaluation seen so far this cycle (deepest gate
            # reached — see _reason_depth()), not just whatever
            # tick happened to run last. Frozen once a trade
            # opens (checked via _cycle_position_id) so a later
            # NO_SIGNAL tick can't overwrite the trade's own
            # record.
            self._record_cycle_reason(
                "NO_SIGNAL", evaluation.rejected_reason, evaluation=evaluation
            )

            # Throttled visibility into why nothing's firing —
            # without this, a quiet stretch is indistinguishable
            # from "engine's broken" vs. "market genuinely isn't
            # offering an A+ setup right now". Once per ~30s wall
            # clock, regardless of how many ticks fired in between
            # (a tick can now fire far more than once/second).
            now = time.time()
            if now - self._last_nosignal_log_time >= 30:
                self._last_nosignal_log_time = now
                detail = evaluation.rejected_reason or "no candidate direction yet"
                if evaluation.candidate_direction and evaluation.breakdown:
                    detail += (
                        f" (score={evaluation.score:.1f}, "
                        f"dir={evaluation.candidate_direction})"
                    )
                self.logger.info(f"[MULTI-SIGNAL] no trade: {detail}")
            return

        if signal and signal != Signal.NO_SIGNAL:
            self.session_stats["signals_detected"] += 1
            # execution_delay_max_ms (user-specified 2026-09-20,
            # BUY_UP): measured from right here — the earliest
            # point the signal is known — to the open_position()
            # call below.
            signal_detected_at = time.time()

            # Only one trade per market: once a position is open
            # this cycle, ignore further signals until it's
            # closed at market end (close_all_positions() in
            # _end_market() always clears it before the next
            # cycle starts, so this naturally resets per market).
            current_condition_id = (
                self.market_data.condition_id if self.market_data else None
            )
            if self.positions.has_open_position(condition_id=current_condition_id):
                self.logger.debug("[SIGNAL] Already in a position this market — skipping")
                return

            # The market's own fee schedule / minimum size don't match what
            # this bot assumes — see _market_trade_blocker().
            if self._market_blocker:
                self._record_cycle_reason(
                    signal.value,
                    f"execution: market not tradable — {self._market_blocker}",
                    evaluation=evaluation,
                )
                return

            # Price the entry from the CORRECT token's book: a
            # BUY_UP bet is bought on the YES/"Up" token, a
            # BUY_DOWN bet on the NO/"Down" token — they don't
            # share a price (yes_price + no_price ≈ 1.0), and
            # PnL is computed against whichever one we actually
            # hold.
            book = (
                self.polymarket.orderbook_cache_yes if signal == Signal.BUY_UP
                else self.polymarket.orderbook_cache_no
            )
            if (book and book.is_valid()
                    and book.best_bid is not None and book.best_ask is not None):
                # This bot always crosses the spread for an
                # immediate fill within its narrow signal
                # window, so it's always the taker, not the
                # maker. A real order for size=trade_size
                # doesn't necessarily fill entirely at
                # best_ask either — if that level's resting
                # size is thinner than our order, the rest
                # walks into worse price levels. vwap_fill_price
                # reproduces that against the book's real
                # depth instead of assuming unlimited size at
                # the top of book (see position_manager.py's
                # fee docstring for the matching taker-fee
                # change).
                mid_price = (book.best_bid + book.best_ask) / 2
                # Flat size, every trade (user-specified
                # 2026-09-20 — no dynamic sizing). The true VWAP
                # fill for this many shares is priced right
                # after, using this same size.
                trade_size = self.config.trade_size
                fill_price = book.vwap_fill_price(trade_size, "buy")
                if fill_price is None:
                    self.logger.warning(
                        f"[LOOP] {signal.value}: insufficient book depth "
                        f"for size {trade_size}, skipping trade"
                    )
                    self._record_cycle_reason(
                        signal.value,
                        f"execution: insufficient book depth for size {trade_size}",
                        evaluation=evaluation,
                    )
                    return
                entry_price = fill_price
                # Slippage in DOLLARS PER SHARE (the unit `edge` is in):
                # what this order pays over the book's mid — the spread it
                # crosses plus the depth its full size walks. There is NO
                # cap (user-specified 2026-09-29; the old max_slippage of one
                # tick is in backups/pre_no_slippage_cap_2026-09-29): the
                # order fills at this price however deep it walks, and the
                # cost is already inside the decision — evaluate() computed
                # the ask range, the fee and the edge from this same
                # volume-weighted price (candidate_ask), so a deeper walk has
                # already shrunk the edge that had to clear min_edge. It is
                # recorded per trade (slippage_* in signal_features), and P&L
                # is net of it because entry_price includes it.
                slippage = entry_price - mid_price
                slippage_cost = slippage * trade_size
                self.logger.info(
                    f"[LOOP] {signal.value}: fill={entry_price:.4f} "
                    f"mid={mid_price:.4f} slippage=${slippage:.4f}/share "
                    f"(${slippage_cost:.4f} on {trade_size} shares)"
                )
            else:
                # The live book is not usable right now, so this tick does
                # not trade. The next one is milliseconds away.
                #
                # There used to be a fallback here that priced entry off
                # the last mid/ask seen for this token, on the theory that
                # slightly stale real data beats skipping a tick. It was
                # removed because it never actually ran and could not have
                # run well:
                #
                #   * Unreachable in practice. MultiFactorSignalEngine
                #     .evaluate() already requires a valid book for the
                #     candidate token — and a VWAP fill for the full trade
                #     size — before it returns a signal at all, so by the
                #     time this code has a signal to price, the book was
                #     valid microseconds ago. The only way in is a race
                #     where the WS thread swaps in an empty book between
                #     evaluate()'s read and this one. Over a four-hour
                #     session at roughly 100 ticks/second it fired zero
                #     times.
                #   * Wrong thing to do if it ever did run. That race
                #     means the book is unknown at this instant, which is
                #     exactly when entering on a remembered price is worst.
                #   * It carried a silent failure. The signal_features
                #     snapshot below reads book.bids/book.asks; on the
                #     empty book that got us here that raises, and the
                #     except swallows it, so the position would have been
                #     opened with all of its diagnostic context missing.
                #
                # Dropping it also retired the four last_known_* caches
                # and the per-tick refresh that maintained them, which ran
                # on every one of those ticks for a consumer that never
                # fired.
                self.logger.debug(
                    f"[LOOP] {signal.value}: book not usable at execution time, skipping tick"
                )
                self._record_cycle_reason(
                    signal.value,
                    "execution: book not usable at execution time",
                    evaluation=evaluation,
                )
                return

            # Feed lag (2026-09-28): is_valid() above only knows when the
            # book arrived. Through a backed-up pipe it can arrive seconds
            # after Polymarket sent it and still look fresh — next to a
            # fresher BTC price, that is edge the exchange no longer
            # offers. See config.max_feed_lag_ms and feed_lag.py.
            signal_now = time.time()
            feed_behind = self.polymarket.feed_behind_s(signal_now)
            feed_behind_ms = None if feed_behind is None else feed_behind * 1000
            btc_behind = None
            if current_btc is not None and current_btc.exchange_ts is not None:
                btc_behind = BINANCE_FEED_LAG.behind_s(current_btc.exchange_ts, signal_now)
            btc_behind_ms = None if btc_behind is None else btc_behind * 1000
            if feed_behind_ms is None or feed_behind_ms > self.config.max_feed_lag_ms:
                lag_text = "unknown" if feed_behind_ms is None else f"{feed_behind_ms:.0f}ms"
                self.logger.warning(
                    f"[LOOP] {signal.value}: book {lag_text} behind Polymarket "
                    f"(max {self.config.max_feed_lag_ms:.0f}ms), skipping trade"
                )
                self._record_cycle_reason(
                    signal.value,
                    f"execution: book {lag_text} behind Polymarket "
                    f"(max {self.config.max_feed_lag_ms:.0f}ms)",
                    evaluation=evaluation,
                    extra={"feed_behind_ms": feed_behind_ms, "btc_behind_ms": btc_behind_ms},
                )
                return

            # Same blind spot on the other input (2026-09-29): get_price()
            # only bounds how long ago the BTC price ARRIVED, and through
            # the same congested tunnel Binance ran 2-5 s behind. A stale
            # BTC price makes the model lag a market that has already moved
            # correctly, so the "edge" it sees is the market being right.
            # Gated on how late the last Binance message arrived (its
            # transport lag), not on time since the last print: aggTrade
            # can be quiet for seconds when nothing trades, which is not
            # staleness — and a stalled tunnel stalls the Polymarket feed
            # too, which the check above already catches. Unknown lag (a
            # REST fallback price) is not known to be fresh, so it blocks.
            btc_lag_ms = (None if current_btc is None or current_btc.feed_lag_s is None
                          else current_btc.feed_lag_s * 1000)
            if btc_lag_ms is None or btc_lag_ms > self.config.max_btc_feed_lag_ms:
                lag_text = ("lag unknown (not from the Binance stream)" if btc_lag_ms is None
                            else f"arrived {btc_lag_ms:.0f}ms late")
                self.logger.warning(
                    f"[LOOP] {signal.value}: BTC price {lag_text} "
                    f"(max {self.config.max_btc_feed_lag_ms:.0f}ms), skipping trade"
                )
                self._record_cycle_reason(
                    signal.value,
                    f"execution: BTC price {lag_text} "
                    f"(max {self.config.max_btc_feed_lag_ms:.0f}ms)",
                    evaluation=evaluation,
                    extra={"feed_behind_ms": feed_behind_ms, "btc_feed_lag_ms": btc_lag_ms},
                )
                return

            self.logger.info(f"[SIGNAL] {signal.value} @ entry {entry_price:.4f}")

            # Phase 2 diagnostics — observability only, not yet
            # gating the trade decision above. Logged AND
            # persisted (signal_features, below) so each real
            # position has this context attached for later
            # review/training (Phase 3) before deciding how/
            # whether to fold any of it into the decision itself.
            # All of this was already computed inside evaluate()
            # to produce the score/decision above — reading it
            # back off `evaluation` instead of recomputing keeps
            # this snapshot consistent with what actually drove
            # the trade, and avoids a second round of OBI/
            # trade-flow/TWAP calls for the same instant.
            try:
                token_side = "yes" if signal == Signal.BUY_UP else "no"
                btc_now = current_btc.btc_usd if current_btc else None

                signal_features = {
                    "token_side": token_side,
                    "score": evaluation.score,
                    "score_breakdown": evaluation.breakdown,
                    "distance_z": evaluation.distance_z,
                    "returns": evaluation.returns,
                    "acceleration": evaluation.acceleration,
                    "token_return": evaluation.token_return,
                    "model_prob": evaluation.model_prob,
                    "edge": evaluation.edge,
                    "vol_per_sqrt_s": evaluation.vol_per_sqrt_s,
                    # log-only, see signal_engine.ShortVolatility
                    "short_vol_per_sqrt_s": evaluation.short_vol_per_sqrt_s,
                    "model_prob_short_vol": evaluation.model_prob_short_vol,
                    "edge_short_vol": evaluation.edge_short_vol,
                    "position_size": trade_size,
                    "obi_1": evaluation.obis.get("obi_1"),
                    "obi_5": evaluation.obis.get("obi_5"),
                    "obi_10": evaluation.obis.get("obi_10"),
                    "trade_flow": evaluation.trade_flow,
                    "btc_now": btc_now,
                    "best_bid": book.best_bid, "best_ask": book.best_ask,
                    # How current both inputs were at the signal (see the
                    # feed-lag check above): Polymarket books vs the
                    # exchange, BTC vs Binance, the lag of this token's
                    # own last update, and the clock baseline they are
                    # measured against. A trade whose book was far older
                    # than its BTC price is the suspect kind.
                    "feed_behind_ms": feed_behind_ms,
                    "btc_behind_ms": btc_behind_ms,
                    "btc_feed_lag_ms": btc_lag_ms,
                    "book_feed_lag_ms": (None if book.feed_lag_s is None
                                         else book.feed_lag_s * 1000),
                    "feed_lag_baseline_ms": (None if POLYMARKET_FEED_LAG.baseline_s is None
                                             else POLYMARKET_FEED_LAG.baseline_s * 1000),
                    # Full order book at the exact moment of
                    # entry — pure record-keeping for later P&L/
                    # execution-quality analysis (e.g. "was
                    # there really enough depth", "what did the
                    # book actually look like"), never read
                    # back into the decision path itself. Capped
                    # at 10 levels/side so one very deep book
                    # can't bloat this row; this market's books
                    # have never been seen anywhere near that
                    # deep.
                    "book_snapshot": {
                        "bids": book.bids[-10:],
                        "asks": book.asks[-10:],
                    },
                }

                self.logger.info(
                    f"[SIGNAL-DIAG] score={evaluation.score:.1f} "
                    f"OBI(1/5/10)={_fmt(evaluation.obis.get('obi_1'))}/"
                    f"{_fmt(evaluation.obis.get('obi_5'))}/{_fmt(evaluation.obis.get('obi_10'))} "
                    f"trade_flow={_fmt(evaluation.trade_flow)} BTC={_fmt(btc_now, 2)}"
                )
            except Exception as e:
                self.logger.debug(f"[SIGNAL-DIAG] failed: {e}")
                signal_features = {"score": evaluation.score}

            # execution_delay_max_ms (user-specified 2026-09-20,
            # BUY_UP) — everything between signal_detected_at
            # and here (book pricing, signal_features build)
            # is synchronous/in-memory,
            # so this is normally sub-millisecond; a real
            # overrun means something (GC pause, GIL
            # contention, a slow log handler) made the price
            # this was computed against stale enough that
            # entering on it now is worse than not trading.
            execution_delay_ms = (time.time() - signal_detected_at) * 1000
            effective_max_delay_ms = (
                self.config.execution_delay_max_ms_up if signal == Signal.BUY_UP
                else self.config.execution_delay_max_ms_down
            )
            if execution_delay_ms > effective_max_delay_ms:
                self.logger.warning(
                    f"[LOOP] {signal.value}: execution delay {execution_delay_ms:.0f}ms "
                    f"> max {effective_max_delay_ms:.0f}ms, skipping trade"
                )
                self._record_cycle_reason(
                    signal.value,
                    f"execution: delay {execution_delay_ms:.0f}ms above max "
                    f"{effective_max_delay_ms:.0f}ms",
                    evaluation=evaluation,
                    extra={"execution_delay_ms": execution_delay_ms},
                )
                return

            # ─── ORDER SUBMISSION BELONGS HERE ───────────────────────
            # Real-money execution goes at THIS point — sign the CLOB
            # order and submit it (py-clob-client-v2) — and
            # open_position() below runs only once the exchange confirms
            # a fill, with the real order id and the real fill price.
            #
            # Not after it. open_position() writes the position, its taker
            # fee and its P&L to bot_trades.db unconditionally, so a
            # submission placed after that call leaves the database
            # asserting a position on any failure — rejected order,
            # timeout, insufficient balance. The mode check used to sit
            # after open_position() for exactly that reason and produced
            # the silent-fabrication behaviour described in
            # _assert_execution_path_exists().
            #
            # Until such a path exists, real-money mode has no business
            # reaching a database write. _assert_execution_path_exists()
            # already refuses it at startup; this is the same invariant
            # asserted where the money would actually move, so removing
            # or bypassing the startup check cannot quietly resurrect the
            # old behaviour.
            if not self.config.is_paper_trading and not self.config.is_dry_run:
                raise NotImplementedError(
                    "real-money order submission is not implemented — "
                    "refusing to record a position for an order that was "
                    "never placed (see _assert_execution_path_exists)"
                )

            # Paper fill: we are a TAKER, so the order crosses the spread
            # and matches at once against the book seen at the signal. The
            # fill price is entry_price above — the volume-weighted average
            # over the depth the FULL order walks (book.vwap_fill_price) —
            # so slippage is already in it (nothing caps it; the edge gate
            # priced it in).
            # No latency wait and no price tolerance (user-specified
            # 2026-09-29; the earlier model — 500 ms wait, live book, a limit
            # one tick above the worst level — is in
            # backups/pre_taker_fill_2026-09-29). What keeps the signal-time
            # book trustworthy is the pair of feed-lag guards above.
            best_ask = book.best_ask
            depth_slippage = entry_price - best_ask  # per share, from walking the depth
            signal_features.update({
                "fill_model": "instant",
                "signal_price": entry_price,
                "fill_price": entry_price,
                "fill_best_ask": best_ask,
                "slippage_vs_best_ask": depth_slippage,
                "slippage_vs_mid": slippage,  # spread crossing + depth walk
                "slippage_cost": depth_slippage * trade_size,
            })
            self.logger.info(
                f"[LOOP] {signal.value}: paper taker fill {entry_price:.4f} "
                f"(best ask {best_ask:.4f}, depth slippage {depth_slippage:+.4f}/share "
                f"= ${depth_slippage * trade_size:.2f} on {trade_size} shares)"
            )

            pos_id = self.positions.open_position(
                order_id=f"order_{int(time.time()*1000)}",
                side="BUY",
                signal_type=signal.value,
                entry_price=entry_price,
                size=trade_size,
                signal_features=signal_features,
                mid_price=mid_price,
                condition_id=self.market_data.condition_id if self.market_data else None
            )

            if pos_id:
                self.session_stats["trades_executed"] += 1

                # Freeze this cycle's decision snapshot on the
                # trade that actually fired, so the
                # market_sessions row _end_market() saves
                # reflects the real decision, not whatever
                # NO_SIGNAL tick happened to run last.
                self._cycle_signal_type = signal.value
                self._cycle_score = evaluation.score
                self._cycle_position_id = pos_id
                try:
                    self._cycle_signal_features_json = json.dumps(signal_features)
                except Exception:
                    self._cycle_signal_features_json = None

        # Log progress — about every 30s wall clock, same reasoning as
        # the no-signal throttle above.
        now = time.time()
        if now - self._last_progress_log_time >= 30:
            self._last_progress_log_time = now
            self.logger.debug(
                f"[LOOP] Elapsed: {elapsed:.0f}s, "
                f"Remaining: {remaining:.0f}s, "
                f"Signals: {self.session_stats['signals_detected']}"
            )
    
    # ========================================================================
    # CLEANUP
    # ========================================================================
    
    def _cleanup(self):
        """
        Shut down in dependency order: everything that can still WRITE
        stops before the database it writes to closes, and everything that
        can still produce a tick stops before the position state it would
        act on goes away.

        The old version skipped most of that. It stopped only the
        dashboard, then closed positions and the DB — leaving the
        external price feeds, the resolution watcher, this cycle's
        PolymarketBot and any pre-warmed one still running. They are
        daemon threads, so they died eventually with the process, but in
        between they kept reconnecting, kept writing into caches, and in
        the watcher's case could still write to a closed database. Proof
        it actually happened: a [MULTI-SIGNAL] evaluation was logged 16
        seconds AFTER "[BOT] Shutdown complete" on 2026-09-22.
        """

        def _stop(label: str, fn):
            try:
                fn()
                self.logger.info(f"[BOT] {label} stopped")
            except Exception as e:
                self.logger.warning(f"[BOT] {label} shutdown warning: {e}")

        try:
            self.logger.info("[BOT] Shutting down...")

            # 1. Tick producers first — a PolymarketBot book event or a
            #    Binance price tick is what wakes signal evaluation, so
            #    these stop before anything they could act on is torn down.
            #    Both the active bot and a pre-warmed one that never got
            #    promoted (its threads have no other teardown path).
            for label, bot in (("Market WS", self.polymarket),
                               ("Pre-warmed market WS", self.next_polymarket)):
                if bot:
                    def _stop_bot(b=bot):
                        b.should_run = False
                        b.end_market()
                    _stop(label, _stop_bot)
            self.next_polymarket = None

            # 1b. Perp momentum shadow: its own feed and short-lived DB writes.
            if self.perp_shadow:
                _stop("Perp momentum shadow", self.perp_shadow.stop)

            # 2. Database writers. Both of these write settlement results,
            #    so both must be down before the DB closes below.
            if self.resolution_watcher:
                _stop("Resolution watcher", self.resolution_watcher.stop)

            if self._orphan_recovery_thread and self._orphan_recovery_thread.is_alive():
                def _stop_orphan_recovery():
                    self._orphan_recovery_stop.set()
                    # Bounded wait: the thread can be inside a 10s HTTP
                    # timeout, and shutdown should not block on that. If it
                    # does not finish in time it is a daemon and dies with
                    # the process — the stop flag means it will not start
                    # another lookup, and its database writes go through a
                    # connection of its own rather than the one closed
                    # below.
                    self._orphan_recovery_thread.join(timeout=3)
                    if self._orphan_recovery_thread.is_alive():
                        raise TimeoutError("still finishing an HTTP lookup after 3s")
                _stop("Orphan recovery", _stop_orphan_recovery)

            # 3. External price feeds.
            if self.external:
                _stop("External connections", self.external.stop)

            # 3b. Market tape recorder (separate process, own database).
            if self.market_recorder is not None:
                _stop("Market recorder", self._stop_market_recorder)

            # 4. Dashboard, before the DB it reads from goes away.
            if self.dashboard_server:
                def _stop_dash():
                    self.dashboard_server.shutdown()
                    self.dashboard_server.server_close()
                _stop("Dashboard", _stop_dash)

            # 5. Close positions — no known resolution at this point (bot
            #    interrupted mid-market), so void each one at its own entry
            #    price (pnl=0) rather than a fabricated outcome. See
            #    PositionManager.close_all_positions's docstring.
            if self.positions and self.market_open:
                self.logger.info("[BOT] Closing open positions...")
                self.positions.close_all_positions()

            # 6. Database last: every writer above has stopped by now.
            if self.db:
                self.db.close()
                self.logger.info("[BOT] Database closed")

            self.logger.info("[BOT] Shutdown complete")

        except Exception as e:
            self.logger.error(f"[BOT] Cleanup failed: {e}", exc_info=True)
    
    # ========================================================================
    # MAIN RUN
    # ========================================================================
    
    def _assert_execution_path_exists(self):
        """
        Refuse to start in real-money mode, because this build has no way
        to place an order.

        There is no order-submission path in this codebase at all: nothing
        POSTs to clob.polymarket.com. The signing/sending helpers that once
        stood in for one (a CLOB V1 order signer the exchange would reject,
        a raw-transaction signer and sender) were never called and were
        removed 2026-09-29; orders must go through py-clob-client-v2.

        What made this dangerous is that open_position() runs BEFORE that
        branch and does not check the mode: in real-money mode the bot
        logged "REAL TRADING MODE — USING ACTUAL CAPITAL", wrote a
        position, a taker fee and a P&L to bot_trades.db, and drew a
        profit curve on the dashboard — while placing no order. That is
        worse than crashing: it silently fabricates a trading history the
        operator has no reason to doubt.

        So the mode is refused outright rather than half-honoured. Paper
        and dry-run modes are unaffected; they are what this build
        actually implements. Remove this check only together with a real,
        tested submission path — and see the ordering note at the
        execution site in _on_market_tick() before wiring one up.
        """
        if self.config.is_paper_trading or self.config.is_dry_run:
            return

        raise NotImplementedError(
            "\n"
            "  REAL-MONEY MODE IS NOT IMPLEMENTED IN THIS BUILD.\n"
            "\n"
            "  The bot is configured to trade actual capital, but it has no\n"
            "  order-submission path: nothing in this codebase sends an order\n"
            "  to Polymarket. Running anyway would record positions, fees and\n"
            "  P&L in bot_trades.db for trades that were never placed.\n"
            "\n"
            "  Set PAPER_TRADING=true (or DRY_RUN=true) to run the modes this\n"
            "  build does implement.\n"
        )

    def run(self, max_cycles: Optional[int] = None):
        """
        Main bot run.

        Runs CONTINUOUSLY: loads the currently-live 5-minute BTC market,
        trades it for its ~5 minute window, settles it, then immediately
        loads the NEXT 5-minute market and repeats. This keeps going
        forever (Ctrl+C / SIGTERM to stop) unless max_cycles is given.

        Args:
            max_cycles: if set, stop after this many 5-minute cycles
                        (mainly useful for testing). None = run forever.

        Raises:
            NotImplementedError: if configured for real-money trading,
                which this build cannot actually execute — see
                _assert_execution_path_exists().
        """

        # Before anything touches the network, wallet or DB: refuse to run
        # in a mode this build cannot honour. See the method's docstring.
        self._assert_execution_path_exists()

        try:
            # Initialize shared modules ONCE (DB, wallet, external feeds,
            # signal engine, market loader). Only the PolymarketBot/WS
            # connection is recreated every cycle, inside _load_market().
            if not self._init_modules():
                self.logger.error("[BOT] Module initialization failed")
                return False

            self.running = True
            if self.config.record_order_book:
                threading.Thread(target=self._book_recorder_loop, daemon=True,
                                 name="BookRecorder").start()
            # Resume the cycle counter from wherever the last run left off
            # (persisted per-cycle in market_sessions) instead of always
            # restarting at 1 — see database.py's get_last_cycle_number.
            cycle_count = self.db.get_last_cycle_number()

            # Consecutive failed market loads, and when that streak began.
            # Polymarket has been unreachable from this machine for hours at
            # a time (2026-09-24/25/27, ~14 h in total) while every other
            # host worked. A flat 5s retry spent those hours making ~470
            # attempts an hour at ~9 log records each; backing off and
            # logging the streak instead of every attempt keeps the log
            # readable and shows how long each outage actually lasted.
            load_failures = 0
            outage_started = None
            outage_last_report = 0.0

            while self.running:
                # Only commit the increment once the market has loaded:
                # a failed load retries, and counting those retries
                # inflated the counter far past the real number of cycles.
                next_cycle = cycle_count + 1
                if load_failures == 0:
                    self.logger.info(
                        f"\n{'='*80}\n"
                        f"[CYCLE {next_cycle}] Loading next 5-minute BTC market...\n"
                        f"{'='*80}"
                    )

                # Load the currently-live 5-minute market
                if not self._load_market():
                    load_failures += 1
                    # 5, 10, 20, then 30s. Capped low on purpose: once
                    # access comes back, every second of waiting is a second
                    # of a 5-minute window the bot sits out.
                    delay = min(5 * 2 ** (load_failures - 1), 30)
                    now = time.time()
                    if load_failures == 1:
                        outage_started = now
                        outage_last_report = now
                        self.logger.error(
                            f"[BOT] Market loading failed — retrying in {delay}s "
                            f"(backing off to every 30s while it keeps failing)"
                        )
                    elif now - outage_last_report >= 300:
                        outage_last_report = now
                        self.logger.warning(
                            f"[BOT] Market loading still failing — {load_failures} attempts "
                            f"over {(now - outage_started) / 60:.0f} min, retrying every {delay}s"
                        )
                    # time.sleep() alone would outlast a Ctrl+C/SIGTERM by up
                    # to the full delay: the handler only clears
                    # self.running, and sleep resumes after it (PEP 475).
                    wake_at = time.monotonic() + delay
                    while self.running and time.monotonic() < wake_at:
                        time.sleep(0.5)
                    continue

                if load_failures:
                    self.logger.warning(
                        f"[BOT] Market loading recovered after "
                        f"{(time.time() - outage_started) / 60:.1f} min "
                        f"({load_failures} failed attempts)"
                    )
                    load_failures = 0
                    outage_started = None

                # A shutdown signal may have arrived while _load_market()
                # (a network call) was in flight — check before starting
                # a whole new cycle instead of only at the bottom of the
                # loop.
                if not self.running:
                    break

                cycle_count = next_cycle
                self._current_cycle_number = cycle_count

                # Start this cycle's market/session
                self._start_market()

                # Run the trading loop for this ~5 minute window
                self._run_trading_loop()

                # Settle/close positions, log session stats
                self._end_market()

                self.logger.info(f"[CYCLE {cycle_count}] Complete.")

                if max_cycles is not None and cycle_count >= max_cycles:
                    self.logger.info(
                        f"[BOT] Reached max_cycles={max_cycles}, stopping."
                    )
                    break

                if not self.running:
                    break

                # Brief pause before rolling into the next 5-minute window
                # (the next market's start time will already have arrived
                # or be arriving shortly, since windows are back-to-back).
                time.sleep(1)

            self.logger.info(
                f"[BOT] Bot completed successfully after {cycle_count} cycle(s)"
            )
            return True

        except Exception as e:
            self.logger.error(f"[BOT] Fatal error: {e}", exc_info=True)
            return False

        finally:
            self._cleanup()

# ============================================================================
# SINGLE-INSTANCE LOCK
# ============================================================================

_LOCKFILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".polybot.lock")

# The open file object holding the flock. It lives at module scope on
# purpose: an flock is released the moment its file object is closed or
# garbage-collected, so this reference IS the lock. Kept here rather than
# in a local variable in main() because a local looked unused to static
# analysis — pyflakes reported it as such — and deleting it, by hand or by
# an automated "unused variable" fix, would silently let a second bot run
# against the same market and database.
_SINGLETON_LOCK_HANDLE = None


def _acquire_singleton_lock():
    """
    Refuse to start a second bot process against the same real market and
    the same bot_trades.db — two live instances were observed running
    concurrently in this same directory (2026-09-19/20), each independently
    opening WebSocket connections to Binance/Polymarket and
    hammering the same endpoints, which produced 3500+ connection errors
    in a single session and made trading decisions nearly impossible for
    both processes.

    Uses flock() rather than a plain "does this PID exist" pidfile check:
    the OS releases an flock automatically the instant the holding process
    exits for ANY reason — clean shutdown, crash, kill -9, power loss — so
    there's no stale-lock cleanup to get wrong and no PID-reuse race to
    worry about. The returned file object must be kept alive (assigned to
    a variable, not thrown away) for as long as the lock should be held;
    it releases as soon as it's garbage-collected or the process exits.

    Exits the process immediately if another instance already holds the
    lock — this must run before any network/wallet/DB setup, so a second
    launch fails fast and loud instead of quietly degrading both bots'
    connections.
    """
    # "a+", not "w": open(path, "w") TRUNCATES immediately, before flock
    # is even attempted. A second launch therefore wiped the running
    # instance's PID out of the file and only then discovered it could not
    # have the lock — leaving the message below telling the operator to go
    # find the other process, having just destroyed the record of which
    # process it is. Verified: holder wrote '12345', the rejected second
    # process left the file empty.
    lock_file = open(_LOCKFILE_PATH, "a+")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_file.seek(0)
        try:
            holder = lock_file.read().strip() or "unknown"
        except Exception:
            holder = "unknown"
        print(
            f"\n[BOT] Another instance (PID {holder}) already holds "
            f"{_LOCKFILE_PATH} — refusing to start a second one (see run.py's "
            f"_acquire_singleton_lock docstring for why). Stop it first if you "
            f"actually want to restart.\n"
        )
        sys.exit(1)

    # We hold the lock: now it is safe to replace the contents with our
    # own PID. Truncating here rather than at open() is the whole point —
    # only the winner rewrites the file.
    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()

    global _SINGLETON_LOCK_HANDLE
    _SINGLETON_LOCK_HANDLE = lock_file
    return lock_file


def main():
    """Main entry point"""

    # Must happen before anything else touches the network, wallet, or DB —
    # see _acquire_singleton_lock's docstring. It parks the file object in
    # _SINGLETON_LOCK_HANDLE, so the lock outlives this call without a
    # local variable that only looks decorative.
    _acquire_singleton_lock()

    try:
        # Load configuration
        config = Config()
        
        # Setup logging
        logger = setup_logging(config)

        # Before any thread starts — see Config.gil_switch_interval_ms.
        if config.gil_switch_interval_ms > 0:
            sys.setswitchinterval(config.gil_switch_interval_ms / 1000.0)
        logger.info(
            f"[BOT] GIL switch interval: {sys.getswitchinterval() * 1000:.2f} ms, "
            f"evaluator min interval: {config.evaluator_min_interval_ms:.0f} ms"
        )

        # Print configuration
        config.print_config()
        
        # Check mode
        if config.is_paper_trading:
            logger.warning("[BOT] PAPER TRADING MODE")
        elif config.is_dry_run:
            logger.warning("[BOT] DRY RUN MODE")
        else:
            logger.warning("[BOT] REAL TRADING MODE — USING ACTUAL CAPITAL")
        
        # Run bot continuously — keeps loading each new 5-minute BTC
        # market and trading it, forever, until Ctrl+C / SIGTERM.
        logger.info(
            "[BOT] Running CONTINUOUSLY — will keep trading every new "
            "5-minute BTC market until stopped (Ctrl+C to stop)"
        )
        controller = BotController(config)
        success = controller.run(max_cycles=None)
        
        # Print final stats
        if controller.external:
            print("\n" + controller.external.export_stats())
        
        if controller.btc_signal_engine:
            # MultiFactorSignalEngine's own stats dict (see signal_engine.py
            # ~line 757) — deliberately NOT the same shape as the older
            # BTCDeviationSignalEngine/SignalEngine stats (no
            # "signals_calculated"/"used_volatility_threshold" keys here).
            # Using .get() with 0 defaults so a future stats-dict change on
            # either side can't turn a cosmetic end-of-run summary into a
            # crash that skips the rest of this shutdown printout again.
            bs = controller.btc_signal_engine.stats
            print(
                "\n" + "="*60 + "\nMULTI-FACTOR SIGNAL\n" + "="*60 +
                f"\nEvaluations:             {bs.get('evaluations', 0)}"
                f"\n  BUY_UP:                {bs.get('buy_up_signals', 0)}"
                f"\n  BUY_DOWN:              {bs.get('buy_down_signals', 0)}"
                f"\nRejected — market status:  {bs.get('rejected_market_quality', 0)}"
                f"\nRejected — time window:    {bs.get('rejected_time_window', 0)}"
                f"\nRejected — no BTC price:   {bs.get('rejected_no_price', 0)}"
                f"\nRejected — no distance:    {bs.get('rejected_no_distance', 0)}"
                f"\nRejected — no valid book:  {bs.get('rejected_no_book', 0)}"
                f"\nRejected — book depth:     {bs.get('rejected_depth', 0)}"
                f"\nRejected — model_prob:     {bs.get('rejected_model_prob', 0)}"
                f"\nRejected — ask range:      {bs.get('rejected_ask_range', 0)}"
                f"\nRejected — edge:           {bs.get('rejected_edge', 0)}\n" + "="*60
            )

        if controller.resolution_watcher:
            rw_stats = controller.resolution_watcher.get_stats()
            print(
                "\n" + "="*60 + "\nRESOLUTION WATCHER (our call vs Polymarket's official)\n" + "="*60 +
                f"\nResolutions checked: {rw_stats['resolutions_seen']}"
                f"\n  via WS:            {rw_stats['via_ws']}"
                f"\n  via REST fallback: {rw_stats['via_rest']}"
                f"\nMatches:             {rw_stats['matches']}"
                f"\nMismatches:          {rw_stats['mismatches']}"
                f"\nStill unmatched:     {rw_stats['unmatched']}\n" + "="*60
            )

        # Exit
        sys.exit(0 if success else 1)
    
    except ValueError as e:
        print(f"Configuration error: {e}")
        print("\nPlease set required environment variables in .env file:")
        print("  PRIVATE_KEY=0x...")
        print("  WALLET_ADDRESS=0x...")
        sys.exit(1)
    
    except NotImplementedError as e:
        # A deliberate refusal, not a crash — printed distinctly so it
        # doesn't read as a bug the operator should try to work around.
        # See BotController._assert_execution_path_exists().
        print(f"\n[BOT] REFUSED TO START{e}")
        sys.exit(1)

    except KeyboardInterrupt:
        print("\nShutdown by user")
        sys.exit(0)
    
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
