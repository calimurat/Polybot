"""
CONFIG — Master Configuration File
All settings, API keys, wallet info, token IDs, parameters

Production-grade: validation, environment variables, defaults, logging
"""

import os
import stat
import logging
from dotenv import load_dotenv
load_dotenv()
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("Config")

# ============================================================================
# ENUMS
# ============================================================================

class Environment(Enum):
    """Environment"""
    DEVELOPMENT = "development"
    PRODUCTION = "production"

class LogLevel(Enum):
    """Log levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"

# ============================================================================
# CONFIG DATACLASS
# ============================================================================

# Accepted spellings for the boolean environment flags below. The old
# parse was `os.getenv(NAME, "").lower() == "true"`, which treated every
# other spelling — "1", "yes", "on", and notably "true " with the
# trailing whitespace that .env files collect invisibly — as False. For
# PAPER_TRADING, False means real money, so each of those was a silent
# upgrade to live trading.
_TRUTHY = frozenset({"1", "true", "yes", "y", "on"})
_FALSEY = frozenset({"0", "false", "no", "n", "off"})

# Second, deliberate confirmation required alongside PAPER_TRADING=false
# before the bot will start in real-money mode — see Config._validate().
CONFIRM_REAL_MONEY_ENV = "I_UNDERSTAND_REAL_MONEY"
CONFIRM_REAL_MONEY_VALUE = "yes"


def _warn_if_env_file_is_exposed():
    """
    Warn loudly if the .env file is readable by anyone but its owner.

    That file holds PRIVATE_KEY — a real Ethereum key, not a placeholder —
    so its permissions are the only thing standing between this wallet and
    every other account on the machine. It was found at 0644
    (world-readable) on 2026-09-22, while .env.example beside it was 0600.

    A warning rather than a hard failure: this is a one-line fix the
    operator can make in a second, and refusing to start over a permission
    bit would be disproportionate. But it runs on EVERY start, because the
    permissions drift back easily — recreating the file, copying it from a
    template, or restoring it from a backup all reset the mode.
    """
    if os.name == "nt":
        return  # POSIX mode bits don't mean the same thing here
    try:
        from dotenv import find_dotenv
        path = find_dotenv(usecwd=False)
        if not path or not os.path.exists(path):
            return
        mode = os.stat(path).st_mode
        exposed = mode & (stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
        if exposed:
            logger.warning(
                "[CONFIG] %s is mode %s — readable by other accounts on this "
                "machine, and it contains PRIVATE_KEY. Restrict it with:  "
                "chmod 600 %s",
                path, oct(stat.S_IMODE(mode)), path,
            )
    except Exception:
        # Never let a permissions check stop the bot from starting.
        pass


def _env_bool(name: str, default: bool) -> bool:
    """
    Read a boolean environment variable.

    Three behaviours that the previous one-liner got wrong:
      - variable ABSENT      -> return `default` (the caller's safe value),
                                not False
      - surrounding junk     -> tolerated: whitespace and wrapping quotes
                                are stripped before comparing
      - unrecognized value   -> ValueError, not a silent False

    The last one matters most. These flags decide whether real capital is
    at risk, so guessing at "maybe" is not acceptable: a value the parser
    does not understand means the operator's intent is unknown, and the
    bot must refuse to start rather than pick a side for them.
    """
    raw = os.getenv(name)
    if raw is None:
        return default

    value = raw.strip().strip('"').strip("'").strip().lower()
    if value in _TRUTHY:
        return True
    if value in _FALSEY:
        return False
    if value == "":
        return default

    raise ValueError(
        f"{name}={raw!r} is not a recognized boolean. "
        f"Use one of {sorted(_TRUTHY)} or {sorted(_FALSEY)}. "
        f"Refusing to guess, because this flag decides whether real "
        f"capital is at risk."
    )


@dataclass
class Config:
    """
    Master configuration for entire bot
    
    Load from environment variables or use defaults
    """
    
    # ===== ENVIRONMENT =====
    environment: str = Environment.PRODUCTION.value
    
    # ===== WALLET (MetaMask EOA) =====
    private_key: str = ""           # 0x... format (from environment)
    wallet_address: str = ""        # 0x... format (Polygon)
    signature_type: int = 0         # 0=EOA, 3=POLY_1271
    
    # ===== POLYMARKET MARKET =====
    # NOTE: Real Polymarket question titles look like:
    #   "Bitcoin Up or Down - September 13, 4:20PM-4:25PM ET"
    # so we match on a substring, not an exact code like "BTCUSD-5M".
    market_name: str = "Bitcoin Up or Down"
    market_duration_seconds: int = 300  # 5 minutes
    
    # Token IDs (dynamic, loaded from Gamma API at startup)
    token_yes_id: str = ""          # Loaded dynamically
    token_no_id: str = ""           # Loaded dynamically
    market_id: str = ""
    condition_id: str = ""
    
    # ===== TRADING =====
    # Flat position size, every trade, both directions (user-specified
    # 2026-09-23: 30, up from the V8.5 set's 27). No dynamic sizing (not
    # risk-per-trade-%, not Kelly): see MultiFactorConfig's docstring for
    # why RISK_PER_TRADE_PCT_UP/DOWN from that same set was dropped
    # (contradicted this flat value, and needs a real bankroll this paper-
    # trading bot doesn't query — already rejected once, "kelly sistemini
    # sil ... her markette 30 share işlem açacak").
    #
    # Always exactly this many shares, never fewer (user-specified
    # 2026-09-29: "sabit 30 hisse ... slippage de olsa alacak anlık"). On a
    # thin book the order walks deeper and pays a worse average price; that
    # slippage is accepted and priced into the edge (see the PAPER FILL
    # MODEL note below). The one case with no trade is a book whose whole
    # ask side holds fewer than trade_size shares, since there is nothing to
    # buy them from: 23 of 116,214 book snapshots recorded inside the
    # trading window (0.02%). A partial-fill variant (take whatever shares
    # exist, down to the market's minimum order size) was built the same day
    # and dropped at the user's request: backups/partial_fill_version_2026-09-29.
    trade_size: int = 30
    min_spread: float = 0.01        # Min spread to trade

    # No slippage cap (user-specified 2026-09-29). There used to be a
    # max_slippage of $0.01/share (fill price minus the book's mid at the
    # signal) that refused deeper trades; now the bot fills at the
    # volume-weighted price of its whole order however far that walks the
    # book. The cost is priced in rather than gated: the edge the engine
    # checks (model_prob - fill price - fee) already uses that same price.
    # The old cap is in backups/pre_no_slippage_cap_2026-09-29.

    # ===== PAPER FILL MODEL (2026-09-29, user-specified) =====
    # The bot is a taker: a paper order fills immediately, at the book seen
    # at the signal, at the volume-weighted price over the depth the whole
    # order walks — so slippage is inside the fill price and never a reason
    # to refuse. No latency wait, no price tolerance. Per trade:
    # signal_features slippage_vs_best_ask, slippage_vs_mid, slippage_cost. The model before this (paper_fill_
    # latency_ms = 500 ms wait against the live book, paper_fill_price_
    # tolerance = 0.01 limit above the worst level; about half the shadow
    # attempts did not fill) is in backups/pre_taker_fill_2026-09-29.

    # ===== FEED LAG GUARD (2026-09-28) =====
    # A signal only executes when the cached books are at most this far
    # behind Polymarket — measured from
    # the exchange's own message timestamps (feed_lag.py), not from when
    # the messages arrived, which is all Orderbook.is_valid can see.
    # Measured that day through the VPN tunnel: pongs 0.1-8.6 s late, the
    # feed up to 78 s behind with nothing cutting the connection, while
    # the Binance price it was compared against ran 2-5 s behind. A book
    # older than the BTC price shows edge the exchange no longer offers,
    # so paper P&L from such books says nothing about real fills.
    # 500 ms: above the 100-300 ms seen (p95) on an uncongested stretch of
    # the same path, and well under the multi-second backlogs this is
    # meant to catch. The value at each trade is recorded (signal_features:
    # feed_behind_ms), so the limit can be tuned from data.
    max_feed_lag_ms: float = 500.0
    # The same limit on the BTC side (2026-09-29): a signal only executes
    # if the last Binance price reached us at most this late (transport
    # lag, feed_lag.py). Recorded per trade as btc_feed_lag_ms.
    max_btc_feed_lag_ms: float = 500.0

    # ===== SHADOW MEASUREMENT (2026-09-29) =====
    # Edge bands (lo <= edge < hi) in which shadow.py records the trade
    # the strategy WOULD have made, beside the real paper trades (which
    # still use MultiFactorConfig's min_edge_up/min_edge). Same gates,
    # guards and simulated fill; one fill per window per band;
    # shadow_trades table, judged by paper_report.py. Empty turns it off.
    # Every 1-cent band from 0.01 to 0.50, plus one open band 0.50-1.00,
    # each measured on its own and for UP and DOWN separately
    # (user-specified 2026-09-29): the whole range, below AND above the
    # bot's own gates (UP >= -0.05 since 2026-09-30, DOWN >= 0.20; before
    # that UP >= 0.15), to see at which edge — if
    # any — entering is actually profitable.
    shadow_edge_bands: tuple = (
        tuple((round(0.01 * i, 2), round(0.01 * (i + 1), 2)) for i in range(1, 50))
        + ((0.50, 1.00),)
    )

    # ===== ORDER BOOK RECORDING (2026-09-28) =====
    # Once a second, both tokens' top of book, full-size fill price and
    # depth go to the book_ticks table (bot_trades.db), so future parameter
    # research can use the real books instead of prices inferred from
    # trade prints. ~2 rows/s, roughly 15-20 MB of database a day.
    record_order_book: bool = True

    # ===== MARKET TAPE (2026-10-02) =====
    # market_recorder.py, started by run.py as a separate process: every
    # 100 ms, for every window, Binance perp/spot books and order flow,
    # Coinbase/Kraken/OKX/Bybit, Chainlink, and Polymarket's Up/Down books
    # go to their own database, for simulations bot_trades.db cannot support
    # (latency, lead-lag, order flow). Shares nothing with trading; measured
    # ~10 rows/s. See market_recorder.py's docstring.
    record_market_tape: bool = True
    market_tape_db: str = "market_tape.db"
    # Parquet copy of market_tape.db and bot_trades.db (parquet_sync.py), kept
    # up to date by the recorder process every 5 minutes; ~6x smaller on disk.
    # SQLite stays the live store. Needs pyarrow (requirements.txt).
    parquet_sync_enabled: bool = True
    parquet_dir: str = "parquet"
    # SQLite keeps only this many days of the two big tables (tape,
    # book_ticks); older rows are deleted once they are in Parquet, so the
    # .db files stop growing (user-requested 2026-10-05: "3 gün sonra db
    # dosyalarını boşaltarak devam etsin"); one hour since 2026-10-10 (user:
    # "3 günlük kuralı sil" -> keep 1 hour), so market_tape.db stays ~30-40 MB
    # on the 8 GB server. Full history is in Parquet. 0 = keep everything.
    # Small tables (trades, sessions, shadows, lab_shadow) are never trimmed.
    sqlite_retention_days: float = 1.0 / 24.0

    # ===== PERP MOMENTUM SHADOW (2026-10-04) =====
    # perp_shadow.py: the Binance perp momentum signal run live on the raw
    # aggTrade stream and recorded to perp_shadow (bot_trades.db), one shadow
    # trade per window. LOG-ONLY: it never opens a real paper trade, so the
    # frozen calibration is unchanged. See perp_shadow.py's docstring.
    perp_shadow_enabled: bool = True
    # (look-back seconds, threshold $) pairs, each recorded on its own (one
    # row per window per variant). 10 s ±$20 was the first candidate; 5 s ±$15
    # was the best of 36 on market_tape.db with +100/+300 ms fills (2026-10-04).
    perp_momentum_variants: tuple = ((10.0, 20.0), (5.0, 15.0))
    perp_shadow_min_remaining_s: float = 30.0
    perp_shadow_max_remaining_s: float = 280.0

    # ===== EXECUTION LATENCY (user-specified 2026-09-20) =====
    # Time from a signal firing to the order actually being placed must
    # stay under this or the trade is skipped — a stale price by the time
    # of execution is worse than no trade.
    execution_delay_max_ms_up: float = 150.0
    execution_delay_max_ms_down: float = 200.0  # updated 2026-09-20, was unbounded

    # NOTE: the risk circuit breaker (MAX_DAILY_LOSS/MAX_CONSECUTIVE_LOSSES/
    # COOLDOWN_MINUTES, V8.5 set) was removed 2026-09-23 at the user's
    # explicit instruction — run.py no longer pauses trading on daily loss
    # or a loss streak.
    #
    # NOTE: the V8.4 signal-strategy fields (signal_down_threshold,
    # signal_up_threshold, min_signal_confidence) were removed along with
    # get_signal_config(). Their only consumer was SignalEngine, which the
    # bot stopped calling long ago — see run.py's _init_modules(). The
    # live thresholds live in signal_engine.MultiFactorConfig.
    #
    # Also removed: max_gas_per_trade and resolution_threshold (a leftover
    # hardcoded "BTC threshold price" of 45000, from a design where the
    # market resolved against a fixed level rather than the window's own
    # opening price), and db_timeout and http_retries — all four were
    # defined, printed in some cases, and read by nothing.

    # ===== BLOCKCHAIN =====
    chain_id: int = 137                   # Polygon mainnet

    # ===== DATABASE =====
    db_file: str = "bot_trades.db"
    
    # ===== LOGGING =====
    log_file: str = "bot.log"
    # Rotation bounds (see run.py's setup_logging). Total on-disk cost is
    # log_max_bytes * (log_backup_count + 1) — ~110 MB at these values,
    # which is several days at the measured ~1.3 MB/hour. The bot runs
    # continuously, so an unbounded log file is a slow disk-fill, not a
    # theoretical concern.
    log_max_bytes: int = 10 * 1024 * 1024   # 10 MB per segment
    log_backup_count: int = 10              # keep 10 rotated segments
    log_level: str = LogLevel.INFO.value
    log_format: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    
    # ===== TIMEOUTS & RETRIES =====
    http_timeout: int = 10
    http_retry_backoff: float = 1.5
    
    # ===== MONITORING =====
    stats_log_interval: int = 60   # Log stats every 60 seconds
    heartbeat_check_interval: int = 10

    # ===== GIL / THREAD SCHEDULING (2026-09-28) =====
    # How long a thread may hold the GIL before a waiting one can force it
    # off (sys.setswitchinterval; Python's default is 5 ms). Each WS reader
    # in this process gives the GIL up and takes it back several times per
    # frame (select, then header and payload recv), and while any other
    # thread is running Python code every take-back can wait out the full
    # interval. Measured on this machine (Python 3.14, websocket-client
    # 1.9.2, ~1.5 KB frames from a server in another process): one busy
    # thread cut a reader from ~32,000 to 309 frames/s at 5 ms — below the
    # 700-800/s this feed bursts to, which is when Polymarket drops us as a
    # "slow consumer" (code 1013) — and 0.5 ms raised it to ~4,000/s. The
    # cost is more frequent switching for threads running Python code.
    gil_switch_interval_ms: float = 0.5

    # Minimum time between the starts of two signal evaluations. With live
    # books (BotConfig.apply_price_changes) every price_change frame
    # requests an evaluation, so in a burst the evaluator would run back to
    # back — the busy thread described above, at exactly the moment the WS
    # readers need the GIL most. Ticks arriving inside the interval still
    # coalesce into the next evaluation, which then sees the newest books;
    # an evaluation after a quiet spell is not delayed. 0 disables it.
    evaluator_min_interval_ms: float = 20.0
    
    # ===== FLAGS =====
    # Default TRUE — simulate, don't spend. A trading bot's mode flag has
    # an asymmetric failure cost: defaulting to "simulate" costs a wasted
    # test run, defaulting to "real" costs money. This used to default to
    # False (real trading), so anything that stopped PAPER_TRADING from
    # being read correctly — a missing/renamed .env, running from another
    # directory, a typo in the key name — silently selected the expensive
    # mode. Turning real trading on now takes a deliberate act: set this
    # to false AND set the confirmation variable below (see _validate).
    paper_trading: bool = True     # True = simulate, no capital at risk
    dry_run: bool = False          # True = simulate without TX
    
    def __post_init__(self):
        """Load from environment and validate"""
        self._load_from_env()
        self._validate()
    
    # ========================================================================
    # ENVIRONMENT LOADING
    # ========================================================================
    
    def _load_from_env(self):
        """Load configuration from environment variables"""
        
        # Wallet
        self.private_key = os.getenv("PRIVATE_KEY", self.private_key)
        self.wallet_address = os.getenv("WALLET_ADDRESS", self.wallet_address)
        
        # Environment
        env = os.getenv("ENVIRONMENT", self.environment)
        if env in [e.value for e in Environment]:
            self.environment = env
        
        # Log level
        log_level = os.getenv("LOG_LEVEL", self.log_level)
        if log_level in [level.value for level in LogLevel]:
            self.log_level = log_level
        
        # Trading
        try:
            self.trade_size = int(os.getenv("TRADE_SIZE", self.trade_size))
        except ValueError:
            pass

        # Flags — see _env_bool(): an absent variable keeps the safe
        # dataclass default instead of silently reading as False, and an
        # unrecognized value is a hard error rather than a coin flip.
        self.paper_trading = _env_bool("PAPER_TRADING", self.paper_trading)
        self.dry_run = _env_bool("DRY_RUN", self.dry_run)
        
        logger.info("[CONFIG] Loaded from environment")
    
    # ========================================================================
    # VALIDATION
    # ========================================================================
    
    def _validate(self):
        """Validate configuration"""

        _warn_if_env_file_is_exposed()

        errors = []

        # Real-money mode needs a SECOND, deliberate confirmation.
        #
        # PAPER_TRADING=false on its own is too easy to arrive at by
        # accident — a stray edit, a copied .env, a shell export left
        # over from a previous session. Requiring an unrelated variable
        # whose name has to be typed out in full means live trading can
        # only be reached on purpose. Nothing else in this config is
        # irreversible, so nothing else gets this treatment.
        if not self.paper_trading and not self.dry_run:
            confirmation = os.getenv(CONFIRM_REAL_MONEY_ENV, "").strip().strip('"').strip("'").lower()
            if confirmation != CONFIRM_REAL_MONEY_VALUE:
                errors.append(
                    f"REAL-MONEY MODE REFUSED. PAPER_TRADING is false and DRY_RUN is "
                    f"false, which would trade actual capital. To confirm that is "
                    f"intended, also set {CONFIRM_REAL_MONEY_ENV}={CONFIRM_REAL_MONEY_VALUE}. "
                    f"To keep simulating instead, set PAPER_TRADING=true."
                )

        # Private key
        if not self.private_key or not self.private_key.startswith("0x"):
            errors.append("PRIVATE_KEY must be hex format: 0x...")
        
        # Wallet address
        if not self.wallet_address or not self.wallet_address.startswith("0x"):
            errors.append("WALLET_ADDRESS must be hex format: 0x...")
        
        # Trade size
        if self.trade_size <= 0:
            errors.append("TRADE_SIZE must be > 0")
        
        if errors:
            error_msg = "\n".join(errors)
            logger.error(f"[CONFIG] Validation failed:\n{error_msg}")
            raise ValueError(f"Configuration error: {error_msg}")
        
        logger.info("[CONFIG] Validation passed")
    
    # ========================================================================
    # PROPERTIES
    # ========================================================================
    
    @property
    def is_production(self) -> bool:
        """Check if production environment"""
        return self.environment == Environment.PRODUCTION.value
    
    @property
    def is_paper_trading(self) -> bool:
        """Check if paper trading mode"""
        return self.paper_trading
    
    @property
    def is_dry_run(self) -> bool:
        """Check if dry run mode"""
        return self.dry_run
    
    # ========================================================================
    # CONNECTION CONFIGS (for sub-modules)
    # ========================================================================
    
    def get_polymarket_config(self):
        """Get Polymarket bot config"""
        from polymarket_production_bot import BotConfig
        
        return BotConfig(
            market_duration_seconds=self.market_duration_seconds,
            market_name=self.market_name,
            token_yes_id=self.token_yes_id,
            token_no_id=self.token_no_id,
            paper_trade_size=self.trade_size
        )
    
    def get_external_config(self):
        """Get external connections config"""
        from external_connections import ExternalConfig
        
        return ExternalConfig()
    
    def get_wallet_config(self):
        """Get wallet config"""
        from wallet_manager import WalletConfig, SignatureType, WalletType

        # SignatureType.EOA(0)->metamask, POLY_PROXY(1, old Magic.link) and
        # POLY_1271(3, new Magic.link)->magic_link, GNOSIS_SAFE(2)->gnosis_safe.
        # A plain "0 -> metamask else magic_link" ternary used to collapse
        # signature_type 1/2/3 all into "magic_link", silently mislabeling
        # e.g. a Gnosis Safe wallet.
        wallet_type_by_signature = {
            SignatureType.EOA: WalletType.METAMASK,
            SignatureType.POLY_PROXY: WalletType.MAGIC_LINK,
            SignatureType.GNOSIS_SAFE: WalletType.GNOSIS_SAFE,
            SignatureType.POLY_1271: WalletType.MAGIC_LINK,
        }

        return WalletConfig(
            private_key=self.private_key,
            wallet_type=wallet_type_by_signature.get(self.signature_type, WalletType.METAMASK),
            signature_type=self.signature_type,
            chain_id=self.chain_id
        )

    # ========================================================================
    # DISPLAY & EXPORT
    # ========================================================================
    
    def print_config(self):
        """Print non-sensitive configuration"""
        
        lines = [
            "="*80,
            "BOT CONFIGURATION",
            "="*80,
            f"Environment:         {self.environment}",
            f"Paper trading:       {self.paper_trading}",
            f"Dry run:             {self.dry_run}",
            "",
            f"Wallet:              {self.wallet_address[:10]}...",
            f"Signature type:      {self.signature_type}",
            "",
            f"Market:              {self.market_name}",
            f"Duration:            {self.market_duration_seconds}s",
            f"Trade size:          {self.trade_size} tokens",
            "",
            "Slippage:            no cap — fills at the full order's VWAP (priced into edge)",
            "",
            f"Database:            {self.db_file}",
            f"Log level:           {self.log_level}",
            f"Log file:            {self.log_file}",
            "="*80
        ]
        
        print("\n".join(lines))
    
    def export_dict(self) -> dict:
        """Export as dictionary (non-sensitive)"""
        return {
            "environment": self.environment,
            "market": self.market_name,
            "duration": self.market_duration_seconds,
            "trade_size": self.trade_size,
            "paper_trading": self.paper_trading,
            "dry_run": self.dry_run,
            "chain_id": self.chain_id
        }

# ============================================================================
# ENVIRONMENT FILE (.env)
# ============================================================================

"""
Create a .env file in the project root with:

PRIVATE_KEY=0x...
WALLET_ADDRESS=0x...
ENVIRONMENT=production
PAPER_TRADING=false
DRY_RUN=false
LOG_LEVEL=INFO
TRADE_SIZE=10
"""

# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    
    try:
        # Load config
        config = Config()
        
        logger.info("="*80)
        logger.info("CONFIGURATION LOADED")
        logger.info("="*80)
        
        # Print config
        config.print_config()
        
        # Export as dict
        config_dict = config.export_dict()
        print(f"\nConfig dict: {config_dict}")
    
    except ValueError as e:
        logger.error(f"[CONFIG] Failed to load: {e}")
        print("Please set required environment variables:")
        print("  PRIVATE_KEY (0x...)")
        print("  WALLET_ADDRESS (0x...)")
