"""
SIGNAL ENGINE — BTC price-to-beat model for the 5-minute Up/Down markets

MultiFactorSignalEngine: BTC's deviation from the window's opening price,
normalized by realized volatility (MinuteVolatility), gives a model
probability; direction-specific gates on that probability, the ask and the
fee-aware edge decide the signal. See MultiFactorConfig for the parameters.

The V8.4 SignalEngine (mean reversion on the token's own price) and
BTCDeviationSignalEngine were removed 2026-09-29: neither was used any more
(backups/pre_market_params_cleanup_2026-09-29 has the last copy).
"""

import logging
import math
import threading
import time
import statistics
from typing import Optional, Dict
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger("SignalEngine")

# ============================================================================
# ENUMS
# ============================================================================

class Signal(Enum):
    """Trading signals"""
    BUY_UP = "BUY_UP"           # Expect price to rise
    BUY_DOWN = "BUY_DOWN"       # Expect price to fall
    NO_SIGNAL = "NO_SIGNAL"     # No signal


class MinuteVolatility:
    """
    Realized BTC volatility from 1-minute closes — the estimate behind
    model_prob since 2026-09-27, replacing the (since removed)
    BTCDeviationSignalEngine's 120 s tick-based _expected_move().

    Why the replacement: _expected_move() scaled the stdev of tick-to-tick
    trade returns over the last 120s up to the time remaining. Measured
    against 491 real trades, the BTC move that actually followed was on
    average 2.5x the move it predicted (a calibrated model gives 1.0), and
    its per-second vol was ~3.2x below the realized 5-minute vol. Tick
    returns over two minutes miss how BTC actually travels over several
    minutes, so model_prob came out near-certain on moves that were
    really coin flips.

    Closes come from two places: Binance REST 1m candles (load_klines(),
    refreshed by run.py at each window start) and live ticks
    (record_tick(), the last price seen in each minute), so the estimate
    survives a REST outage once the bot has been up for vol_min_returns
    minutes. A REST close overwrites a tick-derived one for the same
    minute — it is the exchange's own number.
    """

    def __init__(self, lookback_minutes: int = 60, min_returns: int = 30):
        self.lookback_minutes = lookback_minutes
        self.min_returns = min_returns
        self._closes: Dict[int, float] = {}   # minute open time (s) -> close
        self._lock = threading.RLock()
        self._version = 0
        self._cache_key = None
        self._cache_value: Optional[float] = None

    def load_klines(self, klines) -> None:
        """klines: iterable of (minute_open_time_s, close) for closed candles."""
        with self._lock:
            for minute, close in klines:
                if close and close > 0:
                    self._closes[int(minute)] = float(close)
            self._prune()
            self._version += 1

    def record_tick(self, ts: float, price: float) -> None:
        if not price or price <= 0:
            return
        with self._lock:
            minute = int(ts) // 60 * 60
            if minute not in self._closes:
                # First tick of a new minute: the minute before it is now
                # complete, so a cached estimate may be out of date.
                self._version += 1
            self._closes[minute] = price
            if len(self._closes) > 3 * self.lookback_minutes:
                self._prune()

    def _prune(self) -> None:
        if not self._closes:
            return
        cutoff = max(self._closes) - 2 * 60 * (self.lookback_minutes + 2)
        for minute in [m for m in self._closes if m < cutoff]:
            del self._closes[minute]

    def sigma_per_sqrt_s(self, now: float) -> Optional[float]:
        """
        Per-sqrt-second vol from the lookback_minutes completed minutes
        before `now`, or None with fewer than min_returns usable returns.
        Only consecutive minutes form a return, so a gap in the history
        shortens the sample instead of stretching one return across it.
        """
        current = int(now) // 60 * 60
        with self._lock:
            # evaluate() asks on every tick; only completed minutes feed
            # the estimate, so it can only change when the minute rolls
            # over or load_klines() adds history.
            if self._cache_key == (current, self._version):
                return self._cache_value
            closes = [(m, self._closes.get(m))
                      for m in range(current - 60 * (self.lookback_minutes + 1), current, 60)]
            returns = [math.log(b / a) for (_, a), (_, b) in zip(closes, closes[1:]) if a and b]
            value = (statistics.pstdev(returns) / math.sqrt(60)
                     if len(returns) >= self.min_returns else None)
            self._cache_key, self._cache_value = (current, self._version), value
            return value


class ShortVolatility:
    """
    Realized BTC volatility over the last few minutes, from 10-second closes.
    LOG-ONLY (added 2026-10-02): evaluate() reports the model probability it
    implies next to the real one, and no gate reads it.

    Why: on ~410 clean windows (2026-09-29 to 10-02) the 60-minute estimate
    ran ~20% above the volatility of the last 5 minutes (median ratio
    0.80-0.83), so model_prob came out under-confident. This estimate
    scored better at 150 s left (log-loss 0.3716 -> 0.3638) and 100 s
    (0.3323 -> 0.3135), close to the market price (0.3618 / 0.3111) and
    calibrated without any refit. At 210 s it did not help. It is logged
    here to confirm that on live data before anything trades on it.

    Each step_seconds bucket keeps the last price seen in it (live ticks,
    or Binance 1 s candles via load_second_closes()). Returns are the
    root mean square of consecutive-bucket log returns, NOT demeaned —
    over five minutes the drift is noise, and demeaning would remove a
    genuine move from the estimate. Gaps shorten the sample, as in
    MinuteVolatility.
    """

    def __init__(self, window_seconds: int = 300, step_seconds: int = 10, min_returns: int = 24):
        self.window_seconds = window_seconds
        self.step_seconds = step_seconds
        self.min_returns = min_returns
        self._closes: Dict[int, float] = {}   # bucket start (s) -> last price in it
        self._lock = threading.RLock()
        self._version = 0
        self._cache_key = None
        self._cache_value: Optional[float] = None

    def load_second_closes(self, candles) -> None:
        """
        candles: iterable of (second_open_time_s, close) for closed 1 s
        candles. Only a bucket's final second is taken — that close is the
        bucket's close, the exchange's own number, and it overwrites a
        tick-derived one. Fills history after a restart and any gap between
        trading loops.
        """
        step = self.step_seconds
        with self._lock:
            for second, close in candles:
                second = int(second)
                if close and close > 0 and second % step == step - 1:
                    self._closes[second - step + 1] = float(close)
            self._prune()
            self._version += 1

    def record_tick(self, ts: float, price: float) -> None:
        if not price or price <= 0:
            return
        with self._lock:
            bucket = int(ts) // self.step_seconds * self.step_seconds
            if bucket not in self._closes:
                self._version += 1
            self._closes[bucket] = price
            if len(self._closes) > 3 * (self.window_seconds // self.step_seconds):
                self._prune()

    def _prune(self) -> None:
        if not self._closes:
            return
        cutoff = max(self._closes) - 2 * (self.window_seconds + 2 * self.step_seconds)
        for bucket in [b for b in self._closes if b < cutoff]:
            del self._closes[bucket]

    def sigma_per_sqrt_s(self, now: float) -> Optional[float]:
        """Per-sqrt-second vol over the window before `now` (completed buckets
        only), or None with fewer than min_returns usable returns."""
        current = int(now) // self.step_seconds * self.step_seconds
        with self._lock:
            if self._cache_key == (current, self._version):
                return self._cache_value
            value = self.estimate(self._closes, now, self.window_seconds,
                                  self.step_seconds, self.min_returns)
            self._cache_key, self._cache_value = (current, self._version), value
            return value

    @staticmethod
    def estimate(closes: Dict[int, float], now: float, window_seconds: int,
                 step_seconds: int, min_returns: int) -> Optional[float]:
        """The calculation itself, shared with paper_report.py so the report
        rebuilds exactly what the bot logs."""
        current = int(now) // step_seconds * step_seconds
        n = window_seconds // step_seconds
        cs = [closes.get(b) for b in range(current - step_seconds * (n + 1), current, step_seconds)]
        returns = [math.log(b / a) for a, b in zip(cs, cs[1:]) if a and b]
        if len(returns) < min_returns:
            return None
        return math.sqrt(sum(r * r for r in returns) / len(returns)) / math.sqrt(step_seconds)


# ============================================================================
# MULTI-FACTOR SIGNAL ENGINE
# ============================================================================

@dataclass
class MultiFactorConfig:
    """
    Config for MultiFactorSignalEngine — replaced wholesale 2026-09-22 by
    the user's "V8.5 önerilen başlangıç seti (asimetrik, muhafazakâr)",
    trading ONLY on these parameters per their explicit instruction (no
    added gates beyond what's listed there). Two of that set's fields
    needed an interpretation since neither exists in this codebase as
    written — both decided by the user when asked, or (where they declined
    to answer) resolved by reusing an existing value rather than inventing
    a new mechanism:
      - MOMENTUM_THRESHOLD_UP/DOWN (45/38): no "momentum score" metric
        exists here (the 8-component weighted scorer that had one was
        deleted 2026-09-20). Mapped to model_prob*100 — the only existing
        0-100-like value — so MOMENTUM_THRESHOLD_UP=45 means
        min_model_prob_up=0.45, DOWN=38 means min_model_prob_down=0.38.
      - TRADE_SHARES=30 (fixed, updated 2026-09-23 from the V8.5 set's 27)
        vs RISK_PER_TRADE_PCT_UP/DOWN (%1.5/%2.0 of a bankroll):
        contradictory in the same block, and the percentage form needs a
        real balance — which the user separately confirmed doesn't exist
        here (paper trading, no wallet query; "kelly sistemini sil ...
        her markette 30 share işlem açacak").
        Resolved as flat TRADE_SHARES=30 (config.trade_size), risk-percent
        form dropped.
    MIN_EDGE is fee-aware here (the user's own spec: "tahmini_olasılık -
    fiyat - ücret"), unlike the plain model_prob-minus-ask edge this
    engine used before — see min_edge's own comment. The daily-loss/
    consecutive-loss circuit breakers are new gates this version adds
    (also user-specified) — see run.py, checked before a cycle's signal
    is even evaluated. MIN_BOOK_DEPTH was implemented here too but removed
    2026-09-22 at the user's explicit follow-up instruction.

    2026-09-27 — "en güvenli sistem" (user-requested), second revision
    the same day. Two things changed together:

    1. The formula now follows the rule Polymarket actually settles by
       (resolutionSource btc-usd-twap-60s): a window is UP when the 60s
       TWAP ending at its close is >= the 60s TWAP ending at its start.
       run.py passes that start TWAP as opening_btc, and evaluate() scales
       the remaining variance with _effective_seconds() because the final
       minute is averaged. The Binance equivalent reproduced 98.0% of 820
       official results; the spot price used before, 84.3%. Volatility
       comes from MinuteVolatility (the old tick estimate was ~3x small).
    2. Every gate below was chosen on all 850 windows of 22-27 Sep
       (opened, rejected, circuit-breaker-paused), simulating a check every
       second against an ask rebuilt from Polymarket's own trade prints.
       That ask estimate is unbiased against the VWAP asks this bot logged
       (mean difference ~0 over 2,838 of them), so the gates are tuned to
       the same prices the bot sees live. (An earlier revision was tuned
       on an estimate ~3c too expensive, which made these same gates
       looser live than in the test.) Rule: profitable every day, t >= 2,
       >= 150 trades, then the lowest max drawdown, then the best worst
       hour; ties at equal drawdown went to the setting that stayed safe
       when the ask was assumed ~3c more expensive (a stress test for
       errors in the ask estimate). Ranges were widened until no chosen
       value sat on the edge of what was searched (edge up to 0.30,
       probability up to 0.85), and the neighbourhood was then checked
       exhaustively. In-sample: 152 trades, +$1,269, 82% won, max
       drawdown $32, worst hour -$26, worst day +$75; under the +3c stress
       test +$507, max drawdown $34. Leave-one-day-out (selection redone
       on 5 days, scored on the 6th): +$977, max drawdown $59, 6 of 6
       days positive — expect something nearer that.

    Previous values are noted inline.
    """

    # --- Market quality (hard gate, checked before anything else) ---
    # market_data.accepting_orders/enable_order_book are deliberately NOT
    # gated on: verified live 2026-09-14 that a genuinely tradable 5m BTC
    # market had accepting_orders=None and enable_order_book=False, so
    # those two fields don't reliably reflect tradability for this market
    # type (see market_loader.py's MarketData docstring). Only the
    # question's own status and the live book's own spread/price actually
    # indicate whether this market is usable right now.
    # Price bands barely matter once the edge and probability gates below
    # apply: floors anywhere in 0.05-0.40 and caps in 0.65-0.90 kept max
    # drawdown at $32-36, so they are left wide.
    min_ask: float = 0.30              # MIN_ENTRY_PRICE (DOWN)
    max_ask_down: float = 0.75         # MAX_ENTRY_PRICE_DOWN — 0.65 before
    min_ask_up: float = 0.20           # MIN_ENTRY_PRICE (UP)
    max_ask: float = 0.80              # MAX_ENTRY_PRICE_UP

    # --- Time window (hard gate) — bound on seconds_remaining, no elapsed
    # check. UP may enter from 210s down to 90s left (also allowing
    # 240-210s raised max drawdown to $35, 90-60s to $48); DOWN from 210s
    # down to 60s left (also allowing 240-210s raised it to $50).
    #
    # 2026-09-30 (calibration period, see the note at min_edge_up below):
    # UP's upper bound is back at 240 s. The 240-210 s drawdown remark above
    # came from older data recorded through a congested tunnel.
    min_seconds_to_resolution_up: float = 90.0
    max_seconds_to_resolution_up: float = 240.0    # 210 before 2026-09-30; 240 before that
    min_seconds_to_resolution_down: float = 60.0
    max_seconds_to_resolution_down: float = 210.0  # 180 before

    # Length of the TWAP Polymarket settles on (Chainlink btc-usd-twap-60s):
    # run.py's opening price and _effective_seconds() both assume it.
    settlement_twap_seconds: float = 60.0

    # --- Volatility behind model_prob (see MinuteVolatility) ---
    # Stdev of 1-minute BTC log returns over this many past minutes...
    vol_lookback_minutes: int = 60
    # ...and at least this many returns before the estimate is trusted;
    # below it evaluate() rejects instead of guessing.
    vol_min_returns: int = 30
    # Floor on the per-sqrt-second volatility: half the 22-27 Sep realized
    # 5-minute vol (0.0966% / sqrt(300) = 0.0000558). Replaces the old flat
    # 0.02% floor on the expected move, which bound on 40% of trades and
    # was ~4x below a realistic 4-minute move.
    vol_floor_per_sqrt_s: float = 0.0000279

    # --- Short-horizon volatility (ShortVolatility) — LOG-ONLY, no gate
    # reads it. Window, bucket length and the minimum number of returns
    # (80% of the window) before an estimate is reported.
    short_vol_window_seconds: int = 300
    short_vol_step_seconds: int = 10
    short_vol_min_returns: int = 24

    # --- Model probability (hard gate) — Phi(|z|): the closed-form
    # probability of the candidate direction being correct under a
    # random-walk assumption, from the BTC-deviation z-score evaluate()
    # computes. Stands in for
    # MOMENTUM_THRESHOLD_UP/DOWN (45/38 on a 0-100 scale) — see this
    # class's docstring for why.
    # BUG FIX (see _win_probability_estimate): model_prob is Phi(|z|) —
    # the standard normal CDF of the z-score's MAGNITUDE — so its range
    # is [0.5, 1.0] and it NEVER goes below 0.5. The previous values
    # (0.45 / 0.38, a literal MOMENTUM_THRESHOLD/100 mapping) were
    # therefore unreachable as rejections: this gate could not fire, in
    # either direction, for any input. Confirmed in bot.log (2026-09-22):
    # 1906 evaluations at model_prob=1.000, zero rejections here.
    #
    # Rescaled onto the metric's real range, keeping the user's own 0-100
    # numbers and their ordering (UP stricter than DOWN):
    #     min_model_prob = 0.5 + (MOMENTUM_THRESHOLD / 100) * 0.5
    #   UP:   0.5 + 0.45 * 0.5 = 0.725  -> requires |z| >= ~0.60
    #   DOWN: 0.5 + 0.38 * 0.5 = 0.690  -> requires |z| >= ~0.50
    # i.e. BTC must be at least ~half an "expected move" ahead of the
    # window's opening price before the direction is tradable at all.
    #
    # Retuned 2026-09-27 against the corrected, TWAP-aware probability
    # (see this class's docstring).
    # UP at 0.55 or 0.60 made more but raised max drawdown to $49 / $44.
    # DOWN at 0.60 or 0.65 had the same $32 drawdown at the estimated ask
    # but $51 under the +3c stress test, against $34 at 0.70.
    min_model_prob_up: float = 0.65      # 0.725 originally
    min_model_prob_down: float = 0.70    # 0.75 before (0.690 originally)

    # --- Edge / fair-value (hard gate), fee-aware per the user's own
    # formula: (tahmini_olasılık - fiyat - ücret) >= MIN_EDGE. ücret
    # (fee) is Polymarket's taker fee expressed the same way
    # PositionManager.TAKER_FEE_RATE_CRYPTO computes it in dollar terms
    # (fee = size * rate * price * (1-price)) but per single share (no
    # size factor) so it's directly subtractable from edge, which is
    # already in $-per-share terms — see MultiFactorSignalEngine.evaluate().
    # NOTE despite the "shared" label this only gates DOWN — min_edge_up
    # below is UP's actual minimum.
    #
    # With the corrected probability this is the gate that matters most.
    # UP max drawdown: $162 with no edge requirement, $113 at 0.05, $52
    # at 0.10, $32 at 0.15. DOWN: $54 at 0.10, $52 at 0.15, $32 at 0.20.
    #
    # 2026-09-30 — CALIBRATION-PERIOD SETTINGS (user-approved, paper only).
    # FROZEN: change nothing here while calibrating (a change restarts the
    # trade count) and read results only at 150 / 450 / 1000 trades.
    #   UP:   the edge gate is effectively lifted (-0.05 = buy UP even when
    #         the ask is up to 5c above the model's fair value) and the time
    #         window is 90-240 s.
    #   DOWN: real trades ON at edge >= 0.20, the value before this note.
    #         They were switched off (9.0) for a few hours that day, then
    #         back on at the user's request before the first calibration
    #         run began. Replayed on the live database alone (18 h, 152
    #         windows, instant fills, fees in): edge >= 0.20 was met once
    #         (1 trade, a win); every lower threshold lost — >= 0.10: 4
    #         trades -$45, >= 0.05: 14 trades -$43, >= 0.01: 22 trades
    #         -$50 — so 0.20 is the least-exposed way to let DOWN trade,
    #         not a measured edge. One position per window, so a DOWN
    #         signal that comes first takes the window from UP (never at
    #         0.20; 2 of UP's 60 trades at >= 0.01). The shadow records
    #         DOWN in every edge band either way.
    # Why: recorded books replayed on 183 clean windows (24.6 h), and on the
    # live database alone (152 windows), 1 s delayed fills, fees included:
    # UP-only / edge >= -0.05 / 90-240 s made +$176 (69 trades) and +$219
    # (60 trades), interval above zero in both; DOWN showed no edge at any
    # threshold (edge >= 0.01: 30 trades, 70% wins, -$0.44 per trade,
    # interval -5.6...+4.3). The values before (UP 0.15, DOWN 0.20, UP window
    # to 210 s) were tuned on data recorded through a congested tunnel —
    # books seconds late, see feed_lag.py — and traded about once a day.
    # Old values: backups/pre_calibration_config_2026-09-30.
    min_edge: float = 0.20             # MIN_EDGE (DOWN) — 9.0 (= off) for a few hours on 2026-09-30; 0.15 before 0.20
    min_edge_up: float = -0.05         # MIN_EDGE (UP) — 0.15 before 2026-09-30
    # Must match PositionManager.TAKER_FEE_RATE_CRYPTO — see the long note
    # there, in particular why this is 0.07 and not 0.0007 despite the
    # docs calling it "7 bps". Getting that wrong here understates the fee
    # this gate subtracts by 100x and silently loosens the edge threshold.
    taker_fee_rate: float = 0.07

    def __post_init__(self):
        """
        Reject a model-probability threshold that can never fire.

        This guards the exact bug the min_model_prob_up/_down comment
        above describes: model_prob is Phi(|z|), bounded below by 0.5, so
        any minimum at or under 0.5 turns a hard risk gate into dead code
        that silently passes every signal through. Raising here fails the
        bot at startup instead of letting it trade with a gate that only
        looks like it is protecting anything.
        """
        for name in ("min_model_prob_up", "min_model_prob_down"):
            value = getattr(self, name)
            if not (0.5 < value < 1.0):
                raise ValueError(
                    f"MultiFactorConfig.{name}={value} is outside (0.5, 1.0). "
                    f"model_prob is Phi(|z|), which never returns less than "
                    f"0.5, so a threshold at or below 0.5 can never reject a "
                    f"signal (and one at or above 1.0 rejects every signal)."
                )


@dataclass
class SignalEvaluation:
    """Result of one MultiFactorSignalEngine.evaluate() call."""
    signal: Signal
    score: float
    candidate_direction: Optional[str]  # "UP" / "DOWN" / None
    rejected_reason: Optional[str]      # set when signal == NO_SIGNAL via a hard gate
    breakdown: Dict[str, float]         # per-component score, for logging/tuning
    # Raw inputs behind the score, exposed so a caller building a
    # signal_features snapshot doesn't need to recompute any of this itself.
    distance_z: Optional[float] = None
    returns: Dict[str, Optional[float]] = field(default_factory=dict)
    acceleration: Optional[float] = None
    obis: Dict[str, Optional[float]] = field(default_factory=dict)
    trade_flow: Optional[float] = None
    candidate_ask: Optional[float] = None
    token_return: Optional[float] = None
    model_prob: Optional[float] = None
    edge: Optional[float] = None
    # MinuteVolatility estimate (per sqrt-second, fraction) behind distance_z.
    vol_per_sqrt_s: Optional[float] = None
    # LOG-ONLY: the same probability and edge with ShortVolatility in place
    # of MinuteVolatility (floored the same way). Never gates anything.
    short_vol_per_sqrt_s: Optional[float] = None
    model_prob_short_vol: Optional[float] = None
    edge_short_vol: Optional[float] = None


class MultiFactorSignalEngine:
    """
    Signal engine for the 5-minute BTC up/down market — trades using ONLY
    the user-specified parameters on MultiFactorConfig (see its docstring
    for the 2026-09-20 replacement of the earlier 8-component weighted
    scoring system this class used to run).

    Pipeline: hard gates (market status, time window) -> compute model
    probability from the BTC price-to-beat z-score -> direction-specific
    checks (model probability, ask price range, edge, spread) -> signal.
    The volatility that normalizes the raw price deviation into a z-score
    comes from MinuteVolatility (self.vol) since 2026-09-27; run.py keeps
    it loaded with 1-minute candles.
    """

    # Minimum wall-clock seconds between two identical-direction
    # "signal fired" log lines — see evaluate().
    FIRE_LOG_INTERVAL = 10.0

    def __init__(self, config: Optional[MultiFactorConfig] = None,
                 trade_size: float = 30.0):
        self.config = config or MultiFactorConfig()
        self.vol = MinuteVolatility(self.config.vol_lookback_minutes, self.config.vol_min_returns)
        self.short_vol = ShortVolatility(self.config.short_vol_window_seconds,
                                         self.config.short_vol_step_seconds,
                                         self.config.short_vol_min_returns)
        self.logger = logging.getLogger("MultiFactorSignal")
        # The actual fixed order size this bot trades every time (V8.5:
        # no dynamic sizing — see MultiFactorConfig's docstring). Used to
        # price the candidate token's VWAP fill instead of top-of-book
        # best_ask, so the edge/fee gate below is checked against the
        # same price run.py will actually pay, not a single-share price
        # that understates cost on a thin book.
        self.trade_size = trade_size

        # One counter per gate. These used to share
        # "rejected_market_quality", which hid the fact that the
        # model_prob gate was rejecting literally nothing (see
        # MultiFactorConfig.min_model_prob_up's comment) — a dead gate is
        # indistinguishable from a gate nothing happens to trip unless
        # they are counted apart.
        # Throttle state for the per-fire log line above.
        self._last_fire_log_time = 0.0
        self._last_logged_signal = None
        self._suppressed_fire_logs = 0

        self.stats = {
            "evaluations": 0,
            "rejected_market_quality": 0,
            "rejected_time_window": 0,
            "rejected_no_price": 0,
            "rejected_no_distance": 0,
            "rejected_no_volatility": 0,
            "rejected_no_book": 0,
            "rejected_depth": 0,
            "rejected_model_prob": 0,
            "rejected_ask_range": 0,
            "rejected_edge": 0,
            "buy_up_signals": 0,
            "buy_down_signals": 0,
        }

    def _effective_seconds(self, seconds_remaining: float) -> float:
        """
        Seconds of BTC variance still ahead of the settlement price.

        Polymarket settles on the TWAP of the last L seconds (L =
        settlement_twap_seconds), not the closing spot price. For a random
        walk, the average over the final L seconds carries L/3 seconds'
        worth of variance, so with r >= L seconds left the settlement
        price's variance around today's price is (r - L) + L/3 = r - 2L/3
        (r - 40 for L = 60). Inside the averaging window only the unseen
        part is uncertain: r^3 / (3 L^2), which meets the first formula at
        r = L.
        """
        L = self.config.settlement_twap_seconds
        r = max(seconds_remaining, 0.0)
        if r >= L:
            return r - 2.0 * L / 3.0
        return r ** 3 / (3.0 * L * L)

    @staticmethod
    def _win_probability_estimate(distance_z: float) -> float:
        """
        Phi(|z|) — standard normal CDF of the price-to-beat z-score's
        magnitude. Not a trained model: a closed-form consequence of
        treating the remaining BTC move as normally distributed with the
        std MinuteVolatility estimates. If we're z "expected moves" ahead, this is the
        textbook probability of still being ahead once that remaining move
        actually happens. Same formula for both directions — MultiFactorConfig
        just applies a different minimum (min_model_prob_up/_down) to it.
        """
        return 0.5 * (1.0 + math.erf(abs(distance_z) / math.sqrt(2.0)))

    # --- main entry point ---

    def evaluate(
        self,
        current_btc: Optional[float],
        opening_btc: Optional[float],
        seconds_remaining: float,
        *,
        current_btc_ts: Optional[float] = None,
        elapsed: float,
        market_status: Optional[str],
        yes_book, no_book,  # polymarket_production_bot.Orderbook, or None
        trade_flow_yes: Optional[float],
        trade_flow_no: Optional[float],
    ) -> SignalEvaluation:
        """
        Everything from current_btc_ts onward is keyword-only (the bare
        `*` in the signature). With ten parameters — including two
        near-identical trade-flow sides — a positional call that
        transposes a pair would be silent
        and would corrupt every decision downstream, so the call has to
        name them. run.py's single call site already did.

        Args mirror what run.py already has on hand each loop tick.
        elapsed/trade_flow_yes/trade_flow_no are accepted
        but unused — MultiFactorConfig's 2026-09-20 replacement doesn't
        reference them (both directions' time-window gates now bound
        seconds_remaining only, no elapsed check), kept in the signature
        only so run.py's call site doesn't need touching.

        Always records current_btc into the underlying price history
        (needed for the volatility estimate behind the model-probability
        gate) even when a hard gate rejects the tick: that history must
        stay continuous across the whole window.
        """
        self.stats["evaluations"] += 1

        # Keeps the current minute's close in MinuteVolatility up to date
        # between run.py's per-window REST refreshes.
        now = current_btc_ts or time.time()
        if current_btc:
            self.vol.record_tick(now, current_btc)
            self.short_vol.record_tick(now, current_btc)

        # --- Hard gate: market status ---
        if market_status is not None and market_status != "active":
            self.stats["rejected_market_quality"] += 1
            return SignalEvaluation(Signal.NO_SIGNAL, 0.0, None, f"market status={market_status}", {})

        # Say WHY, don't return a bare None. A rejected_reason of None
        # scores -1 in run.py's _reason_depth(), which never beats the
        # cycle's initial best-depth of -1 — so this outcome recorded
        # nothing at all on the market_sessions row, leaving it
        # indistinguishable from a cycle whose evaluator never ran. That
        # matters more since window-boundary prices got a strict 2s
        # freshness bound: "no price" is now the single most likely reason
        # a cycle does not trade, and it was the one reason the database
        # could not show.
        if not opening_btc or opening_btc <= 0:
            self.stats["rejected_no_price"] += 1
            return SignalEvaluation(
                Signal.NO_SIGNAL, 0.0, None,
                "no BTC price: window opening price unavailable "
                "(cycle cannot resolve or trade)", {},
            )
        if not current_btc:
            self.stats["rejected_no_price"] += 1
            return SignalEvaluation(
                Signal.NO_SIGNAL, 0.0, None,
                "no BTC price: no sufficiently fresh current price", {},
            )

        # --- Direction + model probability, from the BTC price-to-beat
        # z-score (volatility-normalized deviation from the window's
        # opening price). Computed before the time-window gate below
        # (unlike the old shared-threshold version) because that gate is
        # now direction-specific for BUY_UP.
        # No estimate (fewer than vol_min_returns one-minute returns — e.g.
        # right after startup with REST unreachable) means no honest
        # probability, so reject rather than fall back to a guessed
        # threshold the way the old estimator did.
        vol = self.vol.sigma_per_sqrt_s(now)
        if vol is None:
            self.stats["rejected_no_volatility"] += 1
            return SignalEvaluation(
                Signal.NO_SIGNAL, 0.0, None,
                f"no volatility estimate yet (need {self.config.vol_min_returns} "
                f"one-minute BTC returns)", {},
            )
        # Floored so a genuinely quiet hour can't shrink the expected move
        # to nothing and hand distance_z — and so model_prob — an
        # arbitrarily large value (the old 0.02% floor bound on 40% of
        # trades; see vol_floor_per_sqrt_s).
        vol = max(vol, self.config.vol_floor_per_sqrt_s)
        expected_move = vol * math.sqrt(self._effective_seconds(seconds_remaining))
        deviation_pct = (current_btc - opening_btc) / opening_btc
        distance_z = deviation_pct / expected_move if expected_move > 0 else 0.0

        # A zero distance_z carries no directional information — either
        # current_btc == opening_btc exactly, or expected_move came back
        # non-positive (a degenerate/flat realized-volatility estimate).
        # `"UP" if distance_z > 0 else "DOWN"` would otherwise default
        # this tie to DOWN every time, and model_prob=Phi(0)=0.5 clears
        # min_model_prob_down (0.38 by default) easily — spuriously
        # admitting a trade with zero real signal behind it. Reject
        # directly instead of letting a fabricated direction reach the
        # model-probability gate.
        if distance_z == 0.0:
            self.stats["rejected_no_distance"] += 1
            return SignalEvaluation(Signal.NO_SIGNAL, 0.0, None, "no directional signal (zero deviation)", {})

        direction = "UP" if distance_z > 0 else "DOWN"
        model_prob = self._win_probability_estimate(distance_z)

        # LOG-ONLY (see ShortVolatility): the same probability with the
        # last 5 minutes' volatility. Same floor, same direction (vol > 0
        # keeps the sign). Nothing below gates on it.
        short_vol = self.short_vol.sigma_per_sqrt_s(now)
        model_prob_short = None
        if short_vol is not None:
            short_vol = max(short_vol, self.config.vol_floor_per_sqrt_s)
            short_move = short_vol * math.sqrt(self._effective_seconds(seconds_remaining))
            if short_move > 0:
                model_prob_short = self._win_probability_estimate(deviation_pct / short_move)

        # --- Hard gate: time window — user-specified for both directions
        # (2026-09-20): a pure bound on seconds_remaining, no elapsed check.
        min_sec, max_sec = (
            (self.config.min_seconds_to_resolution_up, self.config.max_seconds_to_resolution_up)
            if direction == "UP" else
            (self.config.min_seconds_to_resolution_down, self.config.max_seconds_to_resolution_down)
        )
        if not (min_sec <= seconds_remaining <= max_sec):
            self.stats["rejected_time_window"] += 1
            return SignalEvaluation(Signal.NO_SIGNAL, 0.0, direction, "outside trading window", {})

        # --- Candidate token's book ---
        book = yes_book if direction == "UP" else no_book
        if not book or not book.is_valid() or book.best_bid is None or book.best_ask is None:
            self.stats["rejected_no_book"] += 1
            return SignalEvaluation(Signal.NO_SIGNAL, 0.0, direction, "no valid book for candidate token", {})

        # Price the candidate off the VWAP fill for the bot's actual
        # trade_size, not top-of-book best_ask — run.py always crosses
        # the spread for the full order (see its own vwap_fill_price()
        # comment), and on a thin book that walks past best_ask into
        # worse levels. Using best_ask here would understate both the
        # ask-range check just below and the fee/edge math right after it
        # relative to the price actually paid at execution. None means
        # the book can't fill this size at all.
        candidate_ask = book.vwap_fill_price(self.trade_size, "buy")
        if candidate_ask is None:
            self.stats["rejected_depth"] += 1
            return SignalEvaluation(Signal.NO_SIGNAL, 0.0, direction, "insufficient book depth for trade size", {})

        # Fee-aware edge, per the user's own formula (tahmini_olasılık -
        # fiyat - ücret) — ücret is Polymarket's taker fee, same formula
        # PositionManager.TAKER_FEE_RATE_CRYPTO uses in dollar terms
        # (rate * price * (1-price)) but per single share so it's directly
        # subtractable here, since model_prob/candidate_ask are already
        # $-per-share terms.
        fee_per_share = self.config.taker_fee_rate * candidate_ask * (1 - candidate_ask)
        edge = model_prob - candidate_ask - fee_per_share
        diag = dict(
            distance_z=distance_z, candidate_ask=candidate_ask,
            model_prob=model_prob, edge=edge, vol_per_sqrt_s=vol,
            short_vol_per_sqrt_s=short_vol, model_prob_short_vol=model_prob_short,
            edge_short_vol=(None if model_prob_short is None
                            else model_prob_short - candidate_ask - fee_per_share),
        )

        # --- Direction-specific checks ---
        # (spread gate and spread_pct tracking both removed at the user's
        # explicit instruction 2026-09-22)
        if direction == "UP":
            min_model_prob = self.config.min_model_prob_up
            min_ask, max_ask = self.config.min_ask_up, self.config.max_ask
            min_edge = self.config.min_edge_up
        else:
            min_model_prob = self.config.min_model_prob_down
            min_ask, max_ask = self.config.min_ask, self.config.max_ask_down
            min_edge = self.config.min_edge

        if model_prob < min_model_prob:
            self.stats["rejected_model_prob"] += 1
            return SignalEvaluation(
                Signal.NO_SIGNAL, 0.0, direction,
                f"model_prob {model_prob:.3f} below minimum {min_model_prob:.3f}", {},
                **diag,
            )

        if not (min_ask <= candidate_ask <= max_ask):
            self.stats["rejected_ask_range"] += 1
            return SignalEvaluation(
                Signal.NO_SIGNAL, 0.0, direction,
                f"ask {candidate_ask:.4f} outside [{min_ask}, {max_ask}]", {},
                **diag,
            )

        if edge < min_edge:
            self.stats["rejected_edge"] += 1
            return SignalEvaluation(
                Signal.NO_SIGNAL, 0.0, direction,
                f"edge {edge:.3f} below minimum {min_edge:.3f} "
                f"(model_prob={model_prob:.3f}, ask={candidate_ask:.4f})", {},
                **diag,
            )

        # All gates passed.
        signal = Signal.BUY_UP if direction == "UP" else Signal.BUY_DOWN
        self.stats["buy_up_signals" if direction == "UP" else "buy_down_signals"] += 1

        # Throttled to once every FIRE_LOG_INTERVAL seconds of wall clock.
        #
        # evaluate() runs on every tick, and a tick fires on every WS
        # message — so while conditions hold, this line was written
        # hundreds of times a second with near-identical content. It was
        # the single largest thing in the log file: 16,948 lines and 47%
        # of all bytes over the 2026-09-22 session. Same reasoning, and
        # the same wall-clock pattern, as run.py's no-signal throttle.
        #
        # Nothing is lost that matters: a signal that actually becomes a
        # trade is logged separately and unthrottled by run.py
        # ([SIGNAL] / [SIGNAL-DIAG]), and a direction change always logs
        # immediately regardless of the interval, so a flip from BUY_UP to
        # BUY_DOWN is never hidden behind the throttle.
        now = time.time()
        if (signal is not self._last_logged_signal
                or now - self._last_fire_log_time >= self.FIRE_LOG_INTERVAL):
            suppressed = self._suppressed_fire_logs
            self._last_fire_log_time = now
            self._last_logged_signal = signal
            self._suppressed_fire_logs = 0
            self.logger.info(
                f"[MULTI-SIGNAL] {signal.value} model_prob={model_prob:.3f} "
                f"edge={edge:.3f} ask={candidate_ask:.4f} "
                f"distance_z={distance_z:.2f} vol5m={vol * math.sqrt(300) * 100:.3f}%"
                + (f" | log-only short vol5m={short_vol * math.sqrt(300) * 100:.3f}% "
                   f"model_prob={model_prob_short:.3f}" if model_prob_short is not None else "")
                + (f" (+{suppressed} similar suppressed)" if suppressed else "")
            )
        else:
            self._suppressed_fire_logs += 1

        # score kept as model_prob*100 purely for a human-readable log/
        # diagnostic number on a familiar 0-100 scale — not a weighted
        # composite anymore, see MultiFactorConfig's docstring.
        return SignalEvaluation(signal, model_prob * 100, direction, None, {}, **diag)
