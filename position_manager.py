"""
POSITION MANAGER — Open/Close Positions, Track PnL, Calculate Statistics
Manages position lifecycle: open → track → close → settle

Production-grade: thread-safe, error handling, PnL calculation, analytics
"""

import logging
import threading
import json
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import uuid

from database import Database, Position, Trade, PositionStatus, TradeStatus

logger = logging.getLogger("PositionManager")

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class OpenPosition:
    """In-memory open position tracker"""
    id: str
    order_id: str
    side: str  # BUY/SELL
    signal_type: str  # BUY_UP/BUY_DOWN
    entry_price: float
    entry_time: float
    size: int
    status: str = "open"
    exit_price: Optional[float] = None
    exit_time: Optional[float] = None
    pnl: Optional[float] = None
    pnl_percent: Optional[float] = None
    fee: float = 0.0  # Polymarket taker fee charged at entry (see _calculate_taker_fee)
    mid_price: Optional[float] = None  # book mid-price at signal time, pre-slippage
    condition_id: Optional[str] = None  # the 5-minute market this was opened in

# ============================================================================
# POSITION MANAGER
# ============================================================================

class PositionManager:
    """
    Manage all positions (open + closed)
    
    Features:
    - Open new positions
    - Track open positions
    - Close positions with PnL calculation
    - Calculate statistics
    - Thread-safe operations
    - Error handling
    """

    # Polymarket taker fee rate for the "Crypto" market category (this bot
    # only trades 5-minute BTC up/down markets, which fall in that
    # category). Per docs.polymarket.com/trading/fees:
    # fee = size * feeRate * price * (1 - price), charged only on the taker
    # side, only at match time. Makers pay zero. This bot always crosses
    # the spread to fill within its narrow signal window, so entry is
    # always a taker trade — see the entry pricing in run.py.
    # Exit is NOT a fee event: positions are held to resolution and
    # redeemed on-chain for $1/$0 per share rather than sold back on the
    # CLOB, and redemption isn't a match.
    #
    # DO NOT "CORRECT" THIS TO 0.0007. Re-verified against the docs
    # 2026-09-23. That page labels the crypto rate "0.07 (7 bps)", which
    # is self-contradictory — 7 basis points is 0.0007, a hundred times
    # smaller. Its own worked example settles it:
    #
    #     100 shares at $0.50  ->  documented fee $1.75
    #     100 * 0.07   * 0.5 * 0.5 = 1.7500   <- matches
    #     100 * 0.0007 * 0.5 * 0.5 = 0.0175   <- does not
    #
    # So 0.07 is a plain decimal multiplier and "bps" is loose wording on
    # their side. Reading it as basis points would understate the fee
    # 100x, which matters directly: fee is subtracted in
    # MultiFactorSignalEngine.evaluate()'s edge gate, where at a 0.50 ask
    # it is about 35% of the whole min_edge threshold.
    #
    # Other categories are cheaper (Sports/Economics 0.05, Finance/
    # Politics 0.04, Geopolitics 0) — crypto is the most expensive one,
    # and it is the only one this bot trades. The 20% "maker rebate"
    # column in that table redistributes collected fees to makers; this
    # bot is never a maker, so it does not apply. Smallest chargeable fee
    # is 0.00001 USDC, far below anything this bot's sizes produce.
    TAKER_FEE_RATE_CRYPTO = 0.07

    def __init__(self, db: Database):
        self.db = db
        
        # In-memory position cache
        self.open_positions: Dict[str, OpenPosition] = {}
        self.closed_positions: Dict[str, OpenPosition] = {}
        self.positions_lock = threading.RLock()
        # Positions read back from the database at startup: they belong to
        # a PREVIOUS run's markets, which have long since resolved. Kept
        # separate so they can never be settled against whatever market
        # happens to be open now — see settle_orphaned_positions().
        self._orphan_ids: set = set()
        # Serializes resettle_for_official_resolution(). Two different
        # background threads can reach it — ResolutionWatcher when a
        # market this run traded settles, and the orphan-recovery thread
        # for markets a previous run left behind. They normally work on
        # disjoint markets, but nothing guarantees it, and two concurrent
        # passes over the same one would each read the pre-correction
        # rows and each apply the same delta to the in-memory counters.
        self._resettle_lock = threading.RLock()
        
        # Statistics
        self.stats = {
            "positions_opened": 0,
            "positions_closed": 0,
            "total_pnl": 0.0,
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0
        }
        self.stats_lock = threading.RLock()
        
        # Load existing positions from database
        self._load_from_db()
        
        logger.info("[POS] PositionManager initialized")
    
    # ========================================================================
    # DATABASE OPERATIONS
    # ========================================================================
    
    def _load_from_db(self):
        """Load open positions from database on startup"""
        try:
            open_pos = self.db.get_open_positions()
            
            with self.positions_lock:
                for pos in open_pos:
                    # signal_type/size/fee/mid_price are all real columns on
                    # the DB row (see database.py's Position dataclass) —
                    # previously hardcoded to "UNKNOWN"/0 here, which meant
                    # a position that survived a crash (the only way it'd
                    # still be OPEN at startup — a clean shutdown already
                    # closes everything in _cleanup()) would settle as a
                    # 0-pnl VOID on close_all_positions() instead of its
                    # real win/loss, since "UNKNOWN" never matches
                    # BUY_UP/BUY_DOWN there.
                    open_pos_obj = OpenPosition(
                        id=pos.id,
                        order_id=pos.order_id,
                        side="BUY",  # Extract from order if needed
                        signal_type=pos.signal_type or "UNKNOWN",
                        entry_price=pos.entry_price,
                        entry_time=pos.entry_time,
                        size=pos.size or 0,
                        status=pos.status,
                        fee=pos.fee or 0.0,
                        mid_price=pos.mid_price,
                        condition_id=pos.condition_id
                    )
                    self.open_positions[pos.id] = open_pos_obj
                    self._orphan_ids.add(pos.id)

            if open_pos:
                logger.warning(
                    f"[POS] Loaded {len(open_pos)} open position(s) left over from a "
                    f"previous run — these belong to markets that have already ended "
                    f"and must NOT be settled against the current one "
                    f"(see settle_orphaned_positions)"
                )
            else:
                logger.info("[POS] No carried-over open positions")
        
        except Exception as e:
            logger.error(f"[POS] Failed to load positions from DB: {e}")
    
    # ========================================================================
    # OPEN POSITION
    # ========================================================================
    
    def open_position(self, order_id: str, side: str, signal_type: str,
                     entry_price: float, size: int,
                     signal_features: Optional[Dict[str, Any]] = None,
                     mid_price: Optional[float] = None,
                     condition_id: Optional[str] = None) -> Optional[str]:
        """
        Open new position

        Args:
            order_id: Order ID
            side: BUY/SELL
            signal_type: BUY_UP/BUY_DOWN
            entry_price: Entry price (real VWAP fill price, slippage-inclusive)
            size: Position size
            signal_features: optional snapshot of signal state at entry
                (BTC deviation, OBI, trade flow, TWAP, etc.) — persisted as
                JSON alongside the position for later Phase 3 model
                training. Not used for anything at trading time.
            condition_id: Polymarket condition_id of the market this position
                belongs to. Recorded so the trade can be re-settled later
                against Polymarket's own official resolution — see
                resettle_for_official_resolution().
            mid_price: book mid-price at signal time, before the VWAP walk —
                persisted alongside entry_price so slippage cost
                ((entry_price - mid_price) * size) can be reconstructed per
                trade later instead of only living in the log line.

        Returns: position_id or None
        """

        try:
            pos_id = f"pos_{uuid.uuid4().hex[:16]}"
            entry_time = datetime.now().timestamp()
            fee = self._calculate_taker_fee(entry_price, size)

            # Create in-memory position
            open_pos = OpenPosition(
                id=pos_id,
                order_id=order_id,
                side=side,
                signal_type=signal_type,
                entry_price=entry_price,
                entry_time=entry_time,
                size=size,
                fee=fee,
                mid_price=mid_price,
                condition_id=condition_id
            )

            # Store in memory
            with self.positions_lock:
                self.open_positions[pos_id] = open_pos

            # Save to database
            try:
                features_json = json.dumps(signal_features) if signal_features else None
            except (TypeError, ValueError) as e:
                logger.warning(f"[POS] Could not serialize signal_features: {e}")
                features_json = None

            db_pos = Position(
                id=pos_id,
                order_id=order_id,
                entry_price=entry_price,
                entry_time=entry_time,
                status=PositionStatus.OPEN.value,
                signal_type=signal_type,
                size=size,
                signal_features=features_json,
                fee=fee,
                mid_price=mid_price,
                condition_id=condition_id
            )
            self.db.open_position(db_pos)
            
            # Update stats
            with self.stats_lock:
                self.stats["positions_opened"] += 1
            
            logger.info(f"[POS] Position opened: {pos_id} @ {entry_price} ({signal_type}), taker fee={fee:.4f}")
            return pos_id
        
        except Exception as e:
            logger.error(f"[POS] Failed to open position: {e}")
            return None
    
    # ========================================================================
    # CLOSE POSITION
    # ========================================================================
    
    def close_position(self, position_id: str, exit_price: float, is_void: bool = False) -> Optional[Dict[str, Any]]:
        """
        Close open position with exit price

        Args:
            position_id: Position ID
            exit_price: Exit price
            is_void: True when this isn't a real market resolution (e.g.
                the bot was interrupted mid-cycle and close_all_positions()
                fell back to exit_price=entry_price) — forces status VOID
                regardless of the computed pnl's sign. Without this, a
                breakeven exit still nets pnl=-fee (whenever fee>0), which
                the sign-only WIN/LOSS/VOID check below would otherwise
                record as a LOSS for an outcome that was never observed,
                corrupting win-rate and loss-streak statistics with an
                outcome that never happened.

        Returns: {id, pnl, pnl_percent, status} or None
        """
        
        try:
            with self.positions_lock:
                if position_id not in self.open_positions:
                    logger.error(f"[POS] Position not found: {position_id}")
                    return None
                
                pos = self.open_positions[position_id]
            
            # Calculate PnL — gross price movement minus the taker fee
            # charged at entry (redemption at exit is fee-free, see
            # TAKER_FEE_RATE_CRYPTO).
            exit_time = datetime.now().timestamp()
            gross_pnl = self._calculate_pnl(pos.side, pos.entry_price, exit_price, pos.size)
            pnl = gross_pnl - pos.fee
            # Guard the whole denominator, not just entry_price: size can
            # legitimately be 0 here. _load_from_db() reads `pos.size or 0`,
            # so any pre-migration row with a NULL size comes back as 0 —
            # and those are exactly the rows settle_orphaned_positions()
            # now closes at startup, which would have raised
            # ZeroDivisionError on the first carried-over position.
            notional = pos.entry_price * pos.size
            pnl_percent = (pnl / notional) * 100 if notional > 0 else 0.0
            
            # Determine trade status (WIN/LOSS/VOID)
            if is_void:
                trade_status = TradeStatus.VOID.value
            elif pnl > 0:
                trade_status = TradeStatus.WIN.value
            elif pnl < 0:
                trade_status = TradeStatus.LOSS.value
            else:
                trade_status = TradeStatus.VOID.value
            
            # Build the completed trade row
            trade = Trade(
                id=f"trade_{uuid.uuid4().hex[:16]}",
                position_id=position_id,
                entry_time=pos.entry_time,
                exit_time=exit_time,
                entry_price=pos.entry_price,
                exit_price=exit_price,
                size=pos.size,
                pnl=pnl,
                pnl_percent=pnl_percent,
                status=trade_status,
                fee=pos.fee,
                mid_price=pos.mid_price,
                condition_id=pos.condition_id
            )

            # Update database — position + trade in one transaction (see
            # Database.close_position_with_trade's docstring) so a failure
            # partway through can't leave a position marked CLOSED with no
            # matching trades row.
            db_ok = self.db.close_position_with_trade(
                position_id=position_id,
                exit_price=exit_price,
                pnl=pnl,
                pnl_percent=pnl_percent,
                trade=trade
            )
            if not db_ok:
                logger.error(f"[POS] DB write failed for closed position {position_id} — in-memory state will still update")
            
            # Update memory
            with self.positions_lock:
                pos.exit_price = exit_price
                pos.exit_time = exit_time
                pos.pnl = pnl
                pos.pnl_percent = pnl_percent
                pos.status = PositionStatus.CLOSED.value
                
                # Move to closed
                self.closed_positions[position_id] = pos
                del self.open_positions[position_id]
            
            # Update stats
            with self.stats_lock:
                self.stats["positions_closed"] += 1
                self.stats["total_pnl"] += pnl
                self.stats["total_trades"] += 1
                
                if trade_status == TradeStatus.WIN.value:
                    self.stats["wins"] += 1
                elif trade_status == TradeStatus.LOSS.value:
                    self.stats["losses"] += 1
                
                # Recalculate win rate
                if self.stats["total_trades"] > 0:
                    self.stats["win_rate"] = (self.stats["wins"] / self.stats["total_trades"]) * 100
            
            logger.info(
                f"[POS] Position closed: {position_id} "
                f"Entry={pos.entry_price} Exit={exit_price} "
                f"PnL={pnl:.2f} ({pnl_percent:.2f}%) {trade_status.upper()} "
                f"[gross={gross_pnl:.2f}, fee={pos.fee:.4f}]"
            )
            
            return {
                "id": position_id,
                "pnl": pnl,
                "pnl_percent": pnl_percent,
                "status": trade_status
            }
        
        except Exception as e:
            logger.error(f"[POS] Failed to close position: {e}")
            return None
    
    # ========================================================================
    # PNL CALCULATION
    # ========================================================================
    
    def _calculate_pnl(self, side: str, entry_price: float, exit_price: float, size: int) -> float:
        """
        Calculate PnL
        
        BUY: profit if exit > entry
        SELL: profit if exit < entry
        """
        
        if side.upper() == "BUY":
            # Long position: profit if price goes up
            pnl = (exit_price - entry_price) * size
        elif side.upper() == "SELL":
            # Short position: profit if price goes down
            pnl = (entry_price - exit_price) * size
        else:
            pnl = 0.0

        return pnl

    def _calculate_taker_fee(self, entry_price: float, size: int) -> float:
        """
        Polymarket taker fee for opening this position, per the official
        formula (docs.polymarket.com/trading/fees):

            fee = size * feeRate * price * (1 - price)

        Peaks at price=0.50 and shrinks toward the extremes (0 or 1) —
        symmetric in dollar terms around 50% probability. Only takers pay;
        this bot is always a taker on entry (see TAKER_FEE_RATE_CRYPTO).
        """
        price = max(0.0, min(1.0, entry_price))
        return size * self.TAKER_FEE_RATE_CRYPTO * price * (1 - price)
    
    # ========================================================================
    # POSITION QUERIES
    # ========================================================================
    
    def get_open_positions(self) -> List[OpenPosition]:
        """Get all open positions"""
        with self.positions_lock:
            return list(self.open_positions.values())
    
    def get_position(self, position_id: str) -> Optional[OpenPosition]:
        """Get specific position"""
        with self.positions_lock:
            return self.open_positions.get(position_id)
    
    def get_closed_positions(self) -> List[OpenPosition]:
        """Get all closed positions"""
        with self.positions_lock:
            return list(self.closed_positions.values())
    
    def has_open_position(self, condition_id: Optional[str] = None) -> bool:
        """
        Is a position open? Scoped to one market when condition_id is given.

        run.py uses this to enforce "one trade per market", so it must ask
        about THIS market. Asking globally meant a position carried over
        from a previous run — which belongs to a market that ended hours
        ago — blocked the current market from ever trading. Passing None
        keeps the old global meaning for callers that genuinely want it.
        """
        with self.positions_lock:
            if condition_id is None:
                return len(self.open_positions) > 0
            return any(p.condition_id == condition_id for p in self.open_positions.values())
    
    def count_open_positions(self) -> int:
        """Count open positions"""
        with self.positions_lock:
            return len(self.open_positions)
    
    # ========================================================================
    # STATISTICS
    # ========================================================================
    
    def get_session_pnl(self) -> float:
        """Get total PnL for current session"""
        with self.stats_lock:
            return self.stats["total_pnl"]
    
    def get_win_rate(self) -> float:
        """Get win rate percentage"""
        with self.stats_lock:
            return self.stats["win_rate"]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get all statistics"""
        with self.stats_lock:
            return {
                "positions_opened": self.stats["positions_opened"],
                "positions_closed": self.stats["positions_closed"],
                "open_count": self.count_open_positions(),
                "total_pnl": self.stats["total_pnl"],
                "total_trades": self.stats["total_trades"],
                "wins": self.stats["wins"],
                "losses": self.stats["losses"],
                "win_rate": self.stats["win_rate"],
                "avg_pnl": (self.stats["total_pnl"] / self.stats["total_trades"]) 
                           if self.stats["total_trades"] > 0 else 0.0
            }
    
    def export_stats(self) -> str:
        """Export stats as formatted string"""
        stats = self.get_stats()
        
        lines = [
            "="*60,
            "POSITION STATISTICS",
            "="*60,
            f"Positions opened:    {stats['positions_opened']}",
            f"Positions closed:    {stats['positions_closed']}",
            f"Open positions:      {stats['open_count']}",
            f"Total trades:        {stats['total_trades']}",
            f"Wins:                {stats['wins']}",
            f"Losses:              {stats['losses']}",
            f"Win rate:            {stats['win_rate']:.1f}%",
            f"Total PnL:           ${stats['total_pnl']:.2f}",
            f"Avg PnL per trade:   ${stats['avg_pnl']:.2f}",
            "="*60
        ]
        
        return "\n".join(lines)
    
    # ========================================================================
    # MARKET RESOLUTION
    # ========================================================================
    
    def close_all_positions(self, resolution: Optional[str] = None,
                            exit_price: Optional[float] = None,
                            only_condition_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Close all open positions (market resolution or forced shutdown)
        Used when market closes

        Args:
            resolution: "UP" or "DOWN" — settle each position on whether its
                own signal_type (BUY_UP/BUY_DOWN) called it correctly, binary
                payout (1.0 win / 0.0 loss). A BUY_UP and a BUY_DOWN position
                opened in the same window do NOT share the same outcome, so
                this must be resolved per-position rather than at one flat
                price.
            only_condition_id: settle ONLY positions belonging to this
                market. A resolution is a fact about one specific 5-minute
                window, so applying it to a position opened in a different
                window is simply wrong — it pays out 1.0 or 0.0 on an
                outcome that position never had any relationship to. That
                is what used to happen to any position carried over from a
                previous run: the first _end_market() of the new run swept
                it up and settled it against a market it was never in.
                None means no filtering (used by shutdown, which VOIDs
                everything at its own entry price and so cannot mis-settle
                anything).
            exit_price: flat price applied to every position instead, for
                when there's no resolution to settle against (e.g. bot
                interrupted mid-market). Defaults to each position's own
                entry_price (true breakeven/VOID, pnl=0) — NOT a flat 0.5,
                which used to silently book a fake win or loss on every
                interrupted position whose entry price wasn't itself ~0.50
                (most of them), corrupting win-rate/PnL history for an
                outcome that was never actually observed.

        Returns: list of closed positions with PnL
        """

        try:
            results = []

            open_pos = self.get_open_positions()

            if only_condition_id is not None:
                skipped = [p for p in open_pos if p.condition_id != only_condition_id]
                open_pos = [p for p in open_pos if p.condition_id == only_condition_id]
                if skipped:
                    logger.warning(
                        f"[POS] Leaving {len(skipped)} position(s) from other markets "
                        f"untouched — a resolution only applies to its own window: "
                        f"{[(p.id[:12], (p.condition_id or 'none')[:12]) for p in skipped]}"
                    )

            logger.info(
                f"[POS] Closing {len(open_pos)} positions "
                f"(resolution={resolution}, exit_price={exit_price}, "
                f"market={(only_condition_id or 'all')[:16]})"
            )

            for pos in open_pos:
                is_void = resolution not in ("UP", "DOWN")
                if not is_void:
                    won = (
                        (pos.signal_type == "BUY_UP" and resolution == "UP") or
                        (pos.signal_type == "BUY_DOWN" and resolution == "DOWN")
                    )
                    pos_exit_price = 1.0 if won else 0.0
                else:
                    pos_exit_price = exit_price if exit_price is not None else pos.entry_price

                result = self.close_position(pos.id, pos_exit_price, is_void=is_void)
                if result:
                    results.append(result)

            logger.info(f"[POS] Closed {len(results)} positions")
            return results

        except Exception as e:
            logger.error(f"[POS] Failed to close all positions: {e}")
            return []
    
    def settle_orphaned_positions(self) -> List[str]:
        """
        Close positions carried over from a previous run and return the
        condition_ids they belonged to, so the caller can try to recover
        each market's official outcome.

        A position is only still OPEN at startup because the bot died
        without closing it — a clean shutdown VOIDs everything in
        _cleanup(). Its market therefore ended some time ago and has
        already resolved for real. Two things used to go wrong with it:

          1. The first _end_market() of the new run settled it against the
             CURRENT market's resolution, paying 1.0 or 0.0 on an outcome
             from a completely different five-minute window.
          2. Until then it made has_open_position() true, so the new run's
             first market could not trade at all.

        Closing them here, VOID at their own entry price, fixes both
        without inventing an outcome. The real outcome is recoverable
        afterwards: each condition_id returned can be looked up against
        Polymarket's official resolution and fed to
        resettle_for_official_resolution(), which upgrades a VOID row to
        its true WIN/LOSS exactly as it does for a live cycle.
        """
        with self.positions_lock:
            orphans = [self.open_positions[pid] for pid in list(self._orphan_ids)
                       if pid in self.open_positions]

        if not orphans:
            with self.positions_lock:
                self._orphan_ids.clear()
            return []

        logger.warning(
            f"[POS] Settling {len(orphans)} orphaned position(s) from a previous run "
            f"as VOID (their own markets already resolved; the real outcome is "
            f"recovered separately if available)"
        )

        condition_ids = []
        for pos in orphans:
            if pos.condition_id:
                condition_ids.append(pos.condition_id)
            else:
                logger.warning(
                    f"[POS] Orphan {pos.id[:12]} has no condition_id (predates that "
                    f"column) — settling VOID with no way to recover its real outcome"
                )
            # VOID at its own entry price: pnl is just the entry fee, which
            # was genuinely paid. Not a fabricated win or loss.
            self.close_position(pos.id, pos.entry_price, is_void=True)

        with self.positions_lock:
            self._orphan_ids.clear()

        # De-duplicated: several orphans can share one market.
        return list(dict.fromkeys(condition_ids))

    def resettle_for_official_resolution(self, condition_id: str,
                                         official_resolution: str) -> int:
        """
        Correct a finished market's trades against Polymarket's OWN
        official resolution. Returns how many rows were changed.

        Why this exists: positions have to close the moment their
        5-minute window ends, because the next window starts seconds
        later and has_open_position() gates one trade per market. But
        Polymarket's official settlement lands 2-5 minutes after that.
        So the bot settles immediately on its own BTC-feed-derived
        UP/DOWN, and this reconciles afterwards.

        It is not a rare correction. Measured over the 2026-09-22 session,
        the bot's own call disagreed with Polymarket's official one on 8
        of 43 resolved markets — 18.6%. Until this ran, that disagreement
        was recorded in market_sessions and then ignored: every trade kept
        the P&L implied by the bot's own guess, so roughly one trade in
        five carried the wrong win/loss label. Every number downstream —
        win rate, daily P&L, the consecutive-loss circuit breaker, and any
        future threshold tuning — was being computed off those labels.

        VOID trades are re-settled too, not skipped. A cycle the bot could
        not resolve itself (no fresh BTC price at the boundary, or an
        interrupted run) still held a real position in a market that did
        resolve, so the official result is exactly what it should settle
        against.
        """
        if official_resolution not in ("UP", "DOWN"):
            return 0
        if not condition_id:
            return 0

        # Held across the read-compare-write below so a second thread
        # cannot act on rows this one is already correcting. Cheap: this
        # runs at most a few times per five-minute cycle, off the trading
        # path entirely.
        with self._resettle_lock:
            return self._resettle_locked(condition_id, official_resolution)

    def _resettle_locked(self, condition_id: str, official_resolution: str) -> int:
        """The body of resettle_for_official_resolution(), under its lock."""
        try:
            rows = self.db.get_settled_trades_for_condition(condition_id)
        except Exception as e:
            logger.error(f"[POS] Could not load trades for re-settlement: {e}")
            return 0

        if not rows:
            return 0

        corrected = 0
        for row in rows:
            signal_type = row.get("signal_type")
            if signal_type not in ("BUY_UP", "BUY_DOWN"):
                # Direction unknown (a pre-migration row) — there is no
                # way to derive the correct payout, so leave it alone
                # rather than guess.
                continue

            won = (
                (signal_type == "BUY_UP" and official_resolution == "UP") or
                (signal_type == "BUY_DOWN" and official_resolution == "DOWN")
            )
            correct_exit = 1.0 if won else 0.0

            old_exit = row.get("exit_price")
            if old_exit is not None and abs(old_exit - correct_exit) < 1e-9:
                continue  # already settled correctly — nothing to change

            entry_price = row.get("entry_price") or 0.0
            size = row.get("size") or 0
            fee = row.get("fee") or 0.0
            gross_pnl = (correct_exit - entry_price) * size
            pnl = gross_pnl - fee
            notional = entry_price * size
            pnl_percent = (pnl / notional) * 100 if notional > 0 else 0.0

            if pnl > 0:
                status = TradeStatus.WIN.value
            elif pnl < 0:
                status = TradeStatus.LOSS.value
            else:
                status = TradeStatus.VOID.value

            ok = self.db.resettle_trade(
                trade_id=row["id"], position_id=row["position_id"],
                exit_price=correct_exit, pnl=pnl, pnl_percent=pnl_percent,
                status=status, official_resolution=official_resolution,
            )
            if not ok:
                continue

            corrected += 1
            self._apply_resettlement_to_stats(
                position_id=row["position_id"],
                old_pnl=row.get("pnl") or 0.0, old_status=row.get("status"),
                new_pnl=pnl, new_status=status,
                new_exit_price=correct_exit, new_pnl_percent=pnl_percent,
            )

        if corrected:
            logger.warning(
                f"[POS] Re-settled {corrected} trade(s) on Polymarket's official "
                f"resolution ({official_resolution}) for market {condition_id[:16]}... "
                f"— our own call had been wrong"
            )
            # The cycle's own market_sessions row summarizes these same
            # trades and was written before the correction, so bring it
            # back in line — otherwise the summary contradicts its rows.
            try:
                self.db.refresh_session_totals(condition_id)
            except Exception as e:
                logger.debug(f"[POS] Could not refresh cycle totals: {e}")
        else:
            # Agreed (or nothing to change): still stamp the official
            # result so "checked and agreed" stays distinguishable from
            # "never checked".
            try:
                self.db.mark_trades_official_resolution(condition_id, official_resolution)
            except Exception as e:
                logger.debug(f"[POS] Could not stamp official resolution: {e}")

        return corrected

    def _apply_resettlement_to_stats(self, position_id: str, old_pnl: float,
                                     old_status: Optional[str], new_pnl: float,
                                     new_status: str, new_exit_price: float,
                                     new_pnl_percent: float):
        """
        Move this process's in-memory counters from the old settlement to
        the corrected one.

        Only applies when the trade is one THIS process closed. After a
        restart the trade is on disk but was never counted here, so
        adjusting the counters for it would corrupt them in the opposite
        direction — the DB rows are corrected either way, and those are
        what the circuit breakers actually read.
        """
        with self.positions_lock:
            pos = self.closed_positions.get(position_id)
            if pos is None:
                return
            pos.exit_price = new_exit_price
            pos.pnl = new_pnl
            pos.pnl_percent = new_pnl_percent

        with self.stats_lock:
            self.stats["total_pnl"] += (new_pnl - old_pnl)
            if old_status == TradeStatus.WIN.value:
                self.stats["wins"] -= 1
            elif old_status == TradeStatus.LOSS.value:
                self.stats["losses"] -= 1
            if new_status == TradeStatus.WIN.value:
                self.stats["wins"] += 1
            elif new_status == TradeStatus.LOSS.value:
                self.stats["losses"] += 1
            if self.stats["total_trades"] > 0:
                self.stats["win_rate"] = (self.stats["wins"] / self.stats["total_trades"]) * 100

    # ========================================================================
    # REPORTING
    # ========================================================================
    
    def print_open_positions(self):
        """Print all open positions"""
        open_pos = self.get_open_positions()
        
        if not open_pos:
            logger.info("[POS] No open positions")
            return
        
        logger.info(f"[POS] Open positions ({len(open_pos)}):")
        
        for pos in open_pos:
            logger.info(
                f"  {pos.id}: {pos.signal_type} @ {pos.entry_price} "
                f"({pos.size} tokens, {pos.side})"
            )
    
    def print_statistics(self):
        """Print statistics"""
        print(self.export_stats())

# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    
    # Initialize
    db = Database("test_bot.db")
    pos_manager = PositionManager(db)
    
    logger.info("="*80)
    logger.info("POSITION MANAGER TEST")
    logger.info("="*80)
    
    # Open positions
    pos_id_1 = pos_manager.open_position(
        order_id="order_001",
        side="BUY",
        signal_type="BUY_DOWN",
        entry_price=0.65,
        size=10
    )
    
    pos_id_2 = pos_manager.open_position(
        order_id="order_002",
        side="BUY",
        signal_type="BUY_UP",
        entry_price=0.60,
        size=15
    )
    
    # Print open positions
    pos_manager.print_open_positions()
    
    # Close positions
    result_1 = pos_manager.close_position(pos_id_1, exit_price=0.70)
    result_2 = pos_manager.close_position(pos_id_2, exit_price=0.55)
    
    print(f"\nResult 1: {result_1}")
    print(f"Result 2: {result_2}")
    
    # Print statistics
    pos_manager.print_statistics()
    
    db.close()
