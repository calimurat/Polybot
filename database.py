"""
DATABASE MANAGER — SQLite Storage
Orders, positions, trades, market sessions, order book ticks

Production-grade: schema validation, CRUD, transactions, error handling
"""

import sqlite3
import logging
import threading
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

logger = logging.getLogger("Database")

# ============================================================================
# ENUMS
# ============================================================================

class OrderStatus(Enum):
    """Order status"""
    OPEN = "open"
    FILLED = "filled"
    LOST = "lost"
    VOID = "void"
    CANCELLED = "cancelled"

class PositionStatus(Enum):
    """Position status"""
    OPEN = "open"
    CLOSED = "closed"

class TradeStatus(Enum):
    """Trade outcome"""
    WIN = "win"
    LOSS = "loss"
    VOID = "void"

class OrderSide(Enum):
    """Order side"""
    BUY = "buy"
    SELL = "sell"

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Order:
    """Order record"""
    id: str
    timestamp: float
    side: str  # BUY/SELL
    price: float
    size: int
    status: str  # OPEN/FILLED/LOST/VOID
    tx_hash: Optional[str] = None
    filled_price: Optional[float] = None
    signal_type: Optional[str] = None  # BUY_UP/BUY_DOWN
    created_at: str = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()

@dataclass
class Position:
    """Open position record"""
    id: str
    order_id: str
    entry_price: float
    entry_time: float
    status: str  # OPEN/CLOSED
    exit_price: Optional[float] = None
    exit_time: Optional[float] = None
    pnl: Optional[float] = None
    pnl_percent: Optional[float] = None
    created_at: str = None
    signal_type: Optional[str] = None       # BUY_UP/BUY_DOWN — the direction actually bet
    size: Optional[int] = None
    signal_features: Optional[str] = None   # JSON dict snapshot of signal state at entry
                                             # (BTC deviation, OBI, trade flow, TWAP, etc.) —
                                             # for Phase 3 model training later.
    fee: Optional[float] = None             # Polymarket taker fee charged at entry
                                             # (see PositionManager.TAKER_FEE_RATE_CRYPTO)
    mid_price: Optional[float] = None       # book mid-price at signal time, before the
                                             # VWAP-walked fill (entry_price) — the gap
                                             # between the two is the slippage cost,
                                             # (entry_price - mid_price) * size
    condition_id: Optional[str] = None      # Polymarket condition_id of the 5-minute
                                             # market this position was opened in. Without
                                             # it a row cannot be tied back to the window
                                             # that produced it, which is what makes both
                                             # official-resolution re-settlement and
                                             # crash-recovery matching possible.

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()

@dataclass
class Trade:
    """Completed trade record"""
    id: str
    position_id: str
    entry_time: float
    exit_time: float
    entry_price: float
    exit_price: float
    size: int
    pnl: float
    pnl_percent: float
    status: str  # WIN/LOSS/VOID
    duration_seconds: float = None
    created_at: str = None
    fee: Optional[float] = None
    mid_price: Optional[float] = None
    condition_id: Optional[str] = None      # copied from the position, so a trade can be
                                             # looked up by market without a join
    official_resolution: Optional[str] = None  # Polymarket's own settlement, once known
    resettled: Optional[int] = None         # 1 if this row's P&L was corrected after the
                                             # official resolution disagreed with ours

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.duration_seconds is None:
            self.duration_seconds = self.exit_time - self.entry_time

@dataclass
class MarketSession:
    """
    One row per 5-minute BTC cycle — whether or not a trade was opened.
    Lets later analysis see what the bot passed on (score, rejected_reason,
    signal_features) with the same detail a traded cycle gets, not just
    the cycles it acted on.

    QUERYING THIS TABLE: signal_type does NOT mean "a trade happened".
    It records the cycle's final decision, and a cycle whose signal passed
    every gate and then failed on the way to an order — slippage, book
    depth, execution latency — is stored with signal_type='BUY_UP' (or
    'BUY_DOWN'), trade_opened=0, and a rejected_reason starting
    'execution:'. That is deliberate: those near-misses are the most
    useful rows in the table, and the older behaviour of filing them as
    plain NO_SIGNAL is exactly what hid an unreachable slippage limit
    while it rejected 16,946 signals.

    So `WHERE signal_type = 'BUY_UP'` counts intentions, not trades. For
    actual trades use `WHERE trade_opened = 1`, or join positions /
    trades on position_id.

    Other values signal_type can take: 'NO_SIGNAL' (an engine gate
    rejected it) and 'RISK_PAUSED' (a circuit breaker stopped the cycle
    before any signal was evaluated).
    """
    session_id: str
    open_time: float
    close_time: float
    opening_price: float
    closing_price: float
    resolution: Optional[str] = None  # UP/DOWN/VOID — our own real-time call
    total_trades: int = 0
    total_pnl: float = 0.0
    win_rate: float = 0.0
    cycle_number: Optional[int] = None
    signal_type: Optional[str] = None      # BUY_UP/BUY_DOWN/NO_SIGNAL/RISK_PAUSED — the
                                            # cycle's final decision. NOT a trade flag; see
                                            # this class's docstring before querying on it.
    trade_opened: int = 0                  # 0/1 — THIS is the trade flag
    position_id: Optional[str] = None
    rejected_reason: Optional[str] = None
    score: Optional[float] = None
    signal_features: Optional[str] = None  # JSON, same shape as positions.signal_features
    official_resolution: Optional[str] = None  # filled in later, async, by ResolutionWatcher
    resolution_match: Optional[int] = None     # 0/1, filled in alongside official_resolution
    created_at: str = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class Database:
    """
    SQLite database manager
    
    Tables:
    - orders: All trades (paper + real)
    - positions: Open positions
    - trades: Completed trades
    - market_sessions: 5-minute market sessions
    """
    
    def __init__(self, db_file: str = "bot_trades.db"):
        self.db_file = db_file
        # Thread-local, not one shared connection: sqlite3 connections are
        # thread-affine (check_same_thread=True by default), and this
        # class is called from more than one thread — the main thread
        # during cycle setup/teardown, and the "SignalEvaluator" thread
        # during a live trading cycle (see run.py's event-driven
        # _on_market_tick()). A single shared self.conn touched from a
        # second thread raised sqlite3.ProgrammingError on every call,
        # silently swallowed by each method's broad except-and-log-and-
        # return-default — which meant the risk circuit breakers (since
        # removed) always read a "no risk" default and every position
        # write silently no-op'd. Each thread
        # now gets and keeps its own connection instead.
        self._local = threading.local()
        self._init_db()
        logger.info(f"[DB] Initialized: {db_file}")

    # ========================================================================
    # CONNECTION MANAGEMENT
    # ========================================================================

    def _get_connection(self) -> sqlite3.Connection:
        """Get (or lazily create) the CALLING THREAD's own connection."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_file, timeout=10)
            conn.row_factory = sqlite3.Row
            # timeout=10 above already sets busy_timeout, but state it
            # explicitly so the value survives any change to how the
            # connection is opened.
            conn.execute("PRAGMA busy_timeout = 10000")
            self._local.conn = conn
        return conn

    def close(self):
        """
        Close the CALLING THREAD's connection.

        It cannot close any other thread's: sqlite3 connections are
        thread-affine (check_same_thread), and calling close() on one from
        a different thread raises ProgrammingError — verified, not assumed.
        A central registry closed from the main thread is therefore not
        possible without disabling that guard, which is a real protection
        worth keeping.

        So every thread that opens one is responsible for closing it.
        The one that repeats is run.py's SignalEvaluator, recreated each
        5-minute cycle; it closes its own on the way out of
        _evaluator_loop(). The rest are process-lifetime daemons whose
        connections go away with the process.

        Not closing one is NOT a leak, despite how it looks: a dead
        thread's thread-local connection becomes unreachable and the
        garbage collector reclaims it. Measured over 20 worker threads,
        open connections after gc.collect() were unchanged whether or not
        close() was called. What the explicit close buys is promptness and
        clear ownership, not a fixed resource leak — worth having, not
        worth mistaking for a bug fix.
        """
        conn = getattr(self._local, "conn", None)
        if conn:
            conn.close()
            self._local.conn = None
            logger.debug("[DB] Connection closed for thread %s", threading.current_thread().name)
    
    def _init_db(self):
        """Initialize database schema"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Write-Ahead Logging. The default journal mode ("delete",
            # confirmed on this database) makes a writer block every
            # reader and a reader block every writer. This process has
            # several writers — the main thread during cycle setup and
            # teardown, run.py's SignalEvaluator thread (the risk circuit
            # breaker reads here roughly once a second and open_position()
            # writes from it), the ResolutionWatcher thread, and the
            # orphan-recovery thread — plus the dashboard polling reads.
            # Under "delete" a blocked statement waits out busy_timeout,
            # which is 10 seconds: on the evaluator thread that is a
            # 10-second stall in the path that prices and places a trade.
            # WAL lets readers and the writer proceed concurrently.
            #
            # Set once and persisted in the file itself, so this runs as a
            # no-op on every later startup.
            mode = cursor.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                logger.warning(
                    f"[DB] Could not enable WAL (journal_mode={mode}) — continuing, "
                    f"but concurrent readers and writers will block each other"
                )
            else:
                logger.info("[DB] journal_mode=WAL")

            # synchronous is deliberately left at its default (FULL).
            # NORMAL is the usual companion to WAL and is faster, but it
            # trades away durability of the most recent commits on an OS
            # crash or power loss. The rows here are the record of money
            # having moved; writes are a handful per five-minute cycle, so
            # there is nothing to gain by relaxing it.

            # Orders table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    timestamp REAL,
                    side TEXT,
                    price REAL,
                    size INTEGER,
                    status TEXT,
                    tx_hash TEXT,
                    filled_price REAL,
                    signal_type TEXT,
                    created_at TEXT
                )
            """)
            
            # Positions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id TEXT PRIMARY KEY,
                    order_id TEXT UNIQUE,
                    entry_price REAL,
                    entry_time REAL,
                    exit_price REAL,
                    exit_time REAL,
                    pnl REAL,
                    pnl_percent REAL,
                    status TEXT,
                    created_at TEXT,
                    signal_type TEXT,
                    size INTEGER,
                    signal_features TEXT,
                    fee REAL,
                    mid_price REAL,
                    condition_id TEXT,
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )
            """)

            # Migration: an existing positions table (created before these
            # columns existed) won't get them from CREATE TABLE IF NOT
            # EXISTS above — add them if missing. Needed for Phase 3 data
            # collection (signal_type/size/signal_features weren't
            # persisted before; without signal_type in particular, a
            # position's direction can't be recovered from the DB alone).
            # fee/mid_price let fee and slippage cost be reconstructed per
            # trade instead of only living in log lines.
            cursor.execute("PRAGMA table_info(positions)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            for col, col_type in (
                ("signal_type", "TEXT"),
                ("size", "INTEGER"),
                ("signal_features", "TEXT"),
                ("fee", "REAL"),
                ("mid_price", "REAL"),
                ("condition_id", "TEXT"),
            ):
                if col not in existing_cols:
                    cursor.execute(f"ALTER TABLE positions ADD COLUMN {col} {col_type}")
                    logger.info(f"[DB] Migrated positions table: added column {col}")

            # Trades table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    position_id TEXT,
                    entry_time REAL,
                    exit_time REAL,
                    entry_price REAL,
                    exit_price REAL,
                    size INTEGER,
                    pnl REAL,
                    pnl_percent REAL,
                    status TEXT,
                    duration_seconds REAL,
                    created_at TEXT,
                    fee REAL,
                    mid_price REAL,
                    condition_id TEXT,
                    official_resolution TEXT,
                    resettled INTEGER,
                    FOREIGN KEY (position_id) REFERENCES positions(id)
                )
            """)

            # Migration: same reasoning as the positions-table migration
            # above, for an existing trades table predating these columns.
            cursor.execute("PRAGMA table_info(trades)")
            existing_trade_cols = {row[1] for row in cursor.fetchall()}
            for col, col_type in (
                ("fee", "REAL"),
                ("mid_price", "REAL"),
                ("condition_id", "TEXT"),
                ("official_resolution", "TEXT"),
                ("resettled", "INTEGER"),
            ):
                if col not in existing_trade_cols:
                    cursor.execute(f"ALTER TABLE trades ADD COLUMN {col} {col_type}")
                    logger.info(f"[DB] Migrated trades table: added column {col}")

            # Market sessions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_sessions (
                    session_id TEXT PRIMARY KEY,
                    open_time REAL,
                    close_time REAL,
                    opening_price REAL,
                    closing_price REAL,
                    resolution TEXT,
                    total_trades INTEGER,
                    total_pnl REAL,
                    win_rate REAL,
                    created_at TEXT
                )
            """)

            # Migration: cycle-level fields added so every 5-minute cycle
            # (traded or not) can be persisted here, not just the ones that
            # produced a position — see MarketSession's docstring.
            cursor.execute("PRAGMA table_info(market_sessions)")
            existing_session_cols = {row[1] for row in cursor.fetchall()}
            for col, col_type in (
                ("cycle_number", "INTEGER"),
                ("signal_type", "TEXT"),
                ("trade_opened", "INTEGER"),
                ("position_id", "TEXT"),
                ("rejected_reason", "TEXT"),
                ("score", "REAL"),
                ("signal_features", "TEXT"),
                ("official_resolution", "TEXT"),
                ("resolution_match", "INTEGER"),
            ):
                if col not in existing_session_cols:
                    cursor.execute(f"ALTER TABLE market_sessions ADD COLUMN {col} {col_type}")
                    logger.info(f"[DB] Migrated market_sessions table: added column {col}")

            # Create indices
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_open ON market_sessions(open_time)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_cycle ON market_sessions(cycle_number)")
            
            conn.commit()
            logger.info("[DB] Schema initialized")
        
        except Exception as e:
            logger.error(f"[DB] Schema creation failed: {e}")
            raise
    
    # ========================================================================
    # ORDERS
    # ========================================================================
    
    def save_order(self, order: Order) -> bool:
        """Save order to database"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO orders
                (id, timestamp, side, price, size, status, tx_hash, filled_price, signal_type, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                order.id, order.timestamp, order.side, order.price, order.size,
                order.status, order.tx_hash, order.filled_price, order.signal_type, order.created_at
            ))
            
            conn.commit()
            logger.debug(f"[DB] Order saved: {order.id}")
            return True
        
        except Exception as e:
            logger.error(f"[DB] Save order failed: {e}")
            return False
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
            row = cursor.fetchone()
            
            if row:
                return Order(**dict(row))
            return None
        
        except Exception as e:
            logger.error(f"[DB] Get order failed: {e}")
            return None
    
    def get_all_orders(self, status: Optional[str] = None) -> List[Order]:
        """Get all orders, optionally filtered by status"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if status:
                cursor.execute("SELECT * FROM orders WHERE status = ? ORDER BY timestamp DESC", (status,))
            else:
                cursor.execute("SELECT * FROM orders ORDER BY timestamp DESC")
            
            rows = cursor.fetchall()
            return [Order(**dict(row)) for row in rows]
        
        except Exception as e:
            logger.error(f"[DB] Get all orders failed: {e}")
            return []
    
    def update_order_status(self, order_id: str, status: str, filled_price: Optional[float] = None) -> bool:
        """Update order status"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if filled_price:
                cursor.execute(
                    "UPDATE orders SET status = ?, filled_price = ? WHERE id = ?",
                    (status, filled_price, order_id)
                )
            else:
                cursor.execute("UPDATE orders SET status = ? WHERE id = ?", (status, order_id))
            
            conn.commit()
            logger.debug(f"[DB] Order status updated: {order_id} → {status}")
            return True
        
        except Exception as e:
            logger.error(f"[DB] Update order status failed: {e}")
            return False
    
    # ========================================================================
    # POSITIONS
    # ========================================================================
    
    def open_position(self, position: Position) -> bool:
        """Open new position"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO positions
                (id, order_id, entry_price, entry_time, exit_price, exit_time, pnl, pnl_percent,
                 status, created_at, signal_type, size, signal_features, fee, mid_price,
                 condition_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                position.id, position.order_id, position.entry_price, position.entry_time,
                position.exit_price, position.exit_time, position.pnl, position.pnl_percent,
                position.status, position.created_at, position.signal_type, position.size,
                position.signal_features, position.fee, position.mid_price,
                position.condition_id
            ))
            
            conn.commit()
            logger.info(f"[DB] Position opened: {position.id} @ {position.entry_price}")
            return True
        
        except Exception as e:
            logger.error(f"[DB] Open position failed: {e}")
            return False
    
    def close_position(self, position_id: str, exit_price: float, pnl: float, pnl_percent: float) -> bool:
        """Close position with exit price and PnL"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE positions
                SET status = ?, exit_price = ?, exit_time = ?, pnl = ?, pnl_percent = ?
                WHERE id = ?
            """, (PositionStatus.CLOSED.value, exit_price, datetime.now().timestamp(), pnl, pnl_percent, position_id))

            conn.commit()
            logger.info(f"[DB] Position closed: {position_id} PnL={pnl:.2f} ({pnl_percent:.2f}%)")
            return True

        except Exception as e:
            logger.error(f"[DB] Close position failed: {e}")
            return False

    def close_position_with_trade(self, position_id: str, exit_price: float, pnl: float,
                                   pnl_percent: float, trade: "Trade") -> bool:
        """
        Close a position AND save its completed trade row in one
        transaction — a single commit (or, on any failure, a rollback
        that leaves neither write applied) instead of two independent
        auto-committing calls. Without this, a failure between the two
        (lock contention, a full disk) could leave a position marked
        CLOSED in `positions` with no corresponding row in `trades`,
        which the dashboard and every trade statistic read — silently
        diverging from what PositionManager's in-memory stats believe
        happened.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE positions
                SET status = ?, exit_price = ?, exit_time = ?, pnl = ?, pnl_percent = ?
                WHERE id = ?
            """, (PositionStatus.CLOSED.value, exit_price, datetime.now().timestamp(), pnl, pnl_percent, position_id))

            cursor.execute("""
                INSERT OR REPLACE INTO trades
                (id, position_id, entry_time, exit_time, entry_price, exit_price, size, pnl, pnl_percent,
                 status, duration_seconds, created_at, fee, mid_price, condition_id,
                 official_resolution, resettled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.id, trade.position_id, trade.entry_time, trade.exit_time,
                trade.entry_price, trade.exit_price, trade.size, trade.pnl, trade.pnl_percent,
                trade.status, trade.duration_seconds, trade.created_at, trade.fee, trade.mid_price,
                trade.condition_id, trade.official_resolution, trade.resettled
            ))

            conn.commit()
            logger.info(f"[DB] Position closed + trade saved: {position_id} PnL={pnl:.2f} ({pnl_percent:.2f}%)")
            return True

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.error(f"[DB] Close position + save trade failed: {e}")
            return False
    
    def get_open_positions(self) -> List[Position]:
        """Get all open positions"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM positions WHERE status = ? ORDER BY entry_time DESC", (PositionStatus.OPEN.value,))
            rows = cursor.fetchall()
            return [Position(**dict(row)) for row in rows]
        
        except Exception as e:
            logger.error(f"[DB] Get open positions failed: {e}")
            return []
    
    # ========================================================================
    # TRADES
    # ========================================================================
    
    def save_trade(self, trade: Trade) -> bool:
        """Save completed trade"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO trades
                (id, position_id, entry_time, exit_time, entry_price, exit_price, size, pnl, pnl_percent,
                 status, duration_seconds, created_at, fee, mid_price, condition_id,
                 official_resolution, resettled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.id, trade.position_id, trade.entry_time, trade.exit_time,
                trade.entry_price, trade.exit_price, trade.size, trade.pnl,
                trade.pnl_percent, trade.status, trade.duration_seconds, trade.created_at,
                trade.fee, trade.mid_price, trade.condition_id, trade.official_resolution,
                trade.resettled
            ))
            
            conn.commit()
            logger.info(f"[DB] Trade saved: {trade.id} PnL={trade.pnl:.2f}")
            return True
        
        except Exception as e:
            logger.error(f"[DB] Save trade failed: {e}")
            return False
    
    def get_trades(self, status: Optional[str] = None) -> List[Trade]:
        """Get trades, optionally filtered by status"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if status:
                cursor.execute("SELECT * FROM trades WHERE status = ? ORDER BY exit_time DESC", (status,))
            else:
                cursor.execute("SELECT * FROM trades ORDER BY exit_time DESC")
            
            rows = cursor.fetchall()
            return [Trade(**dict(row)) for row in rows]

        except Exception as e:
            logger.error(f"[DB] Get trades failed: {e}")
            return []

    # ========================================================================
    # MARKET SESSIONS
    # ========================================================================
    
    def save_market_session(self, session: MarketSession) -> bool:
        """Save market session (one row per cycle, traded or not)"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT OR REPLACE INTO market_sessions
                (session_id, open_time, close_time, opening_price, closing_price,
                 resolution, total_trades, total_pnl, win_rate, cycle_number,
                 signal_type, trade_opened, position_id, rejected_reason, score,
                 signal_features, official_resolution, resolution_match, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session.session_id, session.open_time, session.close_time,
                session.opening_price, session.closing_price, session.resolution,
                session.total_trades, session.total_pnl, session.win_rate,
                session.cycle_number, session.signal_type, session.trade_opened,
                session.position_id, session.rejected_reason, session.score,
                session.signal_features, session.official_resolution,
                session.resolution_match, session.created_at
            ))

            conn.commit()
            logger.info(
                f"[DB] Cycle saved: {session.session_id} (#{session.cycle_number}) "
                f"signal={session.signal_type} traded={bool(session.trade_opened)} "
                f"resolution={session.resolution} PnL={session.total_pnl:.2f}"
            )
            return True

        except Exception as e:
            logger.error(f"[DB] Save session failed: {e}")
            return False

    def get_market_sessions(self, limit: int = 100) -> List[MarketSession]:
        """Get recent market sessions"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM market_sessions ORDER BY open_time DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
            return [MarketSession(**dict(row)) for row in rows]

        except Exception as e:
            logger.error(f"[DB] Get sessions failed: {e}")
            return []

    def get_last_cycle_number(self) -> int:
        """
        Highest cycle_number persisted so far, 0 if none yet. Read once at
        bot startup so the [CYCLE N] counter resumes across restarts
        instead of resetting to 1 every time the process is relaunched.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(cycle_number) FROM market_sessions")
            row = cursor.fetchone()
            return row[0] if row and row[0] is not None else 0
        except Exception as e:
            logger.error(f"[DB] Get last cycle number failed: {e}")
            return 0

    def update_cycle_resolution(self, session_id: str, official_resolution: str, match: bool) -> bool:
        """
        Fill in a cycle's official Polymarket resolution once it arrives —
        minutes after the cycle itself already closed, on ResolutionWatcher's
        background WS/REST threads rather than the main thread that owns
        self.conn. Opens its own short-lived connection instead of touching
        self.conn, since sqlite3 connections are thread-affine by default.
        """
        try:
            conn = sqlite3.connect(self.db_file, timeout=10)
            try:
                conn.execute(
                    "UPDATE market_sessions SET official_resolution = ?, resolution_match = ? WHERE session_id = ?",
                    (official_resolution, 1 if match else 0, session_id)
                )
                conn.commit()
            finally:
                conn.close()
            return True
        except Exception as e:
            logger.error(f"[DB] Update cycle resolution failed: {e}")
            return False
    
    def refresh_session_totals(self, condition_id: str) -> bool:
        """
        Recompute a cycle's market_sessions totals from its trades.

        Needed because the cycle row is written the moment the window
        closes, while a trade's settlement can still change afterwards:
        resettle_trade() rewrites P&L once Polymarket's official
        resolution disagrees with ours. Without this the cycle row keeps
        the numbers implied by the bot's own (wrong) call, contradicting
        the very trades it summarizes.

        market_sessions is keyed on condition_id for exactly this reason —
        see run.py's cycle_session_id.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) AS n,
                       COALESCE(SUM(pnl), 0) AS pnl,
                       SUM(CASE WHEN status = 'win' THEN 1 ELSE 0 END) AS wins
                FROM trades WHERE condition_id = ?
            """, (condition_id,))
            row = cursor.fetchone()
            total = row["n"] or 0
            win_rate = ((row["wins"] or 0) / total * 100) if total else 0.0

            cursor.execute("""
                UPDATE market_sessions
                SET total_trades = ?, total_pnl = ?, win_rate = ?
                WHERE session_id = ?
            """, (total, row["pnl"] or 0.0, win_rate, condition_id))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"[DB] Refresh session totals failed: {e}")
            return False

    def get_settled_trades_for_condition(self, condition_id: str) -> List[Dict[str, Any]]:
        """
        Every closed trade belonging to one 5-minute market, with the
        direction it bet (signal_type lives on the position, not the
        trade). Used to correct settlement once Polymarket's own official
        resolution arrives — see
        PositionManager.resettle_for_official_resolution().
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT t.id, t.position_id, t.entry_price, t.exit_price, t.size,
                       t.pnl, t.status, t.fee, t.resettled, p.signal_type
                FROM trades t
                LEFT JOIN positions p ON t.position_id = p.id
                WHERE t.condition_id = ?
            """, (condition_id,))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"[DB] Get trades for condition failed: {e}")
            return []

    def resettle_trade(self, trade_id: str, position_id: str, exit_price: float,
                       pnl: float, pnl_percent: float, status: str,
                       official_resolution: str) -> bool:
        """
        Rewrite one already-closed trade's settlement, and its position's,
        in a single transaction.

        Called when Polymarket's official resolution disagrees with the
        one this bot computed from its own BTC feed at window close. The
        bot has to settle immediately (the next cycle starts seconds
        later) while the official result lands 2-5 minutes afterwards, so
        settling optimistically and correcting is the only option that
        both closes positions on time and ends up with true numbers. The
        row keeps official_resolution and resettled=1 so a corrected trade
        stays visible as corrected rather than silently rewritten.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE trades
                SET exit_price = ?, pnl = ?, pnl_percent = ?, status = ?,
                    official_resolution = ?, resettled = 1
                WHERE id = ?
            """, (exit_price, pnl, pnl_percent, status, official_resolution, trade_id))
            cursor.execute("""
                UPDATE positions
                SET exit_price = ?, pnl = ?, pnl_percent = ?
                WHERE id = ?
            """, (exit_price, pnl, pnl_percent, position_id))
            conn.commit()
            logger.info(
                f"[DB] Trade re-settled on official resolution: {trade_id} "
                f"-> {status.upper()} PnL={pnl:.2f} (official={official_resolution})"
            )
            return True
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.error(f"[DB] Re-settle trade failed: {e}")
            return False

    def mark_trades_official_resolution(self, condition_id: str, official_resolution: str) -> bool:
        """
        Record the official resolution on a market's trades even when it
        AGREES with ours (no P&L change). Keeps official_resolution
        populated for every settled trade, so "agreed" and "never checked"
        stay distinguishable in the data.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE trades SET official_resolution = ? WHERE condition_id = ?",
                (official_resolution, condition_id)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"[DB] Mark official resolution failed: {e}")
            return False

    # ========================================================================
    # ANALYTICS
    # ========================================================================
    
    def get_stats(self) -> Dict[str, Any]:
        """Get overall statistics"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Total trades
            cursor.execute("SELECT COUNT(*) as count FROM trades")
            total_trades = cursor.fetchone()["count"]
            
            # Wins
            cursor.execute("SELECT COUNT(*) as count FROM trades WHERE status = 'win'")
            wins = cursor.fetchone()["count"]
            
            # Losses
            cursor.execute("SELECT COUNT(*) as count FROM trades WHERE status = 'loss'")
            losses = cursor.fetchone()["count"]
            
            # Total PnL
            cursor.execute("SELECT SUM(pnl) as total FROM trades")
            total_pnl = cursor.fetchone()["total"] or 0.0
            
            # Win rate
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0
            
            return {
                "total_trades": total_trades,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "total_pnl": total_pnl,
            }
        
        except Exception as e:
            logger.error(f"[DB] Get stats failed: {e}")
            return {}

# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    
    db = Database("test_bot.db")
    
    logger.info("="*80)
    logger.info("DATABASE MANAGER TEST")
    logger.info("="*80)
    
    # Save order
    order = Order(
        id="order_001",
        timestamp=datetime.now().timestamp(),
        side="BUY",
        price=0.65,
        size=10,
        status=OrderStatus.OPEN.value,
        signal_type="BUY_DOWN"
    )
    db.save_order(order)
    
    # Open position
    position = Position(
        id="pos_001",
        order_id="order_001",
        entry_price=0.65,
        entry_time=datetime.now().timestamp(),
        status=PositionStatus.OPEN.value
    )
    db.open_position(position)
    
    # Close position
    db.close_position("pos_001", exit_price=0.70, pnl=0.50, pnl_percent=7.7)
    
    # Save trade
    trade = Trade(
        id="trade_001",
        position_id="pos_001",
        entry_time=position.entry_time,
        exit_time=datetime.now().timestamp(),
        entry_price=0.65,
        exit_price=0.70,
        size=10,
        pnl=0.50,
        pnl_percent=7.7,
        status=TradeStatus.WIN.value
    )
    db.save_trade(trade)
    
    # Get stats
    stats = db.get_stats()
    print("\nStats:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    db.close()
