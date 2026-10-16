"""
MARKET LOADER — Load Market Data Dynamically
Fetch market metadata, token IDs, opening prices from Gamma API

Production-grade: caching, error handling, validation, fallbacks
"""

import json
import logging
import requests
import time
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger("MarketLoader")

def _opt_float(value) -> Optional[float]:
    """float(value), or None for missing/empty/non-numeric (Gamma sends
    some numbers as strings, e.g. feeSchedule.rate)."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

# ============================================================================
# CONFIG
# ============================================================================

@dataclass
class MarketLoaderConfig:
    """Market loader configuration"""
    gamma_url: str = "https://gamma-api.polymarket.com"
    timeout: int = 10
    max_retries: int = 3
    retry_backoff: float = 1.5
    min_liquidity: float = 100.0  # Min $100 liquidity

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class MarketData:
    """Market data snapshot"""
    id: str
    question: str
    condition_id: str
    token_yes_id: str
    token_no_id: str
    opening_price: float
    outcome_prices: List[float]
    liquidity: float
    volume_24h: float
    created_at: str
    expires_at: str
    status: str  # active, resolved, voided
    # Informational only — NOT used as a hard gate. Verified live 2026-09-14
    # that a genuinely live, successfully-traded 5m BTC market had
    # acceptingOrders=None and enableOrderBook=False, so these two fields
    # don't reliably indicate whether THIS market type can actually be
    # traded right now. Logged for visibility in case that changes.
    accepting_orders: Optional[bool] = None
    enable_order_book: Optional[bool] = None
    # Trading parameters, read from the market itself (2026-09-29) instead
    # of assumed: run.py checks the fee schedule and minimum size against
    # what the bot trades with, and real order code needs all of them.
    tick_size: Optional[float] = None       # orderPriceMinTickSize
    min_order_size: Optional[float] = None  # orderMinSize, in shares
    neg_risk: Optional[bool] = None         # negRisk (different exchange contract)
    fees_enabled: Optional[bool] = None     # feesEnabled
    # feeSchedule: taker fee = shares * rate * (p * (1 - p)) ** exponent
    # (docs.polymarket.com/market-data/market-details#trading-fees)
    fee_rate: Optional[float] = None
    fee_exponent: Optional[float] = None
    fee_taker_only: Optional[bool] = None
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().timestamp()
    
    def is_active(self) -> bool:
        """Check if market is active"""
        return self.status.lower() == "active"
    
    def is_valid(self) -> bool:
        """Check if market data is valid"""
        return (
            self.id and
            self.condition_id and
            self.token_yes_id and
            self.token_no_id and
            self.opening_price > 0 and
            self.liquidity >= 0
        )

# ============================================================================
# MARKET LOADER
# ============================================================================

class MarketLoader:
    """
    Load market data from Polymarket Gamma API
    
    Features:
    - Load active markets
    - Find specific market (by name)
    - Cache results
    - Error handling + retries
    - Fallback handling
    """
    
    def __init__(self, config: MarketLoaderConfig = None):
        self.config = config or MarketLoaderConfig()
        
        # Stats
        self.stats = {
            "api_calls": 0,
            "errors": 0,
            "retries": 0
        }
        
        logger.info(f"[MARKET] Loader initialized: {self.config.gamma_url}")
    
    # ========================================================================
    # API CALLS
    # ========================================================================
    
    def _call_gamma_api(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        Call Gamma API with retries
        
        Args:
            endpoint: API endpoint (e.g., "/markets")
            params: Query parameters
        
        Returns: Response dict or None
        """
        
        url = f"{self.config.gamma_url}{endpoint}"
        params = params or {}
        # Per-attempt failures are logged at DEBUG and only the last one is
        # reported, once, below: during a Polymarket outage every call
        # used to add a WARNING per retry on top of its callers' own errors.
        last_error = None

        for attempt in range(self.config.max_retries):
            try:
                logger.debug(f"[MARKET] API call: {endpoint} (attempt {attempt + 1})")
                
                response = requests.get(
                    url,
                    params=params,
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                
                self.stats["api_calls"] += 1
                return response.json()
            
            except requests.Timeout:
                last_error = "timeout"
                logger.debug(f"[MARKET] Timeout on attempt {attempt + 1}")
                self.stats["retries"] += 1

                if attempt < self.config.max_retries - 1:
                    wait = self.config.retry_backoff ** attempt
                    time.sleep(wait)

            except requests.ConnectionError as e:
                last_error = e
                logger.debug(f"[MARKET] Connection error on attempt {attempt + 1}: {e}")
                self.stats["retries"] += 1
                
                if attempt < self.config.max_retries - 1:
                    wait = self.config.retry_backoff ** attempt
                    time.sleep(wait)
            
            except Exception as e:
                logger.error(f"[MARKET] API error: {e}")
                self.stats["errors"] += 1
                return None
        
        logger.warning(f"[MARKET] Max retries exceeded for {endpoint} (last error: {last_error})")
        self.stats["errors"] += 1
        return None
    
    # ========================================================================
    # MARKET LOADING
    # ========================================================================
    
    def load_live_5m_btc_market(self) -> Optional[MarketData]:
        """
        Load the currently-live 5-minute "Bitcoin Up or Down" market.

        IMPORTANT: The regular /markets?active=true endpoint does NOT
        return these rolling 5-minute markets (Polymarket appears to hide
        them from that listing). The reliable way is to query the
        /events endpoint with the series_slug for the BTC 5-minute
        series, then pick the event whose time window contains "now".

        Series slug: "btc-up-or-down-5m"
        Event/market slug pattern: "btc-updown-5m-<unix_window_start>"

        Returns: MarketData for the live window, or None
        """
        try:
            params = {
                "series_slug": "btc-up-or-down-5m",
                "closed": "false",
                "limit": 500,
                "order": "endDate",
                "ascending": "true",
            }

            response = self._call_gamma_api("/events", params)

            if not response:
                logger.error("[MARKET] Failed to load 5m BTC events")
                return None

            events = response if isinstance(response, list) else response.get("events", [])

            if not events:
                logger.error("[MARKET] No 5m BTC events returned")
                return None

            now_ts = datetime.now().timestamp()
            live_market_raw = None
            live_event_title = None

            for event in events:
                # Each event wraps one market for this series
                event_markets = event.get("markets", [])
                if not event_markets:
                    continue

                market_raw = event_markets[0]

                start_str = event.get("startDate") or market_raw.get("startDate")
                end_str = event.get("endDate") or market_raw.get("endDate")

                try:
                    start_ts = datetime.fromisoformat(start_str.replace("Z", "+00:00")).timestamp() if start_str else None
                    end_ts = datetime.fromisoformat(end_str.replace("Z", "+00:00")).timestamp() if end_str else None
                except (ValueError, AttributeError):
                    start_ts = None
                    end_ts = None

                # Live window = start <= now < end
                if start_ts is not None and end_ts is not None:
                    if start_ts <= now_ts < end_ts:
                        live_market_raw = market_raw
                        live_event_title = event.get("title", "")
                        break

            if live_market_raw is None:
                # Fallback: no window matched exactly (clock skew, gaps) —
                # take the first upcoming/ongoing one by endDate ascending
                for event in events:
                    event_markets = event.get("markets", [])
                    if event_markets:
                        end_str = event.get("endDate") or event_markets[0].get("endDate")
                        try:
                            end_ts = datetime.fromisoformat(end_str.replace("Z", "+00:00")).timestamp() if end_str else None
                        except (ValueError, AttributeError):
                            end_ts = None
                        if end_ts is not None and end_ts > now_ts:
                            live_market_raw = event_markets[0]
                            live_event_title = event.get("title", "")
                            break

            if live_market_raw is None:
                logger.error("[MARKET] Could not determine live 5m BTC market")
                return None

            market_data = self._parse_market(live_market_raw)

            if not market_data:
                logger.error("[MARKET] Failed to parse live 5m BTC market")
                return None

            logger.info(f"[MARKET] Live 5m BTC market: {live_event_title or market_data.question}")
            return market_data

        except Exception as e:
            logger.error(f"[MARKET] load_live_5m_btc_market failed: {e}")
            self.stats["errors"] += 1
            return None

    def load_next_5m_btc_market(self, current_market: MarketData) -> Optional[MarketData]:
        """
        Find the 5-minute BTC market whose window starts right when
        current_market's window ends — used to pre-warm the next cycle's
        WS connection before the current one actually closes, instead of
        discovering + connecting reactively after (see BotController's
        prewarm flow in run.py).

        Matches on endDate, NOT startDate: verified live 2026-09-14 that
        Gamma's "startDate" is when the market row was CREATED (batch-
        created well ahead of time, sometimes a day+ before its own title
        window), not the real trading-window start — so "startDate >=
        current end" almost never matches anything. "endDate" IS the real,
        accurate window-end time (matches what's in the title). Windows are
        back-to-back with no gaps, so the next window is simply the one
        with the smallest endDate that's still after the current one's.

        Same single /events call as load_live_5m_btc_market().
        """
        try:
            current_end_str = current_market.expires_at
            if not current_end_str:
                logger.warning("[MARKET] Current market has no expires_at, can't find next")
                return None

            current_end_ts = datetime.fromisoformat(current_end_str.replace("Z", "+00:00")).timestamp()

            params = {
                "series_slug": "btc-up-or-down-5m",
                "closed": "false",
                "limit": 500,
                "order": "endDate",
                "ascending": "true",
            }

            response = self._call_gamma_api("/events", params)
            if not response:
                logger.error("[MARKET] Failed to load 5m BTC events for next-market lookup")
                return None

            events = response if isinstance(response, list) else response.get("events", [])

            best_market_raw = None
            best_event_title = None
            best_end_ts = None

            for event in events:
                event_markets = event.get("markets", [])
                if not event_markets:
                    continue

                market_raw = event_markets[0]
                end_str = event.get("endDate") or market_raw.get("endDate")

                try:
                    end_ts = datetime.fromisoformat(end_str.replace("Z", "+00:00")).timestamp() if end_str else None
                except (ValueError, AttributeError):
                    end_ts = None

                if end_ts is None:
                    continue

                # The next window to end after the current one — keep the
                # smallest such end_ts seen (1s tolerance for rounding).
                if end_ts > current_end_ts + 1:
                    if best_end_ts is None or end_ts < best_end_ts:
                        best_end_ts = end_ts
                        best_market_raw = market_raw
                        best_event_title = event.get("title", "")

            if best_market_raw is None:
                logger.warning("[MARKET] Next 5m BTC market not found yet (not created by Gamma yet?)")
                return None

            market_data = self._parse_market(best_market_raw)
            if not market_data:
                logger.error("[MARKET] Failed to parse next 5m BTC market")
                return None

            logger.info(f"[MARKET] Next 5m BTC market: {best_event_title or market_data.question}")
            return market_data

        except Exception as e:
            logger.error(f"[MARKET] load_next_5m_btc_market failed: {e}")
            self.stats["errors"] += 1
            return None

    # ========================================================================
    # PARSING
    # ========================================================================
    
    def _parse_market(self, market: Dict[str, Any]) -> Optional[MarketData]:
        """
        Parse raw market data from API
        
        Args:
            market: Raw market dict from API
        
        Returns: MarketData or None
        """
        
        try:
            # Extract token IDs
            # NOTE: Gamma API returns clobTokenIds as a JSON-encoded STRING,
            # e.g. '["1234...", "5678..."]' — not a real list. Must parse it.
            clob_token_ids = market.get("clobTokenIds", [])
            if isinstance(clob_token_ids, str):
                try:
                    clob_token_ids = json.loads(clob_token_ids)
                except (json.JSONDecodeError, ValueError):
                    logger.warning("[MARKET] Failed to parse clobTokenIds string")
                    return None

            if not clob_token_ids or len(clob_token_ids) != 2:
                logger.warning("[MARKET] Expected exactly 2 token IDs")
                return None

            # Which token is which comes from "outcomes", listed in the
            # same order as clobTokenIds (verified live 2026-09-29:
            # '["Up", "Down"]'). The bot buys token_yes_id on BUY_UP and
            # token_no_id on BUY_DOWN, so a swapped pair would put every
            # trade on the wrong side — it used to assume index 0 = Up
            # without looking. Anything but exactly Up/Down is refused
            # rather than guessed at.
            outcomes = market.get("outcomes")
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except (json.JSONDecodeError, ValueError):
                    outcomes = None
            labels = ([str(o).strip().lower() for o in outcomes]
                      if isinstance(outcomes, list) else [])
            if sorted(labels) != ["down", "up"]:
                logger.error(
                    f"[MARKET] Refusing market {market.get('slug') or market.get('id')}: "
                    f"outcomes {outcomes!r} are not exactly Up/Down, so the Up and "
                    f"Down tokens can't be told apart"
                )
                return None
            up_idx, down_idx = labels.index("up"), labels.index("down")

            token_yes_id = str(clob_token_ids[up_idx])
            token_no_id = str(clob_token_ids[down_idx])
            
            # Extract prices
            # NOTE: Gamma API returns outcomePrices as a JSON-encoded STRING too,
            # e.g. '["0.65", "0.35"]' — must parse before indexing/float().
            # Same order as outcomes; stored as [Up, Down].
            outcome_prices = market.get("outcomePrices", [0.5, 0.5])
            if isinstance(outcome_prices, str):
                try:
                    outcome_prices = json.loads(outcome_prices)
                except (json.JSONDecodeError, ValueError):
                    outcome_prices = [0.5, 0.5]
            if isinstance(outcome_prices, list) and len(outcome_prices) == 2:
                outcome_prices = [outcome_prices[up_idx], outcome_prices[down_idx]]

            opening_price = float(outcome_prices[0]) if outcome_prices else 0.5

            # Trading parameters — see MarketData. Missing or malformed
            # values stay None; run.py treats an unknown fee schedule or
            # minimum size as a reason not to trade, never as a default.
            fee_schedule = market.get("feeSchedule")
            if isinstance(fee_schedule, str):
                try:
                    fee_schedule = json.loads(fee_schedule)
                except (json.JSONDecodeError, ValueError):
                    fee_schedule = None
            if not isinstance(fee_schedule, dict):
                fee_schedule = {}
            fees_enabled = market.get("feesEnabled")
            neg_risk = market.get("negRisk")
            taker_only = fee_schedule.get("takerOnly")
            
            # Extract liquidity
            # NOTE: the real Gamma API field is "volume24hr" (with the r) —
            # verified live 2026-09-14 against docs.polymarket.com/market-data.
            # "volume24h" (no r), "expiresAt", and "status" below are NOT real
            # fields either; the API has no "status" field at all, only the
            # active/closed/archived booleans.
            liquidity = market.get("liquidity", market.get("volume24hr", 0))
            if isinstance(liquidity, dict):
                liquidity = liquidity.get("total", 0)
            try:
                liquidity = float(liquidity)
            except (ValueError, TypeError):
                liquidity = 0.0

            # Reconstruct a status string from the real state booleans
            # instead of a "status" key that never exists — the old
            # market.get("status", "active") silently always fell through
            # to the "active" default, making MarketData.is_active() a
            # no-op that could never actually detect a closed/archived market.
            is_really_active = bool(
                market.get("active") and not market.get("closed") and not market.get("archived")
            )

            # Create market data
            market_data = MarketData(
                id=market.get("id", ""),
                question=market.get("question", ""),
                condition_id=market.get("conditionId", ""),
                token_yes_id=token_yes_id,
                token_no_id=token_no_id,
                opening_price=opening_price,
                outcome_prices=outcome_prices,
                liquidity=liquidity,
                volume_24h=float(market.get("volume24hr", 0)),
                created_at=market.get("createdAt", ""),
                expires_at=market.get("endDate", ""),
                status="active" if is_really_active else "closed",
                accepting_orders=market.get("acceptingOrders"),
                enable_order_book=market.get("enableOrderBook"),
                tick_size=_opt_float(market.get("orderPriceMinTickSize")),
                min_order_size=_opt_float(market.get("orderMinSize")),
                neg_risk=neg_risk if isinstance(neg_risk, bool) else None,
                fees_enabled=fees_enabled if isinstance(fees_enabled, bool) else None,
                fee_rate=_opt_float(fee_schedule.get("rate")),
                fee_exponent=_opt_float(fee_schedule.get("exponent")),
                fee_taker_only=taker_only if isinstance(taker_only, bool) else None,
            )
            
            return market_data
        
        except Exception as e:
            logger.error(f"[MARKET] Parse failed: {e}")
            return None
    
    # ========================================================================
    # UTILITIES
    # ========================================================================
    
    def is_sufficient_liquidity(self, market: MarketData) -> bool:
        """Check if market has sufficient liquidity"""
        return market.liquidity >= self.config.min_liquidity
    
    # ========================================================================
    # STATISTICS
    # ========================================================================
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics"""
        return {
            "api_calls": self.stats["api_calls"],
            "errors": self.stats["errors"],
            "retries": self.stats["retries"],
        }
    
    def export_stats(self) -> str:
        """Export stats as formatted string"""
        stats = self.get_stats()
        
        lines = [
            "="*60,
            "MARKET LOADER STATISTICS",
            "="*60,
            f"API calls:           {stats['api_calls']}",
            f"Retries:             {stats['retries']}",
            f"Errors:              {stats['errors']}",
            "="*60
        ]
        
        return "\n".join(lines)
