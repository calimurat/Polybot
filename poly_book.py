"""
POLYMARKET LIVE BOOK — Polymarket market-channel "book" snapshots, with the
"price_change" updates applied on top and each update checked against the
top of book Polymarket reports with it.

Shared by the bot (PolymarketBot) and the market tape recorder
(market_recorder.py) since 2026-10-05. Before that the recorder had its own
copy of these rules, so a fix to one could silently make the tape's prices
differ from the books the bot traded on. The rules are the bot's, unchanged
(see PolymarketBot._apply_price_changes for why they are safe).

State per token is two plain dicts the caller owns:
  levels[side] = {"bids": {price: (price_str, size_str)}, "asks": {...}}
      keyed on the price rounded to 6 places, so "0.5" and "0.50" are one level
  ok[side]     = True from a snapshot until a change cannot be followed or
                 its top of book disagrees; then False until the next snapshot
"""

from typing import Callable, Dict, List, Optional, Sequence, Tuple


def levels_from(entries) -> dict:
    """[{"price","size"}, ...] -> {price_float: (price_str, size_str)}."""
    levels = {}
    for e in entries or []:
        try:
            levels[round(float(e["price"]), 6)] = (str(e["price"]), str(e["size"]))
        except (KeyError, TypeError, ValueError):
            continue
    return levels


def top_matches(have: Optional[float], want, empty_value: float) -> bool:
    """Our top-of-book price vs Polymarket's. An empty side is accepted as
    matching "", "0" or empty_value (how an empty side may be reported)."""
    try:
        w = float(want) if want not in (None, "") else None
    except (TypeError, ValueError):
        return False
    if have is None:
        return w is None or abs(w - empty_value) < 1e-9 or w == 0.0
    return w is not None and abs(have - w) < 1e-9


def apply_price_changes(changes: Sequence, side_of: Callable[[object], Optional[str]],
                        levels: Dict[str, dict], ok: Dict[str, bool]):
    """
    Apply one "price_change" event's changes to `levels` and `ok`, in place.

    Each change is {asset_id, price, size, side, best_bid, best_ask}: `size`
    is the new total resting size at that price ("0" removes the level),
    side "BUY" is the bid side and "SELL" the ask side, and best_bid/best_ask
    are that token's top of book after the change. side_of(asset_id) names
    the book a change belongs to, or None for an asset not followed. Only a
    book with ok[side] is updated. After its changes, a book's best bid/ask
    must equal those reported with its last change in this event; if not, or
    if they are missing, ok[side] goes False until the next snapshot.

    Returns (verified, mismatches, counts): the sides whose rebuilt book
    matched (to publish), (side, have_bid, have_ask, want_bid, want_ask) for
    those that did not, and counts of applied / mismatched / unverifiable /
    ignored.
    """
    touched: Dict[str, dict] = {}
    counts = {"applied": 0, "mismatched": 0, "unverifiable": 0, "ignored": 0}
    for ch in changes or []:
        if not isinstance(ch, dict):
            continue
        side_key = side_of(ch.get("asset_id"))
        if side_key is None or not ok[side_key]:
            counts["ignored"] += 1
            continue
        side = str(ch.get("side", "")).upper()
        book_side = "bids" if side == "BUY" else "asks" if side == "SELL" else None
        try:
            price_s, size_s = str(ch["price"]), str(ch["size"])
            key, size = round(float(price_s), 6), float(size_s)
        except (KeyError, TypeError, ValueError):
            book_side = None
        if book_side is None:
            ok[side_key] = False  # can't follow this book any more
            counts["unverifiable"] += 1
            touched.pop(side_key, None)
            continue
        lv = levels[side_key][book_side]
        if size <= 0:
            lv.pop(key, None)
        else:
            lv[key] = (price_s, size_s)
        touched[side_key] = ch  # last change per book carries its top of book

    verified: List[str] = []
    mismatches: List[Tuple] = []
    for side_key, last in touched.items():
        if not ok[side_key]:
            continue
        want_bid, want_ask = last.get("best_bid"), last.get("best_ask")
        if want_bid is None or want_ask is None:
            ok[side_key] = False
            counts["unverifiable"] += 1
            continue
        lv = levels[side_key]
        have_bid = max(lv["bids"]) if lv["bids"] else None
        have_ask = min(lv["asks"]) if lv["asks"] else None
        if top_matches(have_bid, want_bid, empty_value=0.0) and \
                top_matches(have_ask, want_ask, empty_value=1.0):
            counts["applied"] += 1
            verified.append(side_key)
        else:
            ok[side_key] = False
            counts["mismatched"] += 1
            mismatches.append((side_key, have_bid, have_ask, want_bid, want_ask))
    return verified, mismatches, counts


# ---- readers (the recorder's tape columns) --------------------------------

def best_bid(book: dict) -> Optional[float]:
    return max(book["bids"]) if book["bids"] else None


def best_ask(book: dict) -> Optional[float]:
    return min(book["asks"]) if book["asks"] else None


def size_at(side_levels: dict, price: Optional[float]) -> Optional[float]:
    if price is None or price not in side_levels:
        return None
    try:
        return float(side_levels[price][1])
    except (TypeError, ValueError):
        return None


def vwap_buys(asks: dict, sizes: Sequence[float]) -> List[Optional[float]]:
    """Average price to buy each of `sizes` shares walking `asks` once (best
    first); None for a size the book cannot fill. Results in `sizes` order."""
    order = sorted(range(len(sizes)), key=lambda i: sizes[i])
    out: List[Optional[float]] = [None] * len(sizes)
    k, filled, cost = 0, 0.0, 0.0
    for price in sorted(asks):
        try:
            avail = float(asks[price][1])
        except (TypeError, ValueError):
            continue
        if avail <= 0:
            continue
        while k < len(order) and filled + avail >= sizes[order[k]] - 1e-9:
            size = sizes[order[k]]
            out[order[k]] = (cost + (size - filled) * price) / size
            k += 1
        filled += avail
        cost += avail * price
        if k == len(order):
            break
    return out
