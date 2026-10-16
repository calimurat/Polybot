"""
PAPER REPORT — is the paper edge real, or still indistinguishable from luck?

Reads bot_trades.db read-only and prints, in Turkish, what the paper trades
say so far: win rate against the win rate the entry prices needed, mean P&L
with a bootstrap 95% confidence interval, whether the model's probabilities
matched reality, and how many trades it would take to know.

Only trades recorded with the feed-lag guard count by default (their
signal_features carry feed_behind_ms, 2026-09-28 onward). Earlier trades
may have been priced off books seconds behind the exchange (see
feed_lag.py), so they say little about real fills; --all reports them in a
separate section, never mixed in.

Usage:
    python paper_report.py
    python paper_report.py --all
    python paper_report.py --db archive/2026-09-28_duzeltmeler_oncesi/bot_trades.db --all
"""

import argparse
import json
import math
import os
import random
import sqlite3
from datetime import datetime
from typing import List, Optional

try:
    # The gates the bot itself trades at (edge, $/share) — one source of truth.
    from signal_engine import MultiFactorConfig
    REAL_EDGE_GATE = {"UP": MultiFactorConfig().min_edge_up, "DOWN": MultiFactorConfig().min_edge}
except Exception:  # report run outside the project directory
    REAL_EDGE_GATE = {"UP": 0.15, "DOWN": 0.20}

# Below this many settled trades the report says so instead of a verdict:
# a confidence interval from a handful of binary outcomes is mostly noise.
MIN_TRADES_FOR_VERDICT = 30
BOOTSTRAP_RESAMPLES = 10000


def wilson_interval(wins: int, n: int, z: float = 1.96):
    """95% Wilson score interval for a win rate — well-behaved at small n
    and near 0/1, unlike the plain normal approximation."""
    if n == 0:
        return None, None
    p = wins / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return centre - half, centre + half


def bootstrap_mean_ci(values: List[float], resamples: int = BOOTSTRAP_RESAMPLES):
    """95% percentile-bootstrap interval for the mean. Fixed seed, so the
    same data always prints the same interval."""
    if len(values) < 2:
        return None, None
    rng = random.Random(0)
    n = len(values)
    means = sorted(sum(rng.choice(values) for _ in range(n)) / n for _ in range(resamples))
    return means[int(0.025 * resamples)], means[int(0.975 * resamples) - 1]


def load_trades(db_path: str):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT t.status, t.pnl, t.entry_price, t.size, t.fee, t.entry_time,
               p.signal_type, p.signal_features
        FROM trades t JOIN positions p ON p.id = t.position_id
        WHERE t.pnl IS NOT NULL
        ORDER BY t.entry_time
    """).fetchall()
    sessions = conn.execute("""
        SELECT COUNT(*) AS cycles,
               SUM(CASE WHEN trade_opened THEN 1 ELSE 0 END) AS traded,
               SUM(CASE WHEN rejected_reason LIKE '%behind Polymarket%'
                         OR rejected_reason LIKE '%execution: BTC price%' THEN 1 ELSE 0 END) AS lag_blocked,
               SUM(CASE WHEN rejected_reason LIKE '%market not tradable%' THEN 1 ELSE 0 END) AS market_blocked
        FROM market_sessions
    """).fetchone()
    conn.close()

    trades = []
    for r in rows:
        try:
            features = json.loads(r["signal_features"]) if r["signal_features"] else {}
        except (TypeError, ValueError):
            features = {}
        trades.append({
            "status": (r["status"] or "").lower(),
            "pnl": r["pnl"],
            "entry_price": r["entry_price"],
            "size": r["size"] or 0,
            "fee": r["fee"] or 0.0,
            "entry_time": r["entry_time"],
            "signal_type": r["signal_type"],
            "model_prob": features.get("model_prob"),
            "edge": features.get("edge"),
            "model_prob_short_vol": features.get("model_prob_short_vol"),
            "feed_behind_ms": features.get("feed_behind_ms"),
            "btc_behind_ms": features.get("btc_behind_ms"),
            "slippage": features.get("slippage_vs_best_ask"),
            "guarded": features.get("feed_behind_ms") is not None,
        })
    return trades, dict(sessions)


def fmt_pct(x: Optional[float]) -> str:
    return "—" if x is None else f"%{x * 100:.1f}"


def summarize(trades: list, title: str) -> None:
    settled = [t for t in trades if t["status"] in ("win", "loss")]
    voids = len(trades) - len(settled)
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")
    if not settled:
        print("Henüz sonuçlanmış işlem yok." + (f" ({voids} VOID)" if voids else ""))
        return

    n = len(settled)
    wins = sum(1 for t in settled if t["status"] == "win")
    pnls = [t["pnl"] for t in settled]
    total = sum(pnls)
    mean = total / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in pnls) / (n - 1)) if n > 1 else 0.0
    capital = sum(t["entry_price"] * t["size"] + t["fee"] for t in settled)
    # A share pays $1 on a win and $0 on a loss, so a trade breaks even at a
    # win rate equal to what one share cost, fee included.
    breakeven = sum(t["entry_price"] + (t["fee"] / t["size"] if t["size"] else 0) for t in settled) / n
    wr_lo, wr_hi = wilson_interval(wins, n)
    ci_lo, ci_hi = bootstrap_mean_ci(pnls)
    first = datetime.fromtimestamp(settled[0]["entry_time"]).strftime("%Y-%m-%d %H:%M")
    last = datetime.fromtimestamp(settled[-1]["entry_time"]).strftime("%Y-%m-%d %H:%M")

    print(f"Dönem:              {first} → {last}")
    print(f"İşlem:              {n} sonuçlanmış ({wins} kazanç, {n - wins} kayıp)"
          + (f", {voids} VOID hariç" if voids else ""))
    print(f"Kazanma oranı:      {fmt_pct(wins / n)}  (%95 aralık {fmt_pct(wr_lo)} – {fmt_pct(wr_hi)})")
    print(f"Başabaş oranı:      {fmt_pct(breakeven)}  (giriş fiyatı + ücretin gerektirdiği)")
    print(f"Toplam P&L:         ${total:+.2f}   (yatırılan ${capital:.2f}, ROI {fmt_pct(total / capital if capital else None)})")
    print(f"İşlem başı P&L:     ${mean:+.2f}  (std ${sd:.2f})")
    if ci_lo is not None and n >= MIN_TRADES_FOR_VERDICT:
        print(f"  %95 güven aralığı: ${ci_lo:+.2f} … ${ci_hi:+.2f}  (bootstrap)")
    elif n > 1:
        print(f"  %95 güven aralığı: n<{MIN_TRADES_FOR_VERDICT} olduğu için gösterilmiyor "
              f"(az örnekte, özellikle hepsi kazanmışsa, aralık yanıltıcı biçimde dar çıkar)")

    slips = [t for t in settled if t.get("slippage") is not None]
    if slips:
        cost_total = sum(t["slippage"] * t["size"] for t in slips)
        print(f"Slippage:           ort. {sum(t['slippage'] for t in slips) / len(slips) * 100:+.2f} cent/hisse, "
              f"en yüksek {max(t['slippage'] for t in slips) * 100:+.2f} (dolum − en iyi satış fiyatı; tavan yok), "
              f"toplam ${cost_total:.2f}; P&L bunun düşülmüş hali")
    probs = [t["model_prob"] for t in settled if t["model_prob"] is not None]
    if probs:
        print(f"Model kalibrasyonu: model ort. {fmt_pct(sum(probs) / len(probs))} dedi, "
              f"gerçekleşen {fmt_pct(wins / n)}")

    print("\nYöne göre:")
    for side in ("BUY_UP", "BUY_DOWN"):
        s = [t for t in settled if t["signal_type"] == side]
        if s:
            w = sum(1 for t in s if t["status"] == "win")
            print(f"  {side:9s} n={len(s):4d}  kazanma {fmt_pct(w / len(s))}  P&L ${sum(t['pnl'] for t in s):+.2f}")

    lagged = [t for t in settled if t["feed_behind_ms"] is not None]
    if lagged:
        print("\nPolymarket gecikmesine göre (sinyal anı):")
        for lo, hi in ((0, 100), (100, 300), (300, float("inf"))):
            s = [t for t in lagged if lo <= t["feed_behind_ms"] < hi]
            if s:
                w = sum(1 for t in s if t["status"] == "win")
                label = f"{lo}-{hi} ms" if hi != float("inf") else f"{lo}+ ms"
                print(f"  {label:11s} n={len(s):4d}  kazanma {fmt_pct(w / len(s))}  P&L ${sum(t['pnl'] for t in s):+.2f}")

    print("\nKarar:")
    if n < MIN_TRADES_FOR_VERDICT:
        print(f"  Karar için çok az veri ({n} işlem; en az {MIN_TRADES_FOR_VERDICT} gerekli, "
              f"güvenilir bir sonuç için birkaç yüz).")
    elif ci_lo is not None and ci_lo > 0:
        print("  Pozitif edge: işlem başı P&L'in %95 aralığı sıfırın üstünde.")
        if probs and sum(probs) / len(probs) - wins / n > 0.05:
            print("  Uyarı: model gerçekleşenden belirgin şekilde iyimser; edge beklenenden küçük olabilir.")
    elif ci_hi is not None and ci_hi < 0:
        print("  Negatif edge: işlem başı P&L'in %95 aralığı sıfırın altında.")
    else:
        print("  Belirsiz: sonuç henüz şanstan ayırt edilemiyor.")
    if mean > 0 and sd > 0 and (ci_lo is None or ci_lo <= 0):
        needed = math.ceil((1.96 * sd / mean) ** 2)
        print(f"  Bu ortalama ve dağılım sürerse anlamlılık için ~{needed} işlem gerekir "
              f"(şu an {n}).")


def load_shadow(db_path: str):
    """shadow_trades rows (shadow.py) as dicts, each with its window's
    outcome, or None if this database has no shadow table. The outcome is
    Polymarket's official resolution when known, else the bot's own for
    that window."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='shadow_trades'").fetchone():
            return None
        # fill_model / slippage exist only once the bot has run with the
        # taker fill model; rows without them are from the earlier
        # 500 ms + 1-tick-limit model.
        cols = {r[1] for r in conn.execute("PRAGMA table_info(shadow_trades)")}
        model = "COALESCE(s.fill_model, 'latency500')" if "fill_model" in cols else "'latency500'"
        slip = "s.slippage" if "slippage" in cols else "NULL"
        return [dict(r) for r in conn.execute(f"""
            SELECT s.ts, s.condition_id, s.edge_lo, s.edge_hi, s.edge, s.direction, s.filled,
                   s.fill_price, s.size, s.fee, s.fail_reason,
                   {model} AS fill_model, {slip} AS slippage,
                   COALESCE(m.official_resolution, m.resolution) AS outcome
            FROM shadow_trades s LEFT JOIN market_sessions m ON m.session_id = s.condition_id
            ORDER BY s.ts
        """).fetchall()]
    finally:
        conn.close()


def load_perp_shadow(db_path: str):
    """perp_shadow rows (perp_shadow.py) with each window's outcome, or None
    if this database has no such table yet."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='perp_shadow'").fetchone():
            return None
        return [dict(r) for r in conn.execute("""
            SELECT p.*, COALESCE(m.official_resolution, m.resolution) AS outcome,
                   m.opening_price AS opening_price
            FROM perp_shadow p LEFT JOIN market_sessions m ON m.session_id = p.condition_id
            ORDER BY p.ts
        """).fetchall()]
    finally:
        conn.close()


def summarize_perp_shadow(rows) -> None:
    print(f"\n{'=' * 72}\nPERP MOMENTUM GÖLGESİ — pencere başına ilk sinyal, anlık taker dolum (yalnızca kayıt)\n{'=' * 72}")
    variants = sorted({(r["window_s"], r["threshold_usd"]) for r in rows})
    for w, t in variants:
        _summarize_perp_variant([r for r in rows if (r["window_s"], r["threshold_usd"]) == (w, t)],
                                f"{w:g} sn ±${t:g}")


def _summarize_perp_variant(rows, label: str) -> None:
    settled = [r for r in rows if r["outcome"] in ("UP", "DOWN")]
    print(f"\n--- {label}: {len(rows)} sinyal, sonucu belli {len(settled)}")
    if not settled:
        return

    def pnl(r):
        return r["size"] * ((1.0 if r["direction"] == r["outcome"] else 0.0) - r["fill_price"]) - (r["fee"] or 0.0)

    groups = [("Tümü", settled),
              (f"Temiz book (Polymarket ≤{CAL_MAX_FEED_BEHIND_S * 1000:.0f} ms geride)",
               [r for r in settled if r["feed_behind_ms"] is not None
                and r["feed_behind_ms"] <= CAL_MAX_FEED_BEHIND_S * 1000])]
    for title, g in groups:
        if not g:
            continue
        wins = sum(r["direction"] == r["outcome"] for r in g)
        lo, hi = wilson_interval(wins, len(g))
        print(f"  {title}: n={len(g)}  kazanma {fmt_pct(wins / len(g))} (%95 {fmt_pct(lo)}–{fmt_pct(hi)})  "
              f"P&L ${sum(map(pnl, g)):+.2f}  işlem başı ${sum(map(pnl, g)) / len(g):+.2f}")
        for side in ("UP", "DOWN"):
            s = [r for r in g if r["direction"] == side]
            if s:
                w = sum(r["direction"] == r["outcome"] for r in s)
                print(f"     {side:4s}: n={len(s)}  kazanma {fmt_pct(w / len(s))}  P&L ${sum(map(pnl, s)):+.2f}  "
                      f"ort. fiyat {sum(r['fill_price'] for r in s) / len(s):.3f}")
    for col, label in (("net_lag_ms", "Binance işlemi → bize ulaşma"), ("decide_ms", "ulaşma → karar"),
                       ("feed_behind_ms", "Polymarket book'unun gerisi")):
        v = sorted(r[col] for r in settled if r[col] is not None)
        if v:
            print(f"  Gecikme, {label}: medyan {v[len(v) // 2]:.1f} ms, %90 {v[int(0.9 * (len(v) - 1))]:.1f} ms")


def _verdict(n: int, lo: Optional[float], hi: Optional[float]) -> str:
    if n < MIN_TRADES_FOR_VERDICT:
        return "az veri"
    if lo is not None and lo > 0:
        return "POZİTİF"
    if hi is not None and hi < 0:
        return "negatif"
    return "belirsiz"


def _shadow_won(r: dict) -> bool:
    return r["direction"] == r["outcome"]


def _shadow_pnl(r: dict) -> float:
    """P&L of one filled shadow trade, fee included (a share pays $1 on a
    win and $0 on a loss)."""
    p = r["fill_price"]
    return r["size"] * ((1 - p) if _shadow_won(r) else -p) - (r["fee"] or 0)


def _shadow_cost(r: dict) -> float:
    return r["fill_price"] * r["size"] + (r["fee"] or 0)


def _gate_line(gate: float) -> str:
    return f"{'─' * 8} gerçek işlem eşiği: edge ≥ {gate:.2f} (bundan sonrası botun kendi işlem bölgesi) {'─' * 8}"


def _shadow_direction(rows, direction: str) -> None:
    """Both tables for one direction (UP and DOWN are measured separately:
    the bot's own gates differ and a window can flip direction)."""
    gate = REAL_EDGE_GATE[direction]
    off = gate >= 1.0        # an edge gate nothing can meet: this direction does not trade for real
    rows = [r for r in rows if r["direction"] == direction]
    title = (f"{direction} yönü — gerçek işlemde KAPALI (yalnızca gölgede ölçülüyor)" if off
             else f"{direction} yönü — botun gerçek işlem eşiği: edge ≥ {gate:.2f}")
    print(f"\n{'─' * 72}\n{title}\n{'─' * 72}")
    if not rows:
        print("Henüz kayıt yok.")
        return
    filled_all = [r for r in rows if r["filled"]]

    # ---- table 1: each 1-cent band on its own ------------------------------
    print(f"{'Edge aralığı':<14}{'deneme':>7}{'dolan':>7}{'sonuçlanan':>12}{'kazanma':>9}{'başabaş':>9}"
          f"{'işlem başı':>12}{'toplam P&L':>12}   {'%95 aralık':<20}karar")
    gate_shown = False
    for band in sorted({(r["edge_lo"], r["edge_hi"]) for r in rows}):
        if not off and not gate_shown and band[0] >= gate - 1e-9:
            print(_gate_line(gate))
            gate_shown = True
        rs = [r for r in rows if (r["edge_lo"], r["edge_hi"]) == band]
        filled = [r for r in rs if r["filled"]]
        settled = [r for r in filled if r["outcome"] in ("UP", "DOWN")]
        label = f"{band[0]:.2f}–{band[1]:.2f}"
        n = len(settled)
        if not n:
            print(f"{label:<14}{len(rs):>7d}{len(filled):>7d}{0:>12d}{'—':>9}{'—':>9}{'—':>12}{'—':>12}")
            continue
        pnls = [_shadow_pnl(r) for r in settled]
        wins = sum(_shadow_won(r) for r in settled)
        breakeven = sum(r["fill_price"] + (r["fee"] or 0) / r["size"] for r in settled) / n
        lo, hi = bootstrap_mean_ci(pnls) if n >= MIN_TRADES_FOR_VERDICT else (None, None)
        ci = "—" if lo is None else f"${lo:+.2f} … ${hi:+.2f}"
        print(f"{label:<14}{len(rs):>7d}{len(filled):>7d}{n:>12d}{fmt_pct(wins / n):>9}{fmt_pct(breakeven):>9}"
              f"{'$' + format(sum(pnls) / n, '+.2f'):>12}{'$' + format(sum(pnls), '+.2f'):>12}   {ci:<20}"
              f"{_verdict(n, lo, hi)}")
    if not gate_shown and not off:
        print(f"(edge ≥ {gate:.2f} için henüz kayıt yok — botun kendi işlem bölgesi.)")
    print(f"(deneme: sinyal geldi; dolan: simüle emir doldu; sonuçlanan: pencerenin sonucu belli. "
          f"%95 aralık, n<{MIN_TRADES_FOR_VERDICT} iken gösterilmez.)")

    # ---- table 2: one trade per window, i.e. what ONE chosen threshold does --
    settled_all = [r for r in filled_all if r["outcome"] in ("UP", "DOWN")]
    print(f"\n{direction}: PENCERE BAŞINA EN FAZLA 1 İŞLEM (o pencerede edge ≥ eşik olan ilk dolum) — tek eşik seçilseydi:")
    print(f"{'Edge ≥':<8}{'pencere':>8}{'kazanan':>9}{'yatırılan':>12}{'toplam P&L':>13}{'işlem başı':>12}"
          f"   {'%95 aralık':<20}karar")
    gate_shown = False
    for threshold in sorted({r["edge_lo"] for r in rows}):
        first = {}
        for r in settled_all:  # ordered by time
            if r["edge"] >= threshold and r["condition_id"] not in first:
                first[r["condition_id"]] = r
        chosen = list(first.values())
        n = len(chosen)
        if not n:
            continue
        if not off and not gate_shown and threshold >= gate - 1e-9:
            print(_gate_line(gate))
            gate_shown = True
        pnls = [_shadow_pnl(r) for r in chosen]
        lo, hi = bootstrap_mean_ci(pnls) if n >= MIN_TRADES_FOR_VERDICT else (None, None)
        ci = "—" if lo is None else f"${lo:+.2f} … ${hi:+.2f}"
        print(f"{threshold:<8.2f}{n:>8d}{sum(_shadow_won(r) for r in chosen):>9d}"
              f"{'$' + format(sum(_shadow_cost(r) for r in chosen), '.2f'):>12}"
              f"{'$' + format(sum(pnls), '+.2f'):>13}{'$' + format(sum(pnls) / n, '+.2f'):>12}   {ci:<20}"
              f"{_verdict(n, lo, hi)}")


def summarize_shadow(rows, title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")
    if not rows:
        print("Henüz bu dolum modeliyle gölge işlem yok.")
        return

    windows = {r["condition_id"] for r in rows}
    filled_all = [r for r in rows if r["filled"]]
    by_dir = {d: sum(1 for r in rows if r["direction"] == d) for d in ("UP", "DOWN")}
    print(f"Kayıt: {len(rows)} deneme (UP {by_dir['UP']}, DOWN {by_dir['DOWN']}), {len(windows)} farklı pencere "
          f"(dolum olan: {len({r['condition_id'] for r in filled_all})}); "
          f"dolum oranı {fmt_pct(len(filled_all) / len(rows))}.")
    slips = [r["slippage"] for r in filled_all if r.get("slippage") is not None]
    if slips:
        print(f"Slippage: ort. {sum(slips) / len(slips) * 100:+.2f} cent/hisse, en yüksek {max(slips) * 100:+.2f} "
              f"(dolum − en iyi satış fiyatı; tavan yok); P&L bunun düşülmüş hali.")
    print("UP ve DOWN ayrı ölçülür (bir pencerede yön değişebilir; aynı bandı iki yön ayrı ayrı alabilir).\n"
          "DİKKAT: bir pencerede edge yükseldikçe aynı sonuç birkaç aralığa birden yazılır. Aralık satırlarının\n"
          "P&L'lerini TOPLAMAYIN; toplam için her yönün ikinci tablosuna bakın.")
    for direction in ("UP", "DOWN"):
        _shadow_direction(rows, direction)

    # ---- what the simulated fill left out (earlier fill model only) ----------
    unfilled = [r for r in rows if not r["filled"] and r["outcome"] in ("UP", "DOWN")]
    if unfilled:
        won = sum(_shadow_won(r) for r in unfilled)
        ran = sum(1 for r in unfilled if "moved past" in (r["fail_reason"] or ""))
        lag = sum(1 for r in unfilled if "behind" in (r["fail_reason"] or ""))
        print(f"\nDolmayan {len(unfilled)} denemenin {won}'i ({fmt_pct(won / len(unfilled))}) dolsaydı kazanacaktı "
              f"— bunlar P&L'e dahil DEĞİL. Sebep: {ran} kez fiyat 500 ms içinde limitin ötesine kaçtı, "
              f"{lag} kez dolum anında veri gecikmeliydi (bilinemez).")


# ---------------------------------------------------------------------------
# MODEL CALIBRATION — does the probability the bot computes match what happens?
# ---------------------------------------------------------------------------
# P&L needs hundreds of trades before it says anything (a trade's std is ~$11
# against a mean of a few dollars). Calibration needs far fewer: every
# window ends UP or DOWN, so it can also be scored on the windows the bot
# did NOT trade. Two views:
#   1. the trades it took (selected moments — only the model_prob bins),
#   2. every clean window, at fixed seconds-left, rebuilt from the recorded
#      tape (book_ticks + BTC prices + the official result).
# Both also score the model against the market's own price (Brier score, lower
# is better): if the model does not beat the price, its "edge" is not edge.
CAL_BINS = ((0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0001))
CAL_MIN_BIN = 15          # fewer observations than this and a bin is an anecdote
CAL_TARGET_SECONDS = (210, 150, 100)
CAL_MAX_FEED_BEHIND_S = 0.5


PRICE_BINS = ((0.0, 0.5),) + CAL_BINS
BRIER_TIE = 0.003         # Brier scores closer than this are a tie, not a verdict


def _calibration_block(pairs, indent: str = "  ", names=("model", "piyasa fiyatı"), bins=CAL_BINS) -> None:
    """pairs: (forecast A, forecast B, won) for the side the model favoured. The bins
    are on A (reliability: does what A says happen as often as A says?); the Brier
    comparison scores A against B. Default A = the model, B = the market price."""
    n = len(pairs)
    if n == 0:
        print(f"{indent}veri yok")
        return
    first, second = names
    brier_model = sum((p - w) ** 2 for p, _, w in pairs) / n
    brier_price = sum((q - w) ** 2 for _, q, w in pairs) / n
    gap_sum = 0.0
    for lo, hi in bins:
        b = [(p, q, w) for p, q, w in pairs if lo <= p < hi]
        if not b:
            continue
        k = sum(w for _, _, w in b)
        mean_p = sum(p for p, _, _ in b) / len(b)
        rate = k / len(b)
        lo_ci, hi_ci = wilson_interval(int(k), len(b))
        gap_sum += len(b) * abs(rate - mean_p)
        if len(b) < CAL_MIN_BIN:
            tag = "az veri"
        elif mean_p > hi_ci:
            tag = f"{first} İYİMSER"
        elif mean_p < lo_ci:
            tag = f"{first} temkinli"
        else:
            tag = "uyumlu"
        print(f"{indent}{first} {lo:.1f}-{min(hi, 1.0):.1f}  n={len(b):4d}  ort. {fmt_pct(mean_p)}  gerçek {fmt_pct(rate)} "
              f"(%95 {fmt_pct(lo_ci)}–{fmt_pct(hi_ci)})  fark {100 * (rate - mean_p):+5.1f} puan  [{tag}]")
    if abs(brier_model - brier_price) < BRIER_TIE:
        verdict = "berabere"
    elif brier_model < brier_price:
        verdict = f"{first} daha iyi"
    else:
        verdict = f"{second} daha iyi"
    print(f"{indent}ortalama kalibrasyon hatası {100 * gap_sum / n:.1f} puan | Brier (düşük iyi): {first} {brier_model:.3f}, "
          f"{second} {brier_price:.3f} → {verdict}")


def _short_vol_line(rows, indent: str) -> None:
    """rows: (bot model, short-vol model, market price, won). One line: the three
    Brier scores on the same observations (lower is better)."""
    n = len(rows)
    if n == 0:
        print(f"{indent}kısa-vol modeli: kayıt yok")
        return
    b_bot, b_short, b_mkt = (sum((r[i] - r[3]) ** 2 for r in rows) / n for i in range(3))
    if abs(b_short - b_bot) < BRIER_TIE:
        verdict = "bot modeliyle berabere"
    elif b_short < b_bot:
        verdict = "bot modelinden iyi"
    else:
        verdict = "bot modelinden kötü"
    print(f"{indent}kısa-vol (son 5 dk) modeli, n={n}: Brier bot {b_bot:.3f} → kısa-vol {b_short:.3f} "
          f"({verdict}); piyasa fiyatı {b_mkt:.3f}")


BOOK_TICK_COLUMNS = ["ts", "window_end", "token", "fill_price", "book_age", "btc", "feed_behind"]


_TAPE_SOURCE = ["SQLite"]   # where the last _tape_pairs() read book_ticks from, for the report header


def _book_ticks_rows(db_path: str):
    """book_ticks rows (BOOK_TICK_COLUMNS, btc known) in time order, and where
    they came from. SQLite keeps only Config.sqlite_retention_days of them
    since 2026-10-05, so for the bot's own database the full history comes
    from the Parquet copy (parquet_sync.py), plus the rows SQLite has that
    were not exported yet. Any other database is read as it is."""
    here = os.path.dirname(os.path.abspath(__file__))
    rows, floor, source = [], 0, "SQLite"
    if os.path.abspath(db_path) == os.path.join(here, "bot_trades.db"):
        try:
            from parquet_sync import read_table
            t = read_table("bot_trades", "book_ticks", out_dir=os.path.join(here, "parquet"),
                           columns=BOOK_TICK_COLUMNS)
            if t.num_rows:
                floor = t.column("_rowid")[t.num_rows - 1].as_py()
                rows = [r for r in zip(*(t.column(c).to_pylist() for c in BOOK_TICK_COLUMNS)) if r[5] is not None]
                source = "Parquet + SQLite"
        except (ImportError, FileNotFoundError):
            pass
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows += conn.execute(f"SELECT {', '.join(BOOK_TICK_COLUMNS)} FROM book_ticks "
                             "WHERE btc IS NOT NULL AND rowid > ?", (floor,)).fetchall()
    finally:
        conn.close()
    rows.sort(key=lambda r: r[0])
    return rows, source


def _tape_pairs(db_path: str):
    """For each clean window and each CAL_TARGET_SECONDS: (model_prob, token price, won,
    short-vol model_prob or None) for the favoured side, rebuilt from the recorded tape.
    Volatility comes from the tape's own BTC prices (approximate: the live bot used
    Binance candles); the short-vol model uses signal_engine.ShortVolatility's own
    calculation on the tape's 1 s prices."""
    try:
        from signal_engine import MultiFactorConfig, ShortVolatility
        cfg = MultiFactorConfig()
        floor, lookback, min_returns, twap = (cfg.vol_floor_per_sqrt_s, cfg.vol_lookback_minutes,
                                              cfg.vol_min_returns, cfg.settlement_twap_seconds)
        short = (ShortVolatility.estimate, cfg.short_vol_window_seconds,
                 cfg.short_vol_step_seconds, cfg.short_vol_min_returns)
    except Exception:
        floor, lookback, min_returns, twap = 2.79e-05, 60, 30, 60.0
        short = None
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        sessions = {int(round(ct / 300.0) * 300): (op, off or res) for ct, op, res, off in conn.execute(
            "SELECT close_time, opening_price, resolution, official_resolution FROM market_sessions")}
    except sqlite3.Error:
        return None
    finally:
        conn.close()
    try:
        ticks, _TAPE_SOURCE[0] = _book_ticks_rows(db_path)
    except sqlite3.Error:
        return None
    by_ts, closes, short_closes = {}, {}, {}
    for ts, we, tok, fp, age, btc, fb in ticks:
        d = by_ts.setdefault(ts, {"we": int(round(we / 300.0) * 300), "btc": btc, "fb": fb})
        d[tok] = (fp, age)
        closes[int(ts) // 60 * 60] = btc
        if short:
            short_closes[int(ts) // short[2] * short[2]] = btc

    def sigma(now):
        cur = int(now) // 60 * 60
        cl = [closes.get(m) for m in range(cur - 60 * (lookback + 1), cur, 60)]
        rets = [math.log(b / a) for a, b in zip(cl, cl[1:]) if a and b]
        if len(rets) < min_returns:
            return None
        m = sum(rets) / len(rets)
        return math.sqrt(sum((x - m) ** 2 for x in rets) / len(rets)) / math.sqrt(60)

    def eff(r):
        r = max(r, 0.0)
        return r - 2 * twap / 3 if r >= twap else r ** 3 / (3 * twap * twap)

    best = {}      # (window, target) -> (distance in seconds, tick time)
    for ts, d in by_ts.items():
        sec = d["we"] - ts
        for target in CAL_TARGET_SECONDS:
            gap = abs(sec - target)
            if gap <= 2 and gap < best.get((d["we"], target), (9, 0))[0]:
                best[(d["we"], target)] = (gap, ts)
    out = {t: [] for t in CAL_TARGET_SECONDS}
    for (we, target), (_, ts) in best.items():
        d = by_ts[ts]
        sess = sessions.get(we)
        if not sess or sess[1] not in ("UP", "DOWN") or not sess[0] or d["fb"] is None or d["fb"] > CAL_MAX_FEED_BEHIND_S:
            continue
        v = sigma(ts)
        if v is None:
            continue
        dev = (d["btc"] - sess[0]) / sess[0]
        em = max(v, floor) * math.sqrt(eff(we - ts))
        z = dev / em if em > 0 else 0.0
        if z == 0:
            continue
        side = "UP" if z > 0 else "DOWN"
        tok = d.get("yes" if side == "UP" else "no")
        if not tok or tok[0] is None or (tok[1] is not None and tok[1] >= 2.0):
            continue
        p_short = None
        sv = short[0](short_closes, ts, *short[1:]) if short else None
        if sv is not None:
            em_s = max(sv, floor) * math.sqrt(eff(we - ts))
            if em_s > 0:
                p_short = 0.5 * (1 + math.erf(abs(dev / em_s) / math.sqrt(2)))
        out[target].append((0.5 * (1 + math.erf(abs(z) / math.sqrt(2))), tok[0],
                            1 if sess[1] == side else 0, p_short))
    return out


def calibration_report(settled: list, db_path: str, use_tape: bool = True) -> None:
    print(f"\n{'=' * 72}\nMODEL KALİBRASYONU — model olasılığı gerçekleşenle uyuşuyor mu?\n{'=' * 72}")
    pairs = [(t["model_prob"], t["entry_price"], 1 if t["status"] == "win" else 0)
             for t in settled if t["model_prob"] is not None]
    print(f"1) Botun AÇTIĞI işlemler (seçilmiş anlar), n={len(pairs)}:")
    _calibration_block(pairs)
    print("   Aynı işlemler, piyasa fiyatına göre (giriş fiyatı ne diyordu, ne oldu?):")
    _calibration_block([(q, p, w) for p, q, w in pairs], indent="    ", names=("piyasa fiyatı", "model"), bins=PRICE_BINS)
    # Log-only short-volatility model (signal_engine.ShortVolatility), recorded
    # on trades opened since 2026-10-02.
    short_rows = [(t["model_prob"], t["model_prob_short_vol"], t["entry_price"], 1 if t["status"] == "win" else 0)
                  for t in settled if t["model_prob"] is not None and t["model_prob_short_vol"] is not None]
    if short_rows:
        print("   Kısa-vol modeli kayıtlı işlemler (yalnızca kayıt, karar vermiyor):")
        _short_vol_line(short_rows, "    ")
        _calibration_block([(s, p, w) for _, s, p, w in short_rows], indent="    ",
                           names=("kısa-vol model", "piyasa fiyatı"))
    if not use_tape:
        return
    tape = _tape_pairs(db_path)
    if not tape or not any(tape.values()):
        print("\n2) Kayıtlı kitap/BTC verisi yok ya da yetersiz: pencere bazlı kalibrasyon atlandı.")
        return
    print("\n2) TÜM temiz pencereler (işlem açılmayanlar dahil), sabit kalan sürede, kayıtlı kitaptan yeniden hesaplandı "
          f"(yaklaşık: volatilite kayıtlı BTC'den; kitap kaydı: {_TAPE_SOURCE[0]}):")
    for target in CAL_TARGET_SECONDS:
        rows = tape[target]
        print(f"  --- kalan ≈ {target} sn, pencere sayısı {len(rows)}")
        _calibration_block([(p, q, w) for p, q, w, _ in rows], indent="    ")
        if target == CAL_TARGET_SECONDS[1]:
            print("    aynı pencereler, piyasa fiyatına göre:")
            _calibration_block([(q, p, w) for p, q, w, _ in rows], indent="      ",
                               names=("piyasa fiyatı", "model"), bins=PRICE_BINS)
        short_rows = [(p, s, q, w) for p, q, w, s in rows if s is not None]
        _short_vol_line(short_rows, "    ")
        if short_rows:
            _calibration_block([(s, q, w) for _, s, q, w in short_rows], indent="      ",
                               names=("kısa-vol model", "piyasa fiyatı"))


def main():
    parser = argparse.ArgumentParser(description="Paper trading edge report")
    parser.add_argument("--db", default="bot_trades.db", help="database file (read-only)")
    parser.add_argument("--all", action="store_true",
                        help="also report trades from before the feed-lag guard, separately")
    parser.add_argument("--no-tape", action="store_true",
                        help="skip the all-windows calibration rebuilt from book_ticks (faster)")
    parser.add_argument("--no-forward", action="store_true",
                        help="skip the forward rule evaluation (forward_rules.py)")
    parser.add_argument("--forward-start", metavar="YYYY-MM-DD",
                        help="start the forward evaluation on this Istanbul date instead of the registered ones "
                             "(every rule, R7 included; for trying the section out on older data; such results are not a test)")
    args = parser.parse_args()

    trades, sessions = load_trades(args.db)
    guarded = [t for t in trades if t["guarded"]]
    summarize(guarded, "GECİKME KORUMALI İŞLEMLER (değerlendirmeye esas)")
    if args.all:
        summarize([t for t in trades if not t["guarded"]],
                  "KORUMA ÖNCESİ İŞLEMLER (eski book riski — sadece karşılaştırma için)")

    calibration_report([t for t in guarded if t["status"] in ("win", "loss")], args.db, use_tape=not args.no_tape)

    shadow = load_shadow(args.db)
    if shadow is not None:
        current = [r for r in shadow if r["fill_model"] == "instant"]
        earlier = [r for r in shadow if r["fill_model"] != "instant"]
        summarize_shadow(current, "GÖLGE ÖLÇÜM — anlık taker dolum (sinyal anı, slippage dahil), 1 centlik edge aralıkları, UP/DOWN ayrı")
        if earlier and args.all:
            summarize_shadow(earlier, "GÖLGE ÖLÇÜM — ESKİ DOLUM MODELİ (500 ms gecikme + 1 cent limit; sadece karşılaştırma için)")
        elif earlier:
            print(f"\n(Eski dolum modelinin {len(earlier)} gölge kaydı ayrı tutuluyor; görmek için --all.)")

    perp = load_perp_shadow(args.db)
    if perp is not None:
        summarize_perp_shadow(perp)

    if not args.no_forward:
        try:
            import forward_rules
        except ImportError:        # report copied out of the project directory
            forward_rules = None
        if forward_rules:
            start = forward_rules.FORWARD_START
            if args.forward_start:
                start = datetime.strptime(args.forward_start, "%Y-%m-%d").replace(
                    tzinfo=forward_rules.timezone(forward_rules.timedelta(hours=3))).timestamp()
            # R7 reads the lab shadow (market_tape.db / Parquet), not the bot database
            lab = forward_rules.load_lab_rows(args.db)
            forward_rules.forward_report(guarded, [r for r in shadow or [] if r["fill_model"] == "instant"], perp,
                                         start=start, lab_rows=lab)

    if sessions.get("cycles"):
        print(f"\nDöngüler: {sessions['cycles']} market penceresi, {sessions['traded'] or 0} tanesinde işlem"
              f"; {sessions['lag_blocked'] or 0} pencerenin en iyi sinyali gecikme yüzünden,"
              f" {sessions['market_blocked'] or 0} tanesi market parametreleri yüzünden elendi.")


if __name__ == "__main__":
    main()
