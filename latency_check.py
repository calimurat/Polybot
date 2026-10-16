"""
ORDER-PATH LATENCY CHECK — how long would a taker order take to reach
Polymarket from this machine? (added 2026-10-08, for the perp momentum
signal, whose paper profit falls from +$1,292 at an instant fill to +$487 at
+300 ms: see perp_shadow.py.)

It places NO order and never touches .env or the wallet key. It measures the
parts of the path that can be measured without trading:

  1. fresh connections to the CLOB host: DNS, TCP and TLS handshake times
     (a real order on an idle connection pays these too);
  2. warm keep-alive GETs (/time, and /book of the live market): the round
     trip a real order would add once the connection is open;
  3. EIP-712 signing of an order-shaped message with a THROWAWAY key made on
     the spot (the real order must be signed, and pure-Python secp256k1 can
     cost tens of milliseconds on a small ARM server).

What it cannot measure is the matching engine's own processing time: only a
real order shows that, so the total printed is a lower bound.

Run (on the machine that would place the orders):
  python latency_check.py            # 100 warm requests, 200 signatures
  python latency_check.py --n 300
"""

import argparse
import json
import socket
import ssl
import statistics
import time

import requests

CLOB_HOST = "clob.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com"


def _pct(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * p))]


def _line(name, ms):
    print(f"  {name:<34} medyan {statistics.median(ms):7.1f}   %95 {_pct(ms, .95):7.1f}   "
          f"en kötü {max(ms):7.1f}   en iyi {min(ms):7.1f}  ms   (n={len(ms)})")


def fresh_connections(n=10):
    dns, tcp, tls = [], [], []
    for _ in range(n):
        t0 = time.perf_counter()
        addr = socket.getaddrinfo(CLOB_HOST, 443, type=socket.SOCK_STREAM)[0]
        t1 = time.perf_counter()
        sock = socket.socket(addr[0], socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect(addr[4])
        t2 = time.perf_counter()
        ssl.create_default_context().wrap_socket(sock, server_hostname=CLOB_HOST).close()
        t3 = time.perf_counter()
        dns.append((t1 - t0) * 1000); tcp.append((t2 - t1) * 1000); tls.append((t3 - t2) * 1000)
    return dns, tcp, tls


def warm_gets(url, n, params=None):
    s = requests.Session()
    s.get(url, params=params, timeout=10)         # open the connection, not timed
    ms = []
    for _ in range(n):
        t0 = time.perf_counter()
        r = s.get(url, params=params, timeout=10)
        _ = r.content
        ms.append((time.perf_counter() - t0) * 1000)
        r.raise_for_status()
        time.sleep(0.02)
    return ms


def live_token():
    start = int(time.time() // 300 * 300)
    ev = requests.get(f"{GAMMA_URL}/events", params={"slug": f"btc-updown-5m-{start}"}, timeout=8).json()
    m = (ev if isinstance(ev, list) else ev.get("events", []))[0]["markets"][0]
    ids = m["clobTokenIds"]
    return (json.loads(ids) if isinstance(ids, str) else ids)[0]


def signing(n):
    from eth_account import Account
    from eth_account.messages import encode_typed_data
    acct = Account.create()                       # throwaway key, never the wallet's
    try:
        from eth_keys import keys
        backend = type(keys.backend).__name__
    except Exception:
        backend = "?"
    typed = {
        "types": {
            "EIP712Domain": [{"name": "name", "type": "string"}, {"name": "version", "type": "string"},
                             {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"}],
            "Order": [{"name": "salt", "type": "uint256"}, {"name": "maker", "type": "address"},
                      {"name": "signer", "type": "address"}, {"name": "taker", "type": "address"},
                      {"name": "tokenId", "type": "uint256"}, {"name": "makerAmount", "type": "uint256"},
                      {"name": "takerAmount", "type": "uint256"}, {"name": "expiration", "type": "uint256"},
                      {"name": "nonce", "type": "uint256"}, {"name": "feeRateBps", "type": "uint256"},
                      {"name": "side", "type": "uint8"}, {"name": "signatureType", "type": "uint8"}],
        },
        "primaryType": "Order",
        "domain": {"name": "Polymarket CTF Exchange", "version": "1", "chainId": 137,
                   "verifyingContract": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"},
        "message": {"salt": 123456789, "maker": acct.address, "signer": acct.address,
                    "taker": "0x0000000000000000000000000000000000000000",
                    "tokenId": 7 * 10 ** 70 // 3, "makerAmount": 17400000, "takerAmount": 30000000,
                    "expiration": 0, "nonce": 0, "feeRateBps": 0, "side": 0, "signatureType": 0},
    }
    ms = []
    for i in range(n):
        typed["message"]["salt"] = 123456789 + i
        t0 = time.perf_counter()
        acct.sign_message(encode_typed_data(full_message=typed))
        ms.append((time.perf_counter() - t0) * 1000)
    return ms, backend


def main():
    parser = argparse.ArgumentParser(description="Polymarket order-path latency (no orders placed)")
    parser.add_argument("--n", type=int, default=100)
    args = parser.parse_args()
    print(f"Gecikme ölçümü — {CLOB_HOST} (emir GÖNDERİLMEZ, cüzdan anahtarı kullanılmaz)\n")

    print("1) Yeni bağlantı (boştaki bağlantıda ilk emir bunları öder):")
    dns, tcp, tls = fresh_connections()
    _line("DNS", dns); _line("TCP bağlantısı", tcp); _line("TLS el sıkışma", tls)

    print("\n2) Açık bağlantıda HTTP gidiş-dönüş (emir bu süreyi ekler, işlemden bağımsız):")
    t = warm_gets(f"https://{CLOB_HOST}/time", args.n)
    _line("GET /time", t)
    try:
        b = warm_gets(f"https://{CLOB_HOST}/book", max(20, args.n // 2), params={"token_id": live_token()})
        _line("GET /book (canlı market)", b)
    except Exception as e:
        b = None
        print(f"  /book ölçülemedi: {type(e).__name__}: {e}")

    print("\n3) Emir imzalama (EIP-712, geçici anahtar):")
    s, backend = signing(max(50, args.n * 2))
    _line("imzalama", s)
    print(f"  imza arka ucu: {backend}")

    base = statistics.median(t)
    sign = statistics.median(s)
    print("\nÖzet (emri hazırlayıp Polymarket'e ulaştırma, alt sınır):")
    print(f"  imzalama {sign:.1f} ms + açık bağlantıda gidiş-dönüş {base:.1f} ms ≈ {sign + base:.0f} ms (medyan)")
    print(f"  %95'te: {_pct(s, .95) + _pct(t, .95):.0f} ms.  Eşleştirme motorunun işlem süresi dahil DEĞİL.")


if __name__ == "__main__":
    main()
