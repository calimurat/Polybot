# POLYMARKET BOT — SETUP GUIDE

## Quick Start (Paper Trading)

### 1. Clone / Download Code
```bash
# Copy all 9 Python files to your directory
ls -la
# Should have:
#   config.py
#   database.py
#   external_connections.py
#   market_loader.py
#   polymarket_production_bot.py
#   position_manager.py
#   run.py
#   signal_engine.py
#   wallet_manager.py
```

### 2. Install Dependencies
```bash
pip install --upgrade pip
pip install requests websocket-client eth-account eth-keys python-dotenv

# Optional (for async):
# pip install aiohttp
```

### 3. Setup Environment (.env file)

**Copy template:**
```bash
cp .env.example .env
```

**Edit .env with your values:**
```bash
# Required for paper trading (simulated)
PRIVATE_KEY=0x...              # Your MetaMask private key
WALLET_ADDRESS=0x...           # Your wallet address
ALCHEMY_API_KEY=...            # Alchemy Polygon API key

# Paper trading settings
PAPER_TRADING=true             # ← TEST MODE
DRY_RUN=false
ENVIRONMENT=production
LOG_LEVEL=INFO
TRADE_SIZE=10
```

### 4. Run Bot (Paper Trading)

```bash
# Test 5-minute session
python run.py

# Expected output:
# ================================================================================
# BOT CONFIGURATION
# ================================================================================
# Environment:         production
# Paper trading:       True
# Market:              BTCUSD-5M
# Duration:            300s (5 min)
# ...
# 
# [2024-09-13 17:30:45] [INFO] [BOT] Controller initialized
# [2024-09-13 17:30:46] [INFO] [MARKET] Loading market data...
# [2024-09-13 17:30:48] [INFO] [MARKET] Loaded: Will BTC USD be above $X at 17:35?
# [2024-09-13 17:30:49] [INFO] [MARKET] Session started: session_20240913_173049
# [2024-09-13 17:30:50] [INFO] [LOOP] Starting trading loop...
# [2024-09-13 17:31:00] [SIGNAL] BUY_DOWN @ 0.6800 (confidence: 0.85)
# [2024-09-13 17:31:00] [BOT] Trade executed: order_123456
# ...
# [2024-09-13 17:35:50] [MARKET] Session ended
# [2024-09-13 17:35:51] [SESSION] Statistics:
#   Total trades: 3
#   Wins: 2
#   Losses: 1
#   Win rate: 66.7%
#   Total PnL: +$12.50
```

### 5. Check Logs
```bash
# Real-time logging
tail -f bot.log

# View trades
sqlite3 bot_trades.db "SELECT * FROM orders LIMIT 5;"
sqlite3 bot_trades.db "SELECT * FROM positions LIMIT 5;"
```

---

## Real Trading Setup

⚠️ **WARNING: Use actual capital only after thorough testing!**

### 1. Prepare Wallet (MetaMask)
```
1. Go to https://polygon.technology/ (Polygon network)
2. Bridge USDC to Polygon:
   - Get USDC on Ethereum mainnet
   - Use bridge.polygon.technology
   - Transfer to your wallet address
   
3. Get POL for gas (very cheap):
   - Faucet: https://faucet.polygon.technology/
   - Or buy small amount (~$1)

4. Verify in MetaMask:
   - Network: Polygon Mainnet
   - Token: USDC (6 decimals)
   - Balance: ≥ $100 recommended
```

### 2. Update .env
```bash
# Change settings
PAPER_TRADING=false            # ← REAL MODE
DRY_RUN=false

# Keep everything else the same
PRIVATE_KEY=0x...
WALLET_ADDRESS=0x...
ALCHEMY_API_KEY=...
```

### 3. Start with Small Amount
```bash
# Test with $10-50 first
TRADE_SIZE=1   # 1 token = ~$0.50-1.00
python run.py
```

### 4. Monitor Closely
```bash
# Watch logs in real-time
tail -f bot.log

# Monitor wallet balance
# Check Polygon scan: https://polygonscan.com/
```

---

## Troubleshooting

### Issue: "Configuration error: PRIVATE_KEY must be hex format: 0x..."

**Solution:**
```bash
# Make sure .env has correct format
PRIVATE_KEY=0xYOUR_64_CHAR_HEX_KEY

# Should be exactly 66 chars: 0x + 64 hex chars
# Example: 0xa1b2c3d4e5f6...
```

### Issue: "Failed to load private key"

**Solution:**
```bash
# Check key format
# Correct: 0x1234567890abcdef...
# Wrong: 1234567890abcdef (missing 0x)
# Wrong: 0xZ123... (invalid hex)
```

### Issue: "ALCHEMY_API_KEY not set"

**Solution:**
```bash
# Get free API key:
# 1. Go to https://www.alchemy.com/
# 2. Sign up
# 3. Create app
# 4. Copy API key
# 5. Paste in .env
```

### Issue: "Insufficient liquidity: $0"

**Solution:**
```bash
# Market doesn't exist or is inactive
# Check if BTCUSD-5M market exists on Polymarket
# Try different market or wait for next market
```

### Issue: "WS connection failed"

**Solution:**
```bash
# Network connectivity issue
# Check internet connection
# Try again in 10 seconds
# If persistent, check firewall/proxy
```

---

## File Structure

```
project/
├── .env                              # Configuration (⚠️ KEEP SECRET)
├── .env.example                      # Template (safe to commit)
├── config.py                         # Settings + validation
├── wallet_manager.py                 # Private key + signing
├── database.py                       # SQLite storage
├── market_loader.py                  # Gamma API
├── signal_engine.py                  # V8.4 strategy
├── position_manager.py               # Trade lifecycle
├── polymarket_production_bot.py       # CLOB WS/REST
├── external_connections.py           # Chainlink + Binance + Alchemy
├── run.py                            # Main entry point
├── bot.log                           # Logs (generated)
└── bot_trades.db                     # Database (generated)
```

---

## Database Schema

```sql
-- Orders (all submitted trades)
SELECT * FROM orders;
-- Columns: id, timestamp, side, price, size, status, tx_hash, filled_price, signal_type

-- Positions (open + closed trades)
SELECT * FROM positions;
-- Columns: id, order_id, entry_price, entry_time, exit_price, exit_time, pnl, pnl_percent, status

-- Trades (settled trades)
SELECT * FROM trades;
-- Columns: id, position_id, entry_time, exit_time, entry_price, exit_price, size, pnl, pnl_percent, status, duration_seconds

-- Wallets (balance tracking)
SELECT * FROM wallets;
-- Columns: address, balance_usdc, balance_pol, last_update, transaction_count

-- Market sessions (session summary)
SELECT * FROM market_sessions;
-- Columns: session_id, open_time, close_time, opening_price, closing_price, resolution, total_trades, total_pnl, win_rate
```

---

## Statistics & Monitoring

### View Session Stats
```bash
sqlite3 bot_trades.db << 'SQL'
SELECT 
  session_id,
  total_trades,
  win_rate,
  total_pnl
FROM market_sessions
ORDER BY open_time DESC
LIMIT 10;
SQL
```

### Calculate Win Rate
```bash
sqlite3 bot_trades.db << 'SQL'
SELECT 
  COUNT(*) as total_trades,
  SUM(CASE WHEN status = 'WIN' THEN 1 ELSE 0 END) as wins,
  ROUND(100.0 * SUM(CASE WHEN status = 'WIN' THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate_pct,
  SUM(pnl) as total_pnl
FROM trades
WHERE status IN ('WIN', 'LOSS');
SQL
```

---

## Security Checklist

- [ ] Private key stored in .env (not in code)
- [ ] .env added to .gitignore
- [ ] ALCHEMY_API_KEY set
- [ ] Test with paper trading first
- [ ] Small amount for real trading ($10-50)
- [ ] Monitor logs during trading
- [ ] Check wallet balance frequently
- [ ] Never share .env file

---

## Support

For issues:
1. Check logs: `tail -f bot.log`
2. Verify .env settings
3. Test with paper trading
4. Check internet connection
5. Restart bot

---

**Happy trading! 🚀**

