"""
CoinPredict Mini App — Flask API Server
=======================================
Bridges the Telegram Mini App frontend to the bot's SQLite database.
Run alongside bot.py (they share the same coinpredict.db file).

Usage:
    pip install flask flask-cors
    python server.py
"""

import sqlite3
import hmac
import hashlib
import json
import os
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qsl

from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Allow Mini App origin (Telegram CDN)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
WIN_MULTIPLIER = 2
DB_PATH = "coinpredict.db"

# ── DB ────────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn

# ── TELEGRAM INIT DATA VALIDATION ─────────────────────────────────────────────

def validate_init_data(init_data: str) -> dict | None:
    """
    Validate Telegram WebApp initData signature.
    Returns parsed data dict if valid, None if invalid.
    In dev mode (no real bot token), skips validation.
    """
    if not init_data or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        return {}  # Dev mode: skip validation

    try:
        data_check_string = "\n".join(
            f"{k}={v}"
            for k, v in sorted(parse_qsl(init_data))
            if k != "hash"
        )
        received_hash = dict(parse_qsl(init_data)).get("hash", "")
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        expected_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(expected_hash, received_hash):
            return None

        parsed = dict(parse_qsl(init_data))
        if "user" in parsed:
            parsed["user"] = json.loads(parsed["user"])
        return parsed
    except Exception:
        return None

def require_valid_user(f):
    """Decorator: validate Telegram initData and inject tg_user_id."""
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        init_data = request.headers.get("X-Telegram-Init-Data", "")
        parsed    = validate_init_data(init_data)
        if parsed is None:
            return jsonify(error="Unauthorized"), 401
        return f(*args, **kwargs)
    return wrapper

# ── HELPERS ───────────────────────────────────────────────────────────────────

def _current_week_start() -> str:
    today  = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return monday.isoformat()

def row_to_dict(row):
    return dict(row) if row else None

# ── ROUTES ────────────────────────────────────────────────────────────────────

@app.get("/")
def health():
    return jsonify(status="ok", service="CoinPredict API")

# ── USER ──────────────────────────────────────────────────────────────────────

@app.post("/api/user")
def upsert_user():
    """Register or fetch a user. Returns user record with rank + weekly_gain."""
    body       = request.json or {}
    tg_id      = body.get("telegram_id")
    username   = body.get("username", "unknown")

    if not tg_id:
        return jsonify(error="telegram_id required"), 400

    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (telegram_id, username, coins) VALUES (?,?,0)",
            (tg_id, username),
        )
        conn.execute(
            "UPDATE users SET username=? WHERE telegram_id=?",
            (username, tg_id),
        )

        user = row_to_dict(conn.execute(
            "SELECT * FROM users WHERE telegram_id=?", (tg_id,)
        ).fetchone())

        week = _current_week_start()
        snapshot = conn.execute(
            "SELECT coins_at_week_start FROM weekly_coins_snapshot WHERE user_id=? AND week_start=?",
            (tg_id, week),
        ).fetchone()
        coins_at_start = snapshot["coins_at_week_start"] if snapshot else 0
        user["weekly_gain"] = user["coins"] - coins_at_start

        rank_row = conn.execute(
            "SELECT COUNT(*)+1 AS rank FROM users WHERE coins > ?",
            (user["coins"],),
        ).fetchone()
        user["rank"] = rank_row["rank"]

    return jsonify(user)

# ── MARKET ────────────────────────────────────────────────────────────────────

@app.get("/api/market")
def current_market():
    """Return the currently open market."""
    with get_db() as conn:
        market = conn.execute(
            "SELECT * FROM markets WHERE status='OPEN' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if not market:
        return jsonify(error="No open market"), 404
    return jsonify(row_to_dict(market))

# ── PRICE ─────────────────────────────────────────────────────────────────────

@app.get("/api/price")
def btc_price():
    """Proxy CoinGecko price (avoids CORS from browser)."""
    import requests as req
    try:
        r = req.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd",
            timeout=8,
        )
        r.raise_for_status()
        price = r.json()["bitcoin"]["usd"]
        return jsonify(price=price)
    except Exception as e:
        return jsonify(error=str(e)), 502

# ── BET ───────────────────────────────────────────────────────────────────────

@app.post("/api/bet")
def place_bet():
    """Place a bet for the current market."""
    body      = request.json or {}
    user_id   = body.get("user_id")
    direction = (body.get("direction") or "").upper()
    amount    = body.get("amount")

    if not all([user_id, direction, amount]):
        return jsonify(error="user_id, direction, amount required"), 400
    if direction not in ("UP", "DOWN"):
        return jsonify(error="direction must be UP or DOWN"), 400
    try:
        amount = int(amount)
    except (TypeError, ValueError):
        return jsonify(error="amount must be an integer"), 400
    if amount < 1:
        return jsonify(error="minimum bet is 1 coin"), 400
    if amount > 10000:
        return jsonify(error="maximum bet is 10,000 coins"), 400

    with get_db() as conn:
        market = conn.execute(
            "SELECT * FROM markets WHERE status='OPEN' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not market:
            return jsonify(error="No active market"), 409

        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id=?", (user_id,)
        ).fetchone()
        if not user:
            return jsonify(error="User not found"), 404
        if user["coins"] < amount:
            return jsonify(error=f"Not enough coins (have {user['coins']})"), 409

        existing = conn.execute(
            "SELECT id FROM bets WHERE user_id=? AND market_id=? AND resolved=0",
            (user_id, market["id"]),
        ).fetchone()
        if existing:
            return jsonify(error="Already have an active bet this market"), 409

        conn.execute(
            "UPDATE users SET coins = coins - ? WHERE telegram_id=?",
            (amount, user_id),
        )
        conn.execute(
            "INSERT INTO bets (user_id, market_id, direction, amount) VALUES (?,?,?,?)",
            (user_id, market["id"], direction, amount),
        )

        new_coins = conn.execute(
            "SELECT coins FROM users WHERE telegram_id=?", (user_id,)
        ).fetchone()["coins"]

    return jsonify(
        ok=True,
        direction=direction,
        amount=amount,
        potential_payout=amount * WIN_MULTIPLIER,
        new_balance=new_coins,
    )

# ── ACTIVE BET ────────────────────────────────────────────────────────────────

@app.get("/api/bets/active")
def active_bet():
    """Return unresolved bet for user in the current open market."""
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify(error="user_id required"), 400

    with get_db() as conn:
        market = conn.execute(
            "SELECT id FROM markets WHERE status='OPEN' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not market:
            return jsonify(bet=None)

        bet = conn.execute(
            "SELECT * FROM bets WHERE user_id=? AND market_id=? AND resolved=0",
            (user_id, market["id"]),
        ).fetchone()

    return jsonify(bet=row_to_dict(bet))

# ── RECENT BETS ───────────────────────────────────────────────────────────────

@app.get("/api/bets/recent")
def recent_bets():
    """Return last 10 bets for a user with won/lost info."""
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify(error="user_id required"), 400

    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT b.*, m.open_price, m.close_price,
                   CASE WHEN b.resolved=1
                        AND ((m.close_price > m.open_price AND b.direction='UP')
                          OR (m.close_price < m.open_price AND b.direction='DOWN'))
                        THEN 1 ELSE 0 END AS won
            FROM bets b
            JOIN markets m ON m.id = b.market_id
            WHERE b.user_id = ?
            ORDER BY b.id DESC
            LIMIT 10
            """,
            (user_id,),
        ).fetchall()

    return jsonify(bets=[dict(r) for r in rows])

# ── LEADERBOARD ───────────────────────────────────────────────────────────────

@app.get("/api/leaderboard")
def leaderboard():
    """Return top 10 users by weekly gain."""
    week = _current_week_start()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT u.telegram_id, u.username, u.coins,
                   u.coins - COALESCE(s.coins_at_week_start, 0) AS weekly_gain
            FROM users u
            LEFT JOIN weekly_coins_snapshot s
                ON s.user_id = u.telegram_id AND s.week_start = ?
            ORDER BY weekly_gain DESC, u.coins DESC
            LIMIT 10
            """,
            (week,),
        ).fetchall()

    return jsonify(entries=[dict(r) for r in rows], week_start=week)

# ── RUN ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
