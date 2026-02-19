"""
CoinPredict Telegram Bot
========================
Features:
- Reaction-based daily reward (100 coins, once per 24h)
- /bet UP/DOWN <amount> prediction market (BTC, configurable interval — default 1 min)
- Tie handling: full refund if close price == open price
- /leaderboard (top 10, resets weekly on Monday UTC)
- /balance to check your coins
- Anti-cheat: rate limiting, input validation, duplicate bet prevention
- Thread-safe SQLite with WAL mode
- Graceful CoinGecko API error handling
"""

import sqlite3
import requests
import logging
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageReactionHandler,
    ContextTypes,
)

# ─── CONFIG ───────────────────────────────────────────────────────────────────

TOKEN = "REPLACE WITH TOKEN\!"

# Set this to the message_id of your pinned "Daily Reward" post
DAILY_REWARD_MESSAGE_ID = None  # e.g. 42

DAILY_REWARD_AMOUNT   = 100
MARKET_INTERVAL_MINS  = 5    # 1 | 3 | 5 — change freely, no other code needs updating
WIN_MULTIPLIER        = 2       # correct bet → 2× payout
MAX_BET_AMOUNT        = 10_000  # anti-whale cap per bet
MIN_BET_AMOUNT        = 1

# Rate limiting: max N bet commands per user per minute
BET_RATE_LIMIT_COUNT  = 3
BET_RATE_LIMIT_WINDOW = 60  # seconds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─── DATABASE ─────────────────────────────────────────────────────────────────

_db_lock = threading.Lock()

def get_conn():
    """Return a thread-local WAL-mode SQLite connection."""
    conn = sqlite3.connect("coinpredict.db", check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id   INTEGER PRIMARY KEY,
                username      TEXT,
                coins         INTEGER NOT NULL DEFAULT 0,
                last_daily    TEXT    -- ISO date YYYY-MM-DD UTC
            );

            CREATE TABLE IF NOT EXISTS markets (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                open_price    REAL    NOT NULL,
                close_price   REAL,
                open_time     TEXT    NOT NULL,
                close_time    TEXT,
                status        TEXT    NOT NULL DEFAULT 'OPEN'  -- OPEN | CLOSED
            );

            CREATE TABLE IF NOT EXISTS bets (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id       INTEGER NOT NULL REFERENCES users(telegram_id),
                market_id     INTEGER NOT NULL REFERENCES markets(id),
                direction     TEXT    NOT NULL,  -- UP | DOWN
                amount        INTEGER NOT NULL,
                resolved      INTEGER NOT NULL DEFAULT 0,
                UNIQUE(user_id, market_id)       -- one bet per user per market
            );

            -- Weekly leaderboard snapshots (for display; actual coins never reset)
            CREATE TABLE IF NOT EXISTS weekly_coins_snapshot (
                user_id       INTEGER PRIMARY KEY REFERENCES users(telegram_id),
                coins_at_week_start INTEGER NOT NULL DEFAULT 0,
                week_start    TEXT    NOT NULL  -- ISO date of Monday
            );
        """)
    log.info("Database initialised.")

# ─── PRICE FETCHER ────────────────────────────────────────────────────────────

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=bitcoin&vs_currencies=usd"
)

def get_btc_price() -> float | None:
    """Fetch BTC/USD from CoinGecko. Returns None on failure."""
    try:
        resp = requests.get(COINGECKO_URL, timeout=10)
        resp.raise_for_status()
        price = resp.json()["bitcoin"]["usd"]
        log.info(f"BTC price fetched: ${price:,.2f}")
        return float(price)
    except Exception as e:
        log.error(f"CoinGecko error: {e}")
        return None

# ─── MARKET ENGINE ────────────────────────────────────────────────────────────

def get_open_market(conn):
    return conn.execute(
        "SELECT * FROM markets WHERE status='OPEN' ORDER BY id DESC LIMIT 1"
    ).fetchone()

def open_new_market(conn):
    price = get_btc_price()
    if price is None:
        log.warning("Could not open new market: price unavailable.")
        return
    now       = datetime.now(timezone.utc)
    close_at  = (now + timedelta(minutes=MARKET_INTERVAL_MINS)).isoformat()
    conn.execute(
        "INSERT INTO markets (open_price, open_time, close_time, status) VALUES (?,?,?,'OPEN')",
        (price, now.isoformat(), close_at),
    )
    log.info(f"New market opened at ${price:,.2f}, closes at {close_at}")

def resolve_market():
    """Close the open market, pay winners (or refund on tie), open a new one."""
    close_price = get_btc_price()
    if close_price is None:
        log.warning("Skipping market resolution: price unavailable.")
        return

    with _db_lock, get_conn() as conn:
        market = get_open_market(conn)
        if not market:
            log.info("No open market to resolve; opening one.")
            open_new_market(conn)
            return

        market_id  = market["id"]
        open_price = market["open_price"]
        now        = datetime.now(timezone.utc).isoformat()
        is_tie     = abs(close_price - open_price) < 0.01   # price unchanged

        conn.execute(
            "UPDATE markets SET close_price=?, close_time=?, status='CLOSED' WHERE id=?",
            (close_price, now, market_id),
        )

        all_bets = conn.execute(
            "SELECT user_id, amount, direction FROM bets "
            "WHERE market_id=? AND resolved=0",
            (market_id,),
        ).fetchall()

        if is_tie:
            # ── TIE: refund every bettor their stake ──
            for row in all_bets:
                conn.execute(
                    "UPDATE users SET coins = coins + ? WHERE telegram_id=?",
                    (row["amount"], row["user_id"]),
                )
            log.info(f"Market {market_id} TIED at ${close_price:,.2f}. {len(all_bets)} bets refunded.")
        else:
            # ── WIN/LOSS: pay winners 2× ──
            direction = "UP" if close_price > open_price else "DOWN"
            winners = [r for r in all_bets if r["direction"] == direction]
            for row in winners:
                conn.execute(
                    "UPDATE users SET coins = coins + ? WHERE telegram_id=?",
                    (row["amount"] * WIN_MULTIPLIER, row["user_id"]),
                )
            log.info(
                f"Market {market_id}: ${open_price:,.2f}→${close_price:,.2f} ({direction}). "
                f"{len(winners)}/{len(all_bets)} bets won."
            )

        conn.execute("UPDATE bets SET resolved=1 WHERE market_id=?", (market_id,))
        open_new_market(conn)

# ─── WEEKLY LEADERBOARD RESET ─────────────────────────────────────────────────

def _current_week_start() -> str:
    """Return ISO date string of the most recent Monday (UTC)."""
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return monday.isoformat()

def refresh_weekly_snapshot():
    """Run every Monday at midnight UTC: snapshot current coin balances."""
    week_start = _current_week_start()
    with _db_lock, get_conn() as conn:
        conn.execute("DELETE FROM weekly_coins_snapshot")
        conn.execute(
            """
            INSERT INTO weekly_coins_snapshot (user_id, coins_at_week_start, week_start)
            SELECT telegram_id, coins, ? FROM users
            """,
            (week_start,),
        )
    log.info(f"Weekly leaderboard snapshot refreshed for week of {week_start}.")

# ─── RATE LIMITER ─────────────────────────────────────────────────────────────

_rate_buckets: dict[int, list[float]] = defaultdict(list)
_rate_lock = threading.Lock()

def is_rate_limited(user_id: int) -> bool:
    """True if the user has exceeded BET_RATE_LIMIT_COUNT in the last window."""
    now = datetime.now(timezone.utc).timestamp()
    cutoff = now - BET_RATE_LIMIT_WINDOW
    with _rate_lock:
        bucket = _rate_buckets[user_id]
        # Prune old entries
        _rate_buckets[user_id] = [t for t in bucket if t > cutoff]
        if len(_rate_buckets[user_id]) >= BET_RATE_LIMIT_COUNT:
            return True
        _rate_buckets[user_id].append(now)
        return False

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def ensure_user(conn, user):
    conn.execute(
        "INSERT OR IGNORE INTO users (telegram_id, username, coins) VALUES (?,?,0)",
        (user.id, user.username or user.first_name),
    )
    # Keep username fresh
    conn.execute(
        "UPDATE users SET username=? WHERE telegram_id=?",
        (user.username or user.first_name, user.id),
    )

# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    with _db_lock, get_conn() as conn:
        ensure_user(conn, user)
    await update.message.reply_text(
        "👋 Welcome to *CoinPredict*!\n\n"
        "React ❤️ to the pinned Daily Reward post to earn *100 coins* (once per day).\n"
        "Then use /bet to wager coins on BTC going UP or DOWN in the next 5 minutes!\n\n"
        "Commands:\n"
        "/balance – Check your coins\n"
        "/bet UP 50 – Bet 50 coins on BTC going UP\n"
        "/bet DOWN 50 – Bet 50 coins on BTC going DOWN\n"
        "/leaderboard – See the weekly top 10\n"
        "/price – Current BTC price",
        parse_mode="Markdown",
    )

async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    with _db_lock, get_conn() as conn:
        ensure_user(conn, user)
        row = conn.execute(
            "SELECT coins FROM users WHERE telegram_id=?", (user.id,)
        ).fetchone()
    await update.message.reply_text(f"💰 You have *{row['coins']:,}* coins.", parse_mode="Markdown")

async def cmd_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    price = get_btc_price()
    if price is None:
        await update.message.reply_text("⚠️ Could not fetch BTC price right now. Try again shortly.")
        return
    with _db_lock, get_conn() as conn:
        market = get_open_market(conn)
    if market:
        change = price - market["open_price"]
        pct    = change / market["open_price"] * 100
        sign   = "+" if change >= 0 else ""
        await update.message.reply_text(
            f"₿ BTC is *${price:,.2f}*\n"
            f"Open (current market): ${market['open_price']:,.2f} ({sign}{pct:.2f}%)",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(f"₿ BTC is *${price:,.2f}*", parse_mode="Markdown")

async def cmd_bet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    # ── Rate limit check
    if is_rate_limited(user.id):
        await update.message.reply_text(
            f"⏳ Slow down! You can place at most {BET_RATE_LIMIT_COUNT} bets per minute."
        )
        return

    # ── Input validation
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /bet <UP|DOWN> <amount>\nExample: /bet UP 50"
        )
        return

    direction_raw = context.args[0].upper()
    if direction_raw not in ("UP", "DOWN"):
        await update.message.reply_text("Direction must be UP or DOWN. Example: /bet UP 100")
        return

    try:
        amount = int(context.args[1])
    except ValueError:
        await update.message.reply_text("Amount must be a whole number. Example: /bet UP 100")
        return

    if amount < MIN_BET_AMOUNT:
        await update.message.reply_text(f"Minimum bet is {MIN_BET_AMOUNT} coin.")
        return
    if amount > MAX_BET_AMOUNT:
        await update.message.reply_text(f"Maximum bet is {MAX_BET_AMOUNT:,} coins.")
        return

    with _db_lock, get_conn() as conn:
        ensure_user(conn, user)

        market = get_open_market(conn)
        if not market:
            await update.message.reply_text("⚠️ No active market right now. Try again shortly.")
            return

        row = conn.execute(
            "SELECT coins FROM users WHERE telegram_id=?", (user.id,)
        ).fetchone()
        coins = row["coins"]

        if amount > coins:
            await update.message.reply_text(
                f"❌ Not enough coins. You have {coins:,} coins but tried to bet {amount:,}."
            )
            return

        # ── Duplicate bet check (UNIQUE constraint in DB also guards this)
        existing = conn.execute(
            "SELECT id FROM bets WHERE user_id=? AND market_id=? AND resolved=0",
            (user.id, market["id"]),
        ).fetchone()
        if existing:
            await update.message.reply_text(
                "⚠️ You already have a bet on this market. Wait for it to resolve."
            )
            return

        # ── Place the bet
        conn.execute(
            "UPDATE users SET coins = coins - ? WHERE telegram_id=?",
            (amount, user.id),
        )
        conn.execute(
            "INSERT INTO bets (user_id, market_id, direction, amount) VALUES (?,?,?,?)",
            (user.id, market["id"], direction_raw, amount),
        )

        new_balance = coins - amount
        close_time_str = ""
        if market["open_time"]:
            try:
                open_dt   = datetime.fromisoformat(market["open_time"])
                close_dt  = open_dt + timedelta(minutes=MARKET_INTERVAL_MINS)
                remaining = int((close_dt - datetime.now(timezone.utc)).total_seconds())
                if remaining > 0:
                    close_time_str = f"\n⏱ Market resolves in ~{remaining // 60}m {remaining % 60}s."
            except Exception:
                pass

    direction_emoji = "📈" if direction_raw == "UP" else "📉"
    await update.message.reply_text(
        f"{direction_emoji} Bet placed: *{direction_raw}* with *{amount:,}* coins.\n"
        f"If correct you'll win *{amount * WIN_MULTIPLIER:,}* coins back.{close_time_str}\n"
        f"Balance: {new_balance:,} coins.",
        parse_mode="Markdown",
    )

async def cmd_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    week_start = _current_week_start()
    with _db_lock, get_conn() as conn:
        # Compute weekly gain = current coins − coins_at_week_start (snapshot)
        rows = conn.execute(
            """
            SELECT u.username,
                   u.coins,
                   u.coins - COALESCE(s.coins_at_week_start, 0) AS weekly_gain
            FROM users u
            LEFT JOIN weekly_coins_snapshot s
                ON s.user_id = u.telegram_id AND s.week_start = ?
            ORDER BY weekly_gain DESC, u.coins DESC
            LIMIT 10
            """,
            (week_start,),
        ).fetchall()

    if not rows:
        await update.message.reply_text("No players yet. Be the first!")
        return

    medals = ["🥇", "🥈", "🥉"]
    lines  = [f"🏆 *Weekly Leaderboard* (week of {week_start})\n"]
    for i, row in enumerate(rows):
        medal = medals[i] if i < 3 else f"{i+1}."
        sign  = "+" if row["weekly_gain"] >= 0 else ""
        lines.append(
            f"{medal} {row['username']} — {row['coins']:,} coins "
            f"({sign}{row['weekly_gain']:,} this week)"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ─── REACTION HANDLER (daily reward) ─────────────────────────────────────────

async def on_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Grant 100 coins when a user reacts to the pinned Daily Reward post."""
    reaction = update.message_reaction
    if not reaction or not reaction.new_reaction:
        return  # reaction removed, ignore

    # If a specific message ID is configured, only reward that message
    if DAILY_REWARD_MESSAGE_ID and reaction.message_id != DAILY_REWARD_MESSAGE_ID:
        return

    user = reaction.user
    if not user or user.is_bot:
        return

    today = datetime.now(timezone.utc).date().isoformat()

    with _db_lock, get_conn() as conn:
        ensure_user(conn, user)
        row = conn.execute(
            "SELECT last_daily FROM users WHERE telegram_id=?", (user.id,)
        ).fetchone()

        if row and row["last_daily"] == today:
            # Already claimed — silently skip (no spam reply)
            return

        conn.execute(
            "UPDATE users SET coins = coins + ?, last_daily = ? WHERE telegram_id=?",
            (DAILY_REWARD_AMOUNT, today, user.id),
        )
        new_balance = conn.execute(
            "SELECT coins FROM users WHERE telegram_id=?", (user.id,)
        ).fetchone()["coins"]

    # Try to notify user privately; fall back to channel reply
    try:
        await context.bot.send_message(
            chat_id=user.id,
            text=(
                f"🎉 Daily reward claimed! You received *{DAILY_REWARD_AMOUNT}* coins.\n"
                f"Balance: *{new_balance:,}* coins.\n\n"
                f"Use /bet UP 50 or /bet DOWN 50 to predict BTC!"
            ),
            parse_mode="Markdown",
        )
    except Exception:
        pass  # User hasn't started the bot; that's fine

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    init_db()

    # Open the first market on startup
    with _db_lock, get_conn() as conn:
        if not get_open_market(conn):
            open_new_market(conn)

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("balance",     cmd_balance))
    app.add_handler(CommandHandler("price",       cmd_price))
    app.add_handler(CommandHandler("bet",         cmd_bet))
    app.add_handler(CommandHandler("leaderboard", cmd_leaderboard))
    app.add_handler(MessageReactionHandler(on_reaction))

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(resolve_market,          "interval", minutes=MARKET_INTERVAL_MINS)
    scheduler.add_job(refresh_weekly_snapshot, "cron",     day_of_week="mon", hour=0, minute=0)
    scheduler.start()
    log.info("Scheduler started.")

    log.info("Bot polling…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
