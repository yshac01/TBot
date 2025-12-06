#!/usr/bin/env python3
"""
Trading bot monitor — container-friendly edition
- Reads secrets from environment (.env for local, env_file for Docker)
- Logs to stdout (docker logs) and rotating file logs/bot.log
- Handles SIGTERM/SIGINT cleanly so `docker stop` shuts down
"""

import os
import signal
import logging
from logging.handlers import RotatingFileHandler
import threading
import time
import math
from datetime import datetime, timezone, time as dt_time

import pandas as pd
import numpy as np
import pyodbc
import pytz
import requests
import oandapyV20
import oandapyV20.endpoints.accounts as accounts
import oandapyV20.endpoints.instruments as instruments

# optional dotenv for local dev
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ============================================================================
# === ENVIRONMENT / CONFIG ==================================================
# ============================================================================

API_TOKEN        = os.getenv("API_TOKEN")
ACCOUNT_ID       = os.getenv("ACCOUNT_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

DB_DRIVER   = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
DB_SERVER   = os.getenv("DB_SERVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

missing = [k for k, v in {
    "API_TOKEN": API_TOKEN,
    "ACCOUNT_ID": ACCOUNT_ID,
    "DB_SERVER": DB_SERVER,
    "DB_DATABASE": DB_DATABASE,
    "DB_USERNAME": DB_USERNAME,
    "DB_PASSWORD": DB_PASSWORD,
}.items() if not v]
if missing:
    raise SystemExit(f"Missing required environment variables: {missing}")

# ============================================================================
# === LOGGING ================================================================
# ============================================================================
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

sh = logging.StreamHandler()
sh.setFormatter(fmt)
log.addHandler(sh)

os.makedirs("logs", exist_ok=True)
fh = RotatingFileHandler("logs/bot.log", maxBytes=5 * 1024 * 1024, backupCount=5)
fh.setFormatter(fmt)
log.addHandler(fh)

# ============================================================================
# === SIGNAL HANDLING ========================================================
# ============================================================================
stop_event = threading.Event()

def _handle_exit(sig, frame):
    log.info("Received signal %s, shutting down...", sig)
    stop_event.set()

signal.signal(signal.SIGTERM, _handle_exit)
signal.signal(signal.SIGINT, _handle_exit)

# ============================================================================
# === API / DB CLIENTS =======================================================
# ============================================================================
client = oandapyV20.API(access_token=API_TOKEN, environment="live")

conn_str = (
    f"Driver={{{DB_DRIVER}}};"
    f"Server={DB_SERVER};"
    f"Database={DB_DATABASE};"
    f"Uid={DB_USERNAME};"
    f"Pwd={DB_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=60;"
)

# ============================================================================
# === CONSTANTS ==============================================================
# ============================================================================
SYMBOLS = ["EUR_USD", "GBP_USD", "USD_JPY", 
           "USD_CAD", "NAS100_USD", "EUR_CAD"]

TIMEFRAMES = {
    "M15": "M15",
    "H1": "H1",
    "H4": "H4"
}

seen_retracements = set()
last_cleared_date = None

# ============================================================================
# === HELPERS ================================================================
# ============================================================================
def open_db():
    conn = pyodbc.connect(conn_str)
    return conn, conn.cursor()

def test_api_connection():
    try:
        r = accounts.AccountSummary(accountID=ACCOUNT_ID)
        client.request(r)
        log.info("✅ API Connection successful!")
        return True
    except Exception as e:
        log.error("❌ API Connection failed: %s", e)
        return False

def get_count_since_midnight(granularity_mins):
    now_utc = datetime.now(timezone.utc)
    midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    diff = now_utc - midnight
    return math.ceil(diff.total_seconds() / (granularity_mins * 60))

def get_candles(symbol, granularity, count):
    params = {"granularity": granularity, "count": count, "price": "M"}
    r = instruments.InstrumentsCandles(instrument=symbol, params=params)
    try:
        client.request(r)
        candles = r.response["candles"]
        df = pd.DataFrame([{
            "Date": c["time"],
            "Open": float(c["mid"]["o"]),
            "High": float(c["mid"]["h"]),
            "Low": float(c["mid"]["l"]),
            "Close": float(c["mid"]["c"]),
            "Complete": c["complete"]
        } for c in candles])
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert("Europe/London")
        return df
    except Exception as e:
        log.error("❌ Error fetching %s %s: %s", symbol, granularity, e)
        return pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Complete"])

def detect_ote(df):
    required = {'Date', 'Open', 'High', 'Low', 'Close'}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing required columns {required}, Found {df.columns}")

    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    df['Time'] = df['Date'].dt.time
    df['Time_Diff'] = df['Date'].diff().dt.total_seconds() / 60
    avg_interval = df['Time_Diff'].median()

    is_1hr_interval = 60 <= avg_interval < 75
    is_15m_or_lower = avg_interval <= 15
    is_4hr_interval = avg_interval >= 240

    df[['Bullish_FVG_High', 'Bullish_FVG_Low',
        'Bearish_FVG_High', 'Bearish_FVG_Low',
        'FVG_Date', 'Retracement_Date']] = np.nan

    df[['Bullish_FVG_Flag', 'Bearish_FVG_Flag', 'Retracement_Flag']] = False

    fvg_list = []

    # --- FVG detection ---
    for i in range(2, len(df)):
        prev_high, prev_low = df.at[i-2, 'High'], df.at[i-2, 'Low']
        curr_low, curr_high = df.at[i, 'Low'], df.at[i, 'High']
        fvg_time = df.at[i-1, 'Date'].time()

        if is_15m_or_lower:
            is_valid_fvg_time = (pd.Timestamp("12:15").time() <= fvg_time <= pd.Timestamp("14:30").time())
        elif is_1hr_interval:
            is_valid_fvg_time = (pd.Timestamp("07:00").time() <= fvg_time <= pd.Timestamp("14:30").time())
        else:
            is_valid_fvg_time = True

        # Bullish
        if curr_low > prev_high and is_valid_fvg_time:
            df.at[i-1, 'Bullish_FVG_High'] = prev_high
            df.at[i-1, 'Bullish_FVG_Low'] = curr_low
            df.at[i-1, 'Bullish_FVG_Flag'] = True
            fvg_list.append(('Bullish', prev_high, curr_low, df.at[i-1, 'Date'], i-1))

        # Bearish
        if curr_high < prev_low and is_valid_fvg_time:
            df.at[i-1, 'Bearish_FVG_High'] = curr_high
            df.at[i-1, 'Bearish_FVG_Low'] = prev_low
            df.at[i-1, 'Bearish_FVG_Flag'] = True
            fvg_list.append(('Bearish', curr_high, prev_low, df.at[i-1, 'Date'], i-1))

    # --- Retracement detection ---
    for fvg_type, fvg_high, fvg_low, fvg_date, fvg_idx in fvg_list:
        for j in range(fvg_idx + 2, len(df)):
            retrace_time = df.at[j, 'Date'].time()

            if is_1hr_interval:
                is_valid_time = (pd.Timestamp("12:00").time() <= retrace_time <= pd.Timestamp("14:30").time())
            elif is_15m_or_lower:
                is_valid_time = (pd.Timestamp("12:15").time() <= retrace_time <= pd.Timestamp("14:30").time())
            elif is_4hr_interval:
                is_valid_time = (pd.Timestamp("10:00").time() <= retrace_time <= pd.Timestamp("14:30").time())
            else:
                is_valid_time = True

            if not is_valid_time:
                continue

            moved_away = False
            for k in range(fvg_idx + 1, j):
                if fvg_type == 'Bullish' and df.at[k, 'High'] > fvg_low:
                    moved_away = True
                    break
                elif fvg_type == 'Bearish' and df.at[k, 'Low'] < fvg_high:
                    moved_away = True
                    break

            if not moved_away:
                continue

            if fvg_type == 'Bullish':
                if df.at[j, 'Low'] <= fvg_low and df.at[j, 'High'] >= fvg_high:
                    df.at[j, 'Retracement_Flag'] = True
                    df.at[j, 'FVG_Date'] = fvg_date
                    df.at[j, 'Retracement_Date'] = df.at[j, 'Date']
                    break

            elif fvg_type == 'Bearish':
                if df.at[j, 'High'] >= fvg_high and df.at[j, 'Low'] <= fvg_low:
                    df.at[j, 'Retracement_Flag'] = True
                    df.at[j, 'FVG_Date'] = fvg_date
                    df.at[j, 'Retracement_Date'] = df.at[j, 'Date']
                    break

    return df

def format_ohlc_as_text(df):
    if df.empty:
        return "No data available"
    lines = []
    for _, row in df.iterrows():
        dt = row["Date"].strftime("%m/%d/%Y %H:%M")
        lines.append(f"Date: {dt}, Open: {row['Open']}, High: {row['High']}, Low: {row['Low']}, Close: {row['Close']}")
    return "\n".join(lines)

def format_ote_as_text(df):
    if df.empty:
        return "No OTE patterns detected"
    ote_rows = df[(df['Bullish_FVG_Flag']) | (df['Bearish_FVG_Flag']) | (df['Retracement_Flag'])]
    if ote_rows.empty:
        return "No OTE patterns detected"
    lines = []
    for _, row in ote_rows.iterrows():
        dt = row["Date"].strftime("%m/%d/%Y %H:%M")
        fvg_dt = row["FVG_Date"].strftime("%m/%d/%Y %H:%M") if pd.notna(row["FVG_Date"]) else "N/A"
        retrace_dt = row["Retracement_Date"].strftime("%m/%d/%Y %H:%M") if pd.notna(row["Retracement_Date"]) else "N/A"
        lines.append(
            f"Date: {dt}, Open: {row['Open']}, High: {row['High']}, Low: {row['Low']}, Close: {row['Close']}, "
            f"FVG_Date: {fvg_dt}, Retracement_Date: {retrace_dt}, "
            f"Bullish_FVG_Flag: {row['Bullish_FVG_Flag']}, Bearish_FVG_Flag: {row['Bearish_FVG_Flag']}, Retracement_Flag: {row['Retracement_Flag']}"
        )
    return "\n".join(lines)

def upsert_snapshot(cursor, conn, label, ohlc_text, ote_text):
    cursor.execute("""
    IF EXISTS (SELECT 1 FROM dbo.TestDataset WHERE Label = ?)
        UPDATE dbo.TestDataset SET OHLCDataT = ?, OTEDataT = ? WHERE Label = ?
    ELSE
        INSERT INTO dbo.TestDataset (Label, OHLCDataT, OTEDataT) VALUES (?, ?, ?)
    """, label, ohlc_text, ote_text, label, label, ohlc_text, ote_text)
    conn.commit()
    log.info("✅ %s updated in TestDataset", label)

def clear_database():
    conn, cursor = open_db()
    cursor.execute("TRUNCATE TABLE dbo.TestDataset")
    conn.commit()
    conn.close()
    seen_retracements.clear()
    log.info("🗑️  TestDataset cleared and retracement memory reset")

def is_within_trading_hours():
    london_tz = pytz.timezone('Europe/London')
    now_london = datetime.now(london_tz)
    current_time = now_london.time()
    weekday = now_london.weekday()
    start_time = dt_time(12, 45)
    end_time = dt_time(15, 0)
    return (weekday < 5) and (start_time <= current_time <= end_time)

def run_collector():
    """Main collector loop for all symbols/timeframes."""
    retracement_hits = []

    # open connection lazily
    conn, cursor = open_db()
    log.info("✅ Connected to Azure for run_collector")

    # CREATE TABLE if needed
    cursor.execute("""
    IF OBJECT_ID('dbo.TestDataset','U') IS NULL
        CREATE TABLE dbo.TestDataset(
            [Label] NVARCHAR(100) PRIMARY KEY,
            [OHLCDataT] NVARCHAR(MAX),
            [OTEDataT] NVARCHAR(MAX)
        );
    """)
    conn.commit()

    # --- loop through symbols/timeframes ---
    for symbol in SYMBOLS:
        for tf, granularity in TIMEFRAMES.items():
            minutes = {"M15": 15, "H1": 60, "H4": 240}[tf]
            count = 8 if tf == "H4" else get_count_since_midnight(minutes)

            df = get_candles(symbol, granularity, count)
            if df.empty:
                continue

            ohlc_text = format_ohlc_as_text(df)

            try:
                df_with_ote = detect_ote(df)
                ote_text = format_ote_as_text(df_with_ote)

                retrace_rows = df_with_ote[df_with_ote["Retracement_Flag"]]
                for _, row in retrace_rows.iterrows():
                    key = (symbol, tf, str(row["FVG_Date"]))
                    if key not in seen_retracements:
                        seen_retracements.add(key)

                        # Format timestamp nicely
                        fvg_date_str = row["FVG_Date"].strftime("%Y-%m-%d %H:%M")

                        retracement_hits.append(f"🟢 {symbol}_{tf} (FVG {fvg_date_str})")
            except Exception as e:
                ote_text = "Error in OTE detection"
                log.error("❌ %s_%s error: %s", symbol, tf, e)

            upsert_snapshot(cursor, conn, f"{symbol}_{tf}", ohlc_text, ote_text)

    conn.close()        # <-- release Azure resource immediately
    log.info("🔌 Azure SQL connection closed")

    if retracement_hits:
        send_telegram_message("NEW retracements:\n" + "\n".join(retracement_hits))


def check_and_clear_database():
    global last_cleared_date
    london_tz = pytz.timezone("Europe/London")
    now = datetime.now(london_tz)
    weekday = now.weekday()
    if weekday >= 5:
        return
    if now.hour >= 15 and (last_cleared_date != now.date()):
        clear_database()
        last_cleared_date = now.date()

def start_realtime_monitor(interval=240):
    while not stop_event.is_set():
        check_and_clear_database()
        if is_within_trading_hours():
            log.info("🔄 Running retracement check...")
            run_collector()
            for _ in range(int(interval/5)):
                if stop_event.is_set():
                    break
                time.sleep(5)
        else:
            now_london = datetime.now(pytz.timezone('Europe/London')).strftime("%H:%M:%S")
            log.info("⏸ Outside trading hours. %s", now_london)
            for _ in range(120):
                if stop_event.is_set():
                    break
                time.sleep(5)
    log.info("Monitor loop exited.")

def send_telegram_message(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        requests.post(url, json=payload, timeout=10)
        log.info("📤 Telegram alert sent")
    except Exception as e:
        log.warning("⚠️ Telegram send failed: %s", e)

# ============================================================================
# === ENTRYPOINT =============================================================
# ============================================================================
if __name__ == "__main__":
    if test_api_connection():
        log.info("Starting monitor. Program will only run between 12:45 PM and 3:00 PM UK time.")
        start_realtime_monitor(interval=240)
