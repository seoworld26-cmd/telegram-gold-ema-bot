import os
import time
import requests
import pandas as pd
import yfinance as yf

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

SYMBOL = os.getenv("SYMBOL", "GC=F")
INTERVAL = os.getenv("INTERVAL", "15m")
FAST_EMA = int(os.getenv("FAST_EMA", "9"))
SLOW_EMA = int(os.getenv("SLOW_EMA", "21"))
CHECK_SECONDS = int(os.getenv("CHECK_SECONDS", "60"))

STATE_FILE = "last_signal.txt"

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is missing")
if not CHAT_ID:
    raise ValueError("CHAT_ID is missing")


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text
    }
    r = requests.post(url, data=payload, timeout=20)
    r.raise_for_status()


def get_last_saved_signal():
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return f.read().strip()


def save_last_signal(signal: str):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write(signal)


def get_gold_data():
    df = yf.download(
        tickers=SYMBOL,
        period="5d",
        interval=INTERVAL,
        progress=False,
        auto_adjust=False
    )

    if df is None or df.empty:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    return df


def detect_signal(df: pd.DataFrame):
    if len(df) < SLOW_EMA + 3:
        return None

    close = df["Close"].copy()
    df["ema_fast"] = close.ewm(span=FAST_EMA, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=SLOW_EMA, adjust=False).mean()

    prev_fast = df["ema_fast"].iloc[-2]
    prev_slow = df["ema_slow"].iloc[-2]
    curr_fast = df["ema_fast"].iloc[-1]
    curr_slow = df["ema_slow"].iloc[-1]
    last_close = df["Close"].iloc[-1]

    if prev_fast <= prev_slow and curr_fast > curr_slow:
        return f"GOLD BUY SIGNAL\nEMA {FAST_EMA} crossed above EMA {SLOW_EMA}\nPrice: {last_close}"
    elif prev_fast >= prev_slow and curr_fast < curr_slow:
        return f"GOLD SELL SIGNAL\nEMA {FAST_EMA} crossed below EMA {SLOW_EMA}\nPrice: {last_close}"

    return None


def main():
    print("Bot started...")
    while True:
        try:
            df = get_gold_data()
            if df is not None:
                signal = detect_signal(df)
                last_saved = get_last_saved_signal()

                if signal and signal != last_saved:
                    send_telegram_message(signal)
                    save_last_signal(signal)
                    print("Signal sent:", signal)
                else:
                    print("No new signal.")
            else:
                print("No data returned.")

        except Exception as e:
            print("Error:", str(e))

        time.sleep(CHECK_SECONDS)


if __name__ == "__main__":
    main()
