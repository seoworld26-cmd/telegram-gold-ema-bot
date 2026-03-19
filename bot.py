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
CHECK_SECONDS = int(os.getenv("CHECK_SECONDS", "900"))

STATE_FILE = "last_signal.txt"
LAST_CANDLE_FILE = "last_candle.txt"

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is missing")
if not CHAT_ID:
    raise ValueError("CHAT_ID is missing")


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    r = requests.post(url, data=payload, timeout=20)
    r.raise_for_status()


def read_file(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def write_file(path, value):
    with open(path, "w", encoding="utf-8") as f:
        f.write(str(value))


def get_gold_data():
    try:
        df = yf.download(
            tickers=SYMBOL,
            period="5d",
            interval=INTERVAL,
            progress=False,
            auto_adjust=False,
            threads=False
        )

        if df is None or df.empty:
            print("No data returned from Yahoo.")
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        return df

    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate limited" in err:
            print("Yahoo rate limit hit. Waiting longer before retry...")
        else:
            print("Download error:", err)
        return None


def detect_signal(df: pd.DataFrame):
    if len(df) < SLOW_EMA + 3:
        return None, None

    close = df["Close"].copy()
    df["ema_fast"] = close.ewm(span=FAST_EMA, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=SLOW_EMA, adjust=False).mean()

    # Use last CLOSED candle
    prev_fast = df["ema_fast"].iloc[-3]
    prev_slow = df["ema_slow"].iloc[-3]
    curr_fast = df["ema_fast"].iloc[-2]
    curr_slow = df["ema_slow"].iloc[-2]
    signal_price = df["Close"].iloc[-2]
    candle_time = str(df.index[-2])

    if prev_fast <= prev_slow and curr_fast > curr_slow:
        signal = (
            f"GOLD BUY SIGNAL\n"
            f"EMA {FAST_EMA} crossed above EMA {SLOW_EMA}\n"
            f"Price: {signal_price}\n"
            f"Time: {candle_time}"
        )
        return signal, candle_time

    if prev_fast >= prev_slow and curr_fast < curr_slow:
        signal = (
            f"GOLD SELL SIGNAL\n"
            f"EMA {FAST_EMA} crossed below EMA {SLOW_EMA}\n"
            f"Price: {signal_price}\n"
            f"Time: {candle_time}"
        )
        return signal, candle_time

    return None, candle_time


def main():
    print("Bot started and monitoring EMA crossover...")

    while True:
        try:
            df = get_gold_data()

            if df is None:
                time.sleep(CHECK_SECONDS)
                continue

            signal, candle_time = detect_signal(df)

            last_signal = read_file(STATE_FILE)
            last_candle = read_file(LAST_CANDLE_FILE)

            if candle_time and candle_time != last_candle:
                write_file(LAST_CANDLE_FILE, candle_time)

                if signal and signal != last_signal:
                    send_telegram_message(signal)
                    write_file(STATE_FILE, signal)
                    print("Signal sent:", signal)
                else:
                    print("New candle checked. No crossover signal.")
            else:
                print("No new closed candle yet.")

        except Exception as e:
            print("Main loop error:", str(e))

        time.sleep(CHECK_SECONDS)


if __name__ == "__main__":
    main()
    # update
