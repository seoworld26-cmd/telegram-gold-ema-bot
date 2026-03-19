import os
import time
import json
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

STATE_FILE = "signal_state.json"

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is missing")
if not CHAT_ID:
    raise ValueError("CHAT_ID is missing")


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    r = requests.post(url, data=payload, timeout=20)
    r.raise_for_status()


def load_state():
    if not os.path.exists(STATE_FILE):
        return {"last_signal": None, "last_candle_time": None}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"last_signal": None, "last_candle_time": None}


def save_state(signal_type, candle_time):
    state = {
        "last_signal": signal_type,
        "last_candle_time": candle_time
    }
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


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
            print("Yahoo rate limit hit. Waiting for next cycle...")
        else:
            print("Download error:", err)
        return None


def detect_crossover(df: pd.DataFrame):
    if len(df) < SLOW_EMA + 3:
        return None

    close = df["Close"].copy()
    df["ema_fast"] = close.ewm(span=FAST_EMA, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=SLOW_EMA, adjust=False).mean()

    # Use only CLOSED candles
    prev_fast = df["ema_fast"].iloc[-3]
    prev_slow = df["ema_slow"].iloc[-3]
    curr_fast = df["ema_fast"].iloc[-2]
    curr_slow = df["ema_slow"].iloc[-2]

    signal_price = float(df["Close"].iloc[-2])
    candle_time = str(df.index[-2])

    if prev_fast <= prev_slow and curr_fast > curr_slow:
        return {
            "type": "BUY",
            "price": signal_price,
            "time": candle_time
        }

    if prev_fast >= prev_slow and curr_fast < curr_slow:
        return {
            "type": "SELL",
            "price": signal_price,
            "time": candle_time
        }

    return None


def build_message(signal):
    return (
        f"GOLD {signal['type']} SIGNAL\n"
        f"EMA {FAST_EMA} crossed "
        f"{'above' if signal['type']=='BUY' else 'below'} EMA {SLOW_EMA}\n"
        f"Price: {signal['price']}\n"
        f"Time: {signal['time']}"
    )


def main():
    print("Bot started and monitoring EMA crossover...")

    while True:
        try:
            df = get_gold_data()
            if df is None:
                time.sleep(CHECK_SECONDS)
                continue

            signal = detect_crossover(df)
            state = load_state()

            if signal is None:
                print("No new crossover signal.")
                time.sleep(CHECK_SECONDS)
                continue

            # Prevent duplicate alert for same signal candle
            if (
                state["last_signal"] == signal["type"]
                and state["last_candle_time"] == signal["time"]
            ):
                print("Duplicate crossover already sent. Skipping.")
                time.sleep(CHECK_SECONDS)
                continue

            message = build_message(signal)
            send_telegram_message(message)
            save_state(signal["type"], signal["time"])

            print("Signal sent:", message)

        except Exception as e:
            print("Main loop error:", str(e))

        time.sleep(CHECK_SECONDS)


if __name__ == "__main__":
    main()
    # update
