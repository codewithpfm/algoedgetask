"""
Script A -- Strategy: EMA 8 / VWMA 20 crossover.  (Task 1)

    strategy.py (A)  --orders-->   adapter.py (B)  --orders-->   broker.py (C)
    strategy.py (A)  <--updates--  adapter.py (B)  <--updates--  broker.py (C)

Rules
    BUY  when EMA crosses above VWMA  (closes an open short first)
    SELL when EMA crosses below VWMA  (closes an open long first)
    While in a position, exit on whichever comes first:
        stop loss   : the tighter of  entry -/+ ATR * STOP_LOSS_MULTIPLIER
                                 and  entry -/+ HARD_STOP_LOSS_PERCENT %
        take profit : entry +/- ATR * TAKE_PROFIT_MULTIPLIER


Open three terminals in this folder and start them in this order, because the
broker and the adapter bind the ports the strategy connects to:

    1.  python broker.py          # Script C, the mock broker
    2.  python adapter.py         # Script B, the bridge
    3.  python strategy.py        # Script A, this file

The third terminal runs the demo: five orders, both buys and sells, printing
realised PnL, the order book and open positions after every order and once more
at the end. 
Terminal 2 shows each order on its way out and each fill on its way
back, so you can see both hops. 
Terminal 1 shows what the broker did with them.
Stop the broker and the adapter with Ctrl+C when you are finished.
"""
import argparse
import logging
import re
import time
from typing import Literal, Optional

import numpy as np
import pandas as pd

from adapter import AdapterClient, Order, Portfolio, TERMINAL
from config import (
    TAKE_PROFIT_MULTIPLIER,
    STOP_LOSS_MULTIPLIER,
    HARD_STOP_LOSS_PERCENT,
    EMA,
    VWMA,
    SIGNAL_TF,
    ATR_PERIOD,
)

logger = logging.getLogger("strategy")

FAST_MA = f"EMA_{EMA}"         # EMA 8
SLOW_MA = f"VWMA_{VWMA}"       # VWMA 20
ATR_COL = f"ATR_{ATR_PERIOD}"
CANDLE_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]

# =============================================================================
# Indicators
# =============================================================================
def ema(close: pd.Series, length: int) -> pd.Series:
    return close.ewm(span=length, adjust=False, min_periods=length).mean()


def vwma(close: pd.Series, volume: pd.Series, length: int) -> pd.Series:
    volume_sum = volume.rolling(length).sum().replace(0, np.nan)
    return (close * volume).rolling(length).sum() / volume_sum


def atr(data: pd.DataFrame, length: int) -> pd.Series:
    """Wilder's Average True Range."""
    prev_close = data["Close"].shift(1)
    true_range = pd.concat(
        [
            data["High"] - data["Low"],
            (data["High"] - prev_close).abs(),
            (data["Low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()


# =============================================================================
# Signals
# =============================================================================
class Signals:

    def __init__(self, data: pd.DataFrame):
        self.data = data.sort_index().copy()
        self.add_indicators()

    def add_indicators(self) -> None:
        close, volume = self.data["Close"], self.data["Volume"]
        self.data[FAST_MA] = ema(close, EMA)
        self.data[SLOW_MA] = vwma(close, volume, VWMA)
        self.data[ATR_COL] = atr(self.data, ATR_PERIOD)

    def check_crossover(
        self, fast_average: str, slow_average: str, direction: Literal["above", "below"]
    ) -> pd.Series:
        if not direction:
            raise ValueError("Direction is required")

        if direction == "above":
            return np.logical_and(
                (self.data[fast_average].shift(1) < self.data[slow_average].shift(1)),
                (self.data[fast_average] > self.data[slow_average]),
            )

        if direction == "below":
            return np.logical_and(
                (self.data[fast_average].shift(1) > self.data[slow_average].shift(1)),
                (self.data[fast_average] < self.data[slow_average]),
            )

        raise ValueError("Direction must be 'above' or 'below'")

    def get_long_entries(self) -> pd.Series:
        return self.check_crossover(FAST_MA, SLOW_MA, "above")

    def get_short_entries(self) -> pd.Series:
        return self.check_crossover(FAST_MA, SLOW_MA, "below")

    @staticmethod
    def exit_levels(side: Literal["LONG", "SHORT"], entry_price: float, atr_value: float) -> dict:
        """Stop and take-profit prices for a position opened at entry_price."""
        hard = HARD_STOP_LOSS_PERCENT / 100
        if side == "LONG":
            atr_stop = entry_price - atr_value * STOP_LOSS_MULTIPLIER
            hard_stop = entry_price * (1 - hard)
            stop = max(atr_stop, hard_stop)
            take_profit = entry_price + atr_value * TAKE_PROFIT_MULTIPLIER
        else:
            atr_stop = entry_price + atr_value * STOP_LOSS_MULTIPLIER
            hard_stop = entry_price * (1 + hard)
            stop = min(atr_stop, hard_stop)
            take_profit = entry_price - atr_value * TAKE_PROFIT_MULTIPLIER
        return {
            "side": side,
            "stop": stop,
            "stop_reason": "atr_stop" if stop == atr_stop else "hard_stop",
            "take_profit": take_profit,
        }

    @staticmethod
    def check_exit(levels: dict, bar: pd.Series) -> Optional[tuple[float, str]]:
        """(exit_price, reason) if this candle hits the stop or the target, else None.
        The stop is checked first (conservative). A gap through a level fills at the open."""
        o, h, l = float(bar["Open"]), float(bar["High"]), float(bar["Low"])
        stop, take_profit = levels["stop"], levels["take_profit"]
        if levels["side"] == "LONG":
            if l <= stop:
                return min(o, stop), levels["stop_reason"]
            if h >= take_profit:
                return max(o, take_profit), "take_profit"
        else:
            if h >= stop:
                return max(o, stop), levels["stop_reason"]
            if l <= take_profit:
                return min(o, take_profit), "take_profit"
        return None



class Strategy:

    def __init__(self, signals: Signals, client: AdapterClient, book: Portfolio,
                 symbol: str, qty: float, max_orders: int, fill_timeout: float, delay: float):
        self.signals = signals
        self.client = client
        self.book = book
        self.symbol = symbol
        self.qty = qty
        self.max_orders = max_orders
        self.fill_timeout = fill_timeout
        self.delay = delay
        self.levels: Optional[dict] = None
        self.orders_sent = 0
        self.updates_seen = 0

    @property
    def position(self) -> float:
        return self.book.position_qty(self.symbol)

    def run(self) -> None:
        data = self.signals.data
        long_entries = self.signals.get_long_entries()
        short_entries = self.signals.get_short_entries()
        logger.info("%d candles | %d long crossovers | %d short crossovers",
                    len(data), int(long_entries.sum()), int(short_entries.sum()))

        for ts, bar in data.iterrows():
            if self.orders_sent >= self.max_orders:
                break
            self.book.mark(self.symbol, float(bar["Close"]))

            # 1. stop or target on this candle, for a position opened earlier
            if self.levels is not None and self.position != 0:
                hit = Signals.check_exit(self.levels, bar)
                if hit is not None:
                    self.close_position(ts, *hit)

            # 2. crossover at this candle's close
            if long_entries[ts]:
                self.enter("LONG", ts, bar)
            elif short_entries[ts]:
                self.enter("SHORT", ts, bar)

        if self.orders_sent < self.max_orders:
            logger.warning("ran out of candles after %d of %d orders", self.orders_sent, self.max_orders)

    def enter(self, side: Literal["LONG", "SHORT"], ts, bar: pd.Series) -> None:
        want = 1 if side == "LONG" else -1
        if self.position * want > 0:
            return                                   # already positioned this way

        price = float(bar["Close"])
        if self.position != 0:
            self.close_position(ts, price, "reverse_signal")
            if self.position != 0:
                return                               # close did not fill, do not stack the opposite side

        atr_value = float(bar[ATR_COL])
        if not np.isfinite(atr_value):
            logger.info("candle %s | %s crossover skipped, ATR still warming up", ts, side)
            return

        order = self.place(ts, "BUY" if want > 0 else "SELL", self.qty, price, f"ENTRY_{side}", "crossover")
        if order is not None and order.filled_qty > 0:
            self.levels = Signals.exit_levels(side, order.avg_fill_price, atr_value)
            logger.info("%s levels | stop %.4f (%s) | take profit %.4f",
                        side, self.levels["stop"], self.levels["stop_reason"], self.levels["take_profit"])

    def close_position(self, ts, price: float, reason: str) -> None:
        pos = self.position
        if pos == 0:
            self.levels = None
            return
        side, tag = ("SELL", "EXIT_LONG") if pos > 0 else ("BUY", "EXIT_SHORT")
        self.place(ts, side, abs(pos), price, tag, reason)
        if self.position == 0:
            self.levels = None    # if the exit was rejected the levels stay, and it retries next candle

    def place(self, ts, side: str, qty: float, price: float, tag: str, reason: str) -> Optional[Order]:
        if self.orders_sent >= self.max_orders:
            return None
        order = self.book.create_order(self.symbol, side, qty, price, tag, reason)
        self.orders_sent += 1
        logger.info("candle %s | %s %s %g @ %.4f (%s) -> %s", ts, tag, side, qty, price, reason, order.order_id)

        if self.client.send_order(order):
            self.await_result(order)
        else:
            order.status = "SEND_FAILED"

        print(self.book.dashboard(f"order {self.orders_sent}/{self.max_orders} | candle {ts}"), flush=True)
        if self.delay > 0:
            time.sleep(self.delay)
        return order

    def await_result(self, order: Order) -> None:
        """Block until the broker settles this order, or the timeout runs out."""
        deadline = time.monotonic() + self.fill_timeout
        while order.status not in TERMINAL and time.monotonic() < deadline:
            for msg in self.client.poll_updates(200):
                self.updates_seen += 1
                updated = self.book.on_update(msg)
                if updated is not None:
                    logger.info("UPDATE %s -> %s", updated.order_id, updated.status)
        if order.status in TERMINAL:
            return

        logger.warning("no final update for %s within %.1fs (status %s)",
                       order.order_id, self.fill_timeout, order.status)
        if self.updates_seen == 0:
            # A PUSH socket happily queues orders with nothing on the other end,
            # so silence here almost always means the other two scripts are not up.
            raise ConnectionError(
                f"no reply from the adapter on {self.client.update_endpoint}. "
                f"Nothing will fill, so there is no PnL to show. "
                f"Start the other two scripts first: 'python broker.py', then 'python adapter.py'."
            )


# =============================================================================
# Candles: the strategy's input. Anything finer than SIGNAL_TF is resampled up.
# =============================================================================
def tf_to_pandas_freq(timeframe: str) -> str:
    """'5M' -> '5min', '1H' -> '1h', '1D' -> '1D'."""
    match = re.fullmatch(r"\s*(\d+)\s*([MHD])\s*", timeframe.upper())
    if not match:
        raise ValueError(f"unsupported timeframe {timeframe!r}")
    return match.group(1) + {"M": "min", "H": "h", "D": "D"}[match.group(2)]


def load_candles(path: str) -> pd.DataFrame:
    """Read candles from a .parquet or .csv file, at whatever timeframe they are stored."""
    if path.lower().endswith((".parquet", ".pq")):
        raw = pd.read_parquet(path)
        if not isinstance(raw.index, pd.DatetimeIndex):
            raw = raw.set_index(next(c for c in raw.columns if "time" in c.lower() or "date" in c.lower()))
    else:
        raw = pd.read_csv(path)
        time_col = next((c for c in raw.columns if c.strip().lower() in
                         ("timestamp", "datetime", "date", "time")), None)
        if time_col is None:
            raise ValueError(f"{path}: needs a timestamp/datetime/date/time column")
        raw = raw.set_index(time_col)

    raw.index = pd.to_datetime(raw.index)
    # match Open/High/Low/Close/Volume however the file capitalised them
    by_lower = {str(c).strip().lower(): c for c in raw.columns}
    missing = [c for c in CANDLE_COLUMNS if c.lower() not in by_lower]
    if missing:
        raise ValueError(f"{path}: missing columns {missing}, found {list(raw.columns)}")
    data = raw[[by_lower[c.lower()] for c in CANDLE_COLUMNS]]
    data.columns = CANDLE_COLUMNS
    return data.astype(float).sort_index()

#extra function to test out on real market data data fetched in 1min -> resample it to 5 mins 
def resample_candles(data: pd.DataFrame, timeframe: str = SIGNAL_TF) -> pd.DataFrame:
    """Aggregate finer candles up to `timeframe`. Already-coarse data is returned as is."""
    target = pd.Timedelta(tf_to_pandas_freq(timeframe))
    native = data.index.to_series().diff().median()
    if pd.isna(native) or native >= target:
        return data

    out = data.resample(tf_to_pandas_freq(timeframe), label="left", closed="left").agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    ).dropna(subset=["Open"])
    logger.info("resampled %d candles of %s up to %d candles of %s",
                len(data), native, len(out), timeframe)
    return out


def make_demo_candles(bars: int = 2000, seed: int = 7, start_price: float = 100.0,
                      start: str = "2026-01-05 09:15") -> pd.DataFrame:
    """Random walk with slow trend waves, so EMA/VWMA crossovers actually happen."""
    rng = np.random.default_rng(seed)
    t = np.arange(bars)
    returns = rng.normal(0, 0.0015, bars) + 0.0004 * np.sin(2 * np.pi * t / 150)
    close = start_price * np.exp(np.cumsum(returns))
    open_ = np.concatenate([[start_price], close[:-1]])
    wick = np.abs(rng.normal(0, 0.0008, bars))
    return pd.DataFrame(
        {
            "Open": open_,
            "High": np.maximum(open_, close) * (1 + wick),
            "Low": np.minimum(open_, close) * (1 - wick),
            "Close": close,
            "Volume": rng.lognormal(8, 0.5, bars),
        },
        index=pd.date_range(start, periods=bars, freq=tf_to_pandas_freq(SIGNAL_TF)),
    )


# =============================================================================
# Entry point
# =============================================================================
def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Script A: EMA/VWMA crossover strategy")
    parser.add_argument("--data", "--csv", dest="data",
                        help="candles from a .parquet or .csv file; anything finer than SIGNAL_TF is resampled "
                             "(default: synthetic candles)")
    parser.add_argument("--bars", type=int, default=2000, help="synthetic candles to generate (default 2000)")
    parser.add_argument("--seed", type=int, default=7, help="synthetic data seed (default 7)")
    parser.add_argument("--symbol", default="DEMO")
    parser.add_argument("--qty", type=float, default=1.0)
    parser.add_argument("--max-orders", type=int, default=5, help="stop after this many orders (default 5)")
    parser.add_argument("--timeout", type=float, default=5.0, help="seconds to wait for a fill or reject (default 5)")
    parser.add_argument("--delay", type=float, default=0.5, help="pause after each order in seconds (default 0.5)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s [A strategy] %(message)s", datefmt="%H:%M:%S")

    if args.data:
        candles = resample_candles(load_candles(args.data))
        logger.info("%s | %d candles of %s | %s -> %s", args.data, len(candles), SIGNAL_TF,
                    candles.index[0], candles.index[-1])
    else:
        candles = make_demo_candles(args.bars, args.seed)
    book = Portfolio(owner="STRATEGY A", id_prefix="A")
    client = AdapterClient()

    strategy = Strategy(Signals(candles), client, book, args.symbol, args.qty,
                        args.max_orders, args.timeout, args.delay)
    try:
        strategy.run()
        for msg in client.poll_updates(500):         # anything still in flight
            book.on_update(msg)
    except ConnectionError as exc:
        logger.error("%s", exc)
    except KeyboardInterrupt:
        logger.info("stopped by user")
    finally:
        client.close()
        print(book.dashboard("FINAL"), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
