"""
Run:  python api_threading.py     then, in another terminal:  python client.py
"""
import itertools, threading, time

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

MODE, PORT, LATENCY = "threading", 8000, 0.3

app = FastAPI(title=f"order api ({MODE})")
lock = threading.Lock()
orders, next_id, prices = {}, itertools.count(1), {}


class NewOrder(BaseModel):
    symbol: str
    side: str
    qty: float


def price_of(symbol: str) -> float:
    return prices.setdefault(symbol.upper(), 100.0)


def tick() -> None:
    """Background thread: drift every price once a second."""
    while True:
        time.sleep(1)
        with lock:
            prices.update({s: round(p * 1.001, 4) for s, p in prices.items()})


@app.get("/health")
def health():
    return {"mode": MODE, "orders": len(orders)}


@app.get("/price/{symbol}")
def price(symbol: str):
    time.sleep(LATENCY)                       # blocking I/O, off the event loop
    with lock:
        return {"symbol": symbol.upper(), "price": price_of(symbol)}


@app.post("/orders")
def place(new: NewOrder):
    if new.side.upper() not in ("BUY", "SELL") or new.qty <= 0:
        raise HTTPException(422, "side must be BUY or SELL, qty must be positive")
    time.sleep(LATENCY)                       # pretend the broker took a moment
    with lock:
        order = {"order_id": f"T-{next(next_id):03d}", "symbol": new.symbol.upper(),
                 "side": new.side.upper(), "qty": new.qty,
                 "price": price_of(new.symbol), "status": "FILLED"}
        orders[order["order_id"]] = order
        return order


@app.get("/orders")
def book():
    with lock:
        return list(orders.values())


if __name__ == "__main__":
    threading.Thread(target=tick, daemon=True).start()
    uvicorn.run(app, host="127.0.0.1", port=PORT)
