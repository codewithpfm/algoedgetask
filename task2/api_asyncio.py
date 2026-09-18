"""
Run:  python api_asyncio.py     then, in another terminal:  python client.py --port 8001
"""
import asyncio, itertools
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

MODE, PORT, LATENCY = "asyncio", 8001, 0.3

orders, next_id, prices = {}, itertools.count(1), {}


class NewOrder(BaseModel):
    symbol: str
    side: str
    qty: float


def price_of(symbol: str) -> float:
    return prices.setdefault(symbol.upper(), 100.0)


async def tick() -> None:
    """Background task: drift every price once a second."""
    while True:
        await asyncio.sleep(1)
        prices.update({s: round(p * 1.001, 4) for s, p in prices.items()})


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(tick())
    yield
    task.cancel()


app = FastAPI(title=f"order api ({MODE})", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"mode": MODE, "orders": len(orders)}


@app.get("/price/{symbol}")
async def price(symbol: str):
    await asyncio.sleep(LATENCY)              # yields the loop to other requests
    return {"symbol": symbol.upper(), "price": price_of(symbol)}


@app.post("/orders")
async def place(new: NewOrder):
    if new.side.upper() not in ("BUY", "SELL") or new.qty <= 0:
        raise HTTPException(422, "side must be BUY or SELL, qty must be positive")
    await asyncio.sleep(LATENCY)              # pretend the broker took a moment
    order = {"order_id": f"A-{next(next_id):03d}", "symbol": new.symbol.upper(),
             "side": new.side.upper(), "qty": new.qty,
             "price": price_of(new.symbol), "status": "FILLED"}
    orders[order["order_id"]] = order
    return order


@app.get("/orders")
async def book():
    return list(orders.values())


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT)
