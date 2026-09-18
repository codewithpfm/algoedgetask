"""
Run:  python client.py              # threading version on port 8000
      python client.py --port 8001  # asyncio version
"""
import argparse, asyncio, time

import httpx

SYMBOLS = ["DEMO", "ACME", "ZEST", "NOVA", "ORBIT"]
ORDERS = [("DEMO", "BUY", 10), ("ACME", "SELL", 5), ("ZEST", "BUY", 8),
          ("NOVA", "SELL", 3), ("DEMO", "SELL", 10)]


async def main(port: int) -> None:
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=30) as api:
        print("GET /health ->", (await api.get("/health")).json())

        started = time.perf_counter()
        quotes = await asyncio.gather(*(api.get(f"/price/{s}") for s in SYMBOLS))
        print(f"\nGET /price x{len(SYMBOLS)} in parallel, {time.perf_counter() - started:.2f}s")
        for q in quotes:
            print("   ", q.json())

        print("\nPOST /orders")
        for symbol, side, qty in ORDERS:
            reply = await api.post("/orders", json={"symbol": symbol, "side": side, "qty": qty})
            print("   ", reply.json())

        bad = await api.post("/orders", json={"symbol": "DEMO", "side": "HOLD", "qty": 1})
        print("    rejected ->", bad.status_code, bad.json()["detail"])

        print("\nGET /orders ->", len((await api.get("/orders")).json()), "orders on the book")
        print("GET /health ->", (await api.get("/health")).json())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script B: REST client for the Script A API")
    parser.add_argument("--port", type=int, default=8000, help="8000 threading, 8001 asyncio")
    asyncio.run(main(parser.parse_args().port))
