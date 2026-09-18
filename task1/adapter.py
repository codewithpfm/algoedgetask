import argparse
import json
import logging
import time
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Literal, Optional

import numpy as np
import pandas as pd
import zmq

from config import (
    ORDER_ENDPOINT,
    UPDATE_ENDPOINT,
    BROKER_ORDER_ENDPOINT,
    BROKER_UPDATE_ENDPOINT,
)

logger = logging.getLogger("adapter")


# =============================================================================
# Wire protocol, shared by all three scripts
# =============================================================================
ACKED = "ACKED"
PARTIAL = "PARTIAL"
FILLED = "FILLED"
REJECTED = "REJECTED"
CANCELLED = "CANCELLED"
TERMINAL = {FILLED, REJECTED, CANCELLED}

STATUS_ALIASES = {
    "ACK": ACKED, "ACKED": ACKED, "ACCEPTED": ACKED, "NEW": ACKED, "OPEN": ACKED,
    "PARTIAL": PARTIAL, "PARTIAL_FILL": PARTIAL, "PARTIALLY_FILLED": PARTIAL,
    "FILL": FILLED, "FILLED": FILLED, "COMPLETE": FILLED, "EXECUTED": FILLED,
    "REJECT": REJECTED, "REJECTED": REJECTED,
    "CANCEL": CANCELLED, "CANCELLED": CANCELLED, "CANCELED": CANCELLED,
}

# broker execType -> strategy status
EXEC_TYPES = {
    "ACK": ACKED,
    "PARTIAL_FILL": PARTIAL,
    "FILL": FILLED,
    "REJECT": REJECTED,
    "CANCEL": CANCELLED,
}


def normalise_status(raw) -> Optional[str]:
    return STATUS_ALIASES.get(str(raw or "").upper().replace(" ", "_"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def update_message(order_id: str, status: str, fill_qty: float = 0.0,
                   fill_price: Optional[float] = None, reason: str = "") -> dict:
    msg = {"type": "update", "order_id": order_id, "ts": now_iso(), "status": status}
    if fill_qty:
        msg["fill_qty"] = float(fill_qty)
        msg["fill_price"] = float(fill_price) if fill_price is not None else None
    if reason:
        msg["reason"] = reason
    return msg


def socket(ctx: zmq.Context, kind: int, endpoint: str, bind: bool, **options) -> zmq.Socket:
    sock = ctx.socket(kind)
    sock.setsockopt(zmq.LINGER, 0)
    for name, value in options.items():
        sock.setsockopt(getattr(zmq, name.upper()), value)
    if kind == zmq.SUB:
        sock.setsockopt_string(zmq.SUBSCRIBE, "")
    sock.bind(endpoint) if bind else sock.connect(endpoint)
    return sock


def recv_json(sock: zmq.Socket) -> Optional[dict]:
    """Read one message. Accepts [json] and [topic, json] frames."""
    frames = sock.recv_multipart()
    try:
        msg = json.loads(frames[-1])
    except ValueError:
        logger.warning("dropping unparseable frame: %r", frames)
        return None
    return msg if isinstance(msg, dict) else None


def publish(sock: zmq.Socket, msg: dict, topic: str = "") -> None:
    """PUB with a topic frame, so subscribers can filter by order id if they want."""
    sock.send_multipart([topic.encode(), json.dumps(msg).encode()])


@dataclass
class Order:
    order_id: str
    symbol: str
    side: Literal["BUY", "SELL"]
    qty: float
    price: float                     # reference price when the order was created
    tag: str = ""                    # ENTRY_LONG / EXIT_LONG / ...
    reason: str = ""                 # crossover / atr_stop / take_profit / ...
    order_type: str = "MARKET"
    ts: str = field(default_factory=now_iso)
    status: str = "NEW"
    filled_qty: float = 0.0
    avg_fill_price: float = float("nan")
    note: str = ""

    @property
    def remaining(self) -> float:
        return self.qty - self.filled_qty

    def to_message(self) -> dict:
        return {
            "type": "order",
            "order_id": self.order_id,
            "ts": self.ts,
            "symbol": self.symbol,
            "side": self.side,
            "qty": self.qty,
            "order_type": self.order_type,
            "price": self.price,
            "tag": self.tag,
            "reason": self.reason,
        }


@dataclass
class Position:
    qty: float = 0.0                 # signed: + long, - short
    avg_price: float = 0.0
    realised_pnl: float = 0.0


class Portfolio:
    """Orders, positions and realised PnL for one participant."""

    def __init__(self, owner: str = "STRATEGY A", id_prefix: str = "A") -> None:
        self.owner = owner
        self.id_prefix = id_prefix
        self.orders: dict[str, Order] = {}
        self.positions: dict[str, Position] = {}
        self.last_price: dict[str, float] = {}
        self._seq = 0

    # ---- orders ----
    def create_order(self, symbol: str, side: str, qty: float, price: float,
                     tag: str = "", reason: str = "") -> Order:
        self._seq += 1
        order = Order(
            order_id=f"{self.id_prefix}-{self._seq:04d}-{uuid.uuid4().hex[:6]}",
            symbol=symbol, side=side, qty=float(qty), price=float(price), tag=tag, reason=reason,
        )
        self.orders[order.order_id] = order
        return order

    def add_order(self, order: Order) -> Order:
        self.orders[order.order_id] = order
        return order

    def position_qty(self, symbol: str) -> float:
        return self.positions.get(symbol, Position()).qty

    def mark(self, symbol: str, price: float) -> None:
        """Latest traded or observed price, used for unrealised PnL."""
        self.last_price[symbol] = float(price)

    # ---- fills ----
    def on_update(self, msg: dict) -> Optional[Order]:
        """Apply an update message from the adapter. Returns the order it touched."""
        order = self.orders.get(msg.get("order_id"))
        if order is None:
            logger.debug("ignoring update for unknown order: %s", msg)
            return None
        status = normalise_status(msg.get("status"))
        if status is None:
            logger.warning("unknown status in update: %s", msg)
            return order

        fill_qty = msg.get("fill_qty")
        if status == FILLED and not fill_qty:
            fill_qty = order.remaining          # "filled" with no quantity means the rest
        if fill_qty and float(fill_qty) > 0 and order.remaining > 0:
            price = msg.get("fill_price")
            self.apply_fill(order, min(float(fill_qty), order.remaining),
                            float(price) if price is not None else order.price)

        if order.filled_qty >= order.qty - 1e-9:
            order.status = FILLED
        elif status in (REJECTED, CANCELLED):
            order.status = status
        elif order.filled_qty > 0:
            order.status = PARTIAL
        else:
            order.status = status
        if msg.get("reason"):
            order.note = str(msg["reason"])
        return order

    def apply_fill(self, order: Order, qty: float, price: float) -> None:
        """Book a fill against the order and the position (average cost)."""
        done = order.filled_qty
        order.avg_fill_price = price if done == 0 else (order.avg_fill_price * done + price * qty) / (done + qty)
        order.filled_qty = done + qty

        pos = self.positions.setdefault(order.symbol, Position())
        signed = qty if order.side == "BUY" else -qty
        if pos.qty == 0 or (pos.qty > 0) == (signed > 0):
            # opening or adding: average the entry price
            pos.avg_price = (abs(pos.qty) * pos.avg_price + qty * price) / (abs(pos.qty) + qty)
            pos.qty += signed
        else:
            # reducing or closing: realise PnL on the part that was closed
            closed = min(abs(pos.qty), qty)
            direction = 1 if pos.qty > 0 else -1
            pos.realised_pnl += (price - pos.avg_price) * closed * direction
            pos.qty += signed
            if abs(pos.qty) < 1e-9:
                pos.qty, pos.avg_price = 0.0, 0.0
            elif (pos.qty > 0) == (signed > 0):
                pos.avg_price = price           # flipped through zero
        self.mark(order.symbol, price)
        logger.info("FILL %s %s %g @ %.4f | position %+g | realised %.2f",
                    order.order_id, order.side, qty, price, pos.qty, pos.realised_pnl)

    # ---- reporting ----
    @property
    def realised_pnl(self) -> float:
        return sum(p.realised_pnl for p in self.positions.values())

    def unrealised_pnl(self) -> float:
        return sum(
            (self.last_price[s] - p.avg_price) * p.qty
            for s, p in self.positions.items()
            if p.qty != 0 and s in self.last_price
        )

    def order_book(self) -> pd.DataFrame:
        return pd.DataFrame(
            [asdict(o) for o in self.orders.values()],
            columns=["order_id", "symbol", "side", "qty", "price", "tag", "reason",
                     "status", "filled_qty", "avg_fill_price", "note"],
        )

    def open_positions(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "symbol": s,
                    "side": "LONG" if p.qty > 0 else "SHORT",
                    "qty": abs(p.qty),
                    "avg_price": p.avg_price,
                    "last_price": self.last_price.get(s, np.nan),
                    "unrealised_pnl": (self.last_price.get(s, np.nan) - p.avg_price) * p.qty,
                }
                for s, p in self.positions.items()
                if p.qty != 0
            ],
            columns=["symbol", "side", "qty", "avg_price", "last_price", "unrealised_pnl"],
        )

    def dashboard(self, title: str = "") -> str:
        book, positions = self.order_book(), self.open_positions()
        fmt = lambda x: f"{x:.4f}"
        filled = sum(o.status == FILLED for o in self.orders.values())
        return "\n".join([
            "=" * 100,
            f" {self.owner} | {title}",
            f" Realised PnL: {self.realised_pnl:,.2f}   Unrealised PnL: {self.unrealised_pnl():,.2f}"
            f"   Orders: {len(self.orders)} ({filled} filled)",
            "-" * 100,
            " ORDER BOOK",
            book.to_string(index=False, float_format=fmt) if not book.empty else "   (no orders)",
            "-" * 100,
            " OPEN POSITIONS",
            positions.to_string(index=False, float_format=fmt) if not positions.empty else "   (flat)",
            "=" * 100,
        ])


# =============================================================================
# Strategy side of the link: what Script A uses to reach the adapter
# =============================================================================
class AdapterClient:
    """Sends orders to the adapter and receives updates back."""

    def __init__(self, order_endpoint: str = ORDER_ENDPOINT,
                 update_endpoint: str = UPDATE_ENDPOINT, send_timeout_ms: int = 2000) -> None:
        ctx = zmq.Context.instance()
        self.order_endpoint = order_endpoint
        self.update_endpoint = update_endpoint
        self.orders = socket(ctx, zmq.PUSH, order_endpoint, bind=False, sndtimeo=send_timeout_ms)
        self.updates = socket(ctx, zmq.SUB, update_endpoint, bind=False)
        self.poller = zmq.Poller()
        self.poller.register(self.updates, zmq.POLLIN)
        logger.info("orders -> %s | updates <- %s", order_endpoint, update_endpoint)
        time.sleep(0.3)      # let the subscription reach the adapter before the first order

    def send_order(self, order: Order) -> bool:
        """A PUSH socket queues messages even with nothing listening, so a True
        here means "handed to ZMQ", not "the adapter got it"."""
        try:
            self.orders.send_json(order.to_message())
        except zmq.Again:
            logger.error("adapter not reachable, order %s not sent", order.order_id)
            return False
        order.status = "SENT"
        return True

    def poll_updates(self, timeout_ms: int) -> list[dict]:
        """Every update that arrives within timeout_ms."""
        messages: list[dict] = []
        deadline = time.monotonic() + timeout_ms / 1000
        while True:
            left_ms = max(0, int((deadline - time.monotonic()) * 1000))
            if not self.poller.poll(left_ms):
                return messages
            msg = recv_json(self.updates)
            if msg is not None:
                messages.append(msg)

    def close(self) -> None:
        self.orders.close(0)
        self.updates.close(0)


# =============================================================================
# The adapter process itself
# =============================================================================
class BrokerAdapter:

    def __init__(self) -> None:
        ctx = zmq.Context.instance()
        self.from_strategy = socket(ctx, zmq.PULL, ORDER_ENDPOINT, bind=True)
        self.to_strategy = socket(ctx, zmq.PUB, UPDATE_ENDPOINT, bind=True)
        self.to_broker = socket(ctx, zmq.PUSH, BROKER_ORDER_ENDPOINT, bind=False)
        self.from_broker = socket(ctx, zmq.SUB, BROKER_UPDATE_ENDPOINT, bind=False)

        self.poller = zmq.Poller()
        self.poller.register(self.from_strategy, zmq.POLLIN)
        self.poller.register(self.from_broker, zmq.POLLIN)

        self.routed: dict[str, dict] = {}       # order_id -> the order as the strategy sent it
        logger.info("strategy: orders <- %s, updates -> %s", ORDER_ENDPOINT, UPDATE_ENDPOINT)
        logger.info("broker:   orders -> %s, reports <- %s", BROKER_ORDER_ENDPOINT, BROKER_UPDATE_ENDPOINT)

    def run(self) -> None:
        logger.info("adapter ready")
        while True:
            # Timeout rather than an indefinite block, so Ctrl+C stops the bridge
            # straight away instead of waiting for the next message.
            for sock, _ in self.poller.poll(200):
                msg = recv_json(sock)
                if msg is None:
                    continue
                try:
                    if sock is self.from_strategy:
                        self.on_strategy_order(msg)
                    else:
                        self.on_broker_report(msg)
                except Exception:               # a bad message must not take the bridge down
                    logger.exception("failed to route message: %s", msg)

    # -----------------------------
    # A -> B -> C
    # -----------------------------
    def on_strategy_order(self, msg: dict) -> None:
        order_id = str(msg.get("order_id", ""))
        if not order_id:
            logger.warning("dropping order without order_id: %s", msg)
            return
        if order_id in self.routed:
            logger.warning("duplicate order_id %s, dropping", order_id)
            return

        self.routed[order_id] = msg
        logger.info("A -> C  %s %s %g %s @ %.4f (%s)", order_id, msg.get("side"), msg.get("qty", 0),
                    msg.get("symbol"), float(msg.get("price", 0)), msg.get("tag", ""))
        self.to_broker.send_json({
            "clOrdId": order_id,
            "instrument": msg.get("symbol"),
            "side": str(msg.get("side", "")).upper(),
            "quantity": float(msg.get("qty", 0)),
            "orderType": msg.get("order_type", "MARKET"),
            "limitPrice": float(msg.get("price", 0)),
        })

    # -----------------------------
    # C -> B -> A
    # -----------------------------
    def on_broker_report(self, report: dict) -> None:
        order_id = str(report.get("clOrdId", ""))
        status = EXEC_TYPES.get(str(report.get("execType", "")).upper())
        if status is None:
            logger.warning("unknown execType from broker: %s", report)
            return
        if order_id not in self.routed:
            logger.warning("report for an order this adapter did not route: %s", order_id)

        update = update_message(
            order_id=order_id,
            status=status,
            fill_qty=float(report.get("lastQty") or 0),
            fill_price=report.get("lastPx"),
            reason=str(report.get("text", "")),
        )
        publish(self.to_strategy, update, topic=order_id)
        logger.info("C -> A  %s %s%s", order_id, status,
                    f" {update['fill_qty']:g} @ {update['fill_price']:.4f}" if update.get("fill_qty") else "")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Script B: broker adapter between the strategy and the broker")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s [B adapter] %(message)s", datefmt="%H:%M:%S")
    try:
        BrokerAdapter().run()
    except KeyboardInterrupt:
        logger.info("stopped by user")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
