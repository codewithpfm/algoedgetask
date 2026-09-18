import argparse
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import zmq

from config import BROKER_ORDER_ENDPOINT, BROKER_UPDATE_ENDPOINT

logger = logging.getLogger("broker")


# =============================================================================
# Wire helpers. Its own copy, so this script does not depend on the others.
# =============================================================================
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def socket(ctx: zmq.Context, kind: int, endpoint: str, bind: bool) -> zmq.Socket:
    sock = ctx.socket(kind)
    sock.setsockopt(zmq.LINGER, 0)
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
    sock.send_multipart([topic.encode(), json.dumps(msg).encode()])


# =============================================================================
# The broker's own book: what it filled, and what that leaves the client holding
# =============================================================================
@dataclass
class BrokerOrder:
    cl_ord_id: str
    instrument: str
    side: str
    qty: float
    price: float
    ts: str = field(default_factory=now_iso)
    filled_qty: float = 0.0
    avg_fill_price: float = float("nan")
    status: str = "ACCEPTED"

    @property
    def remaining(self) -> float:
        return self.qty - self.filled_qty


@dataclass
class Position:
    qty: float = 0.0                 # signed: + long, - short
    avg_price: float = 0.0
    realised_pnl: float = 0.0


class BrokerBook:
    """Orders, positions and realised PnL, average cost method."""

    def __init__(self) -> None:
        self.orders: dict[str, BrokerOrder] = {}
        self.positions: dict[str, Position] = {}
        self.last_price: dict[str, float] = {}

    def add(self, order: BrokerOrder) -> BrokerOrder:
        self.orders[order.cl_ord_id] = order
        return order

    def position_qty(self, instrument: str) -> float:
        return self.positions.get(instrument, Position()).qty

    def apply_fill(self, order: BrokerOrder, qty: float, price: float) -> None:
        done = order.filled_qty
        order.avg_fill_price = price if done == 0 else (order.avg_fill_price * done + price * qty) / (done + qty)
        order.filled_qty = done + qty
        order.status = "FILLED" if order.remaining <= 1e-9 else "PARTIAL"

        pos = self.positions.setdefault(order.instrument, Position())
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
                pos.avg_price = price        # flipped through zero
        self.last_price[order.instrument] = price

    @property
    def realised_pnl(self) -> float:
        return sum(p.realised_pnl for p in self.positions.values())

    def dashboard(self, title: str = "") -> str:
        lines = [
            "=" * 100,
            f" BROKER C | {title}",
            f" Realised PnL: {self.realised_pnl:,.2f}   Orders: {len(self.orders)}"
            f" ({sum(o.status == 'FILLED' for o in self.orders.values())} filled)",
            "-" * 100,
            " ORDER BOOK",
            f"   {'clOrdId':<18}{'instrument':<12}{'side':<6}{'qty':>10}{'filled':>10}{'avgPx':>12}  status",
        ]
        for o in self.orders.values():
            lines.append(f"   {o.cl_ord_id:<18}{o.instrument:<12}{o.side:<6}{o.qty:>10.4f}"
                         f"{o.filled_qty:>10.4f}{o.avg_fill_price:>12.4f}  {o.status}")
        if not self.orders:
            lines.append("   (no orders)")

        lines += ["-" * 100, " OPEN POSITIONS",
                  f"   {'instrument':<12}{'side':<7}{'qty':>10}{'avgPx':>12}{'lastPx':>12}"]
        open_positions = [(i, p) for i, p in self.positions.items() if p.qty != 0]
        for instrument, p in open_positions:
            lines.append(f"   {instrument:<12}{'LONG' if p.qty > 0 else 'SHORT':<7}{abs(p.qty):>10.4f}"
                         f"{p.avg_price:>12.4f}{self.last_price.get(instrument, float('nan')):>12.4f}")
        if not open_positions:
            lines.append("   (flat)")
        lines.append("=" * 100)
        return "\n".join(lines)


# =============================================================================
# The broker process
# =============================================================================
class MockBroker:

    def __init__(self, slippage_bps: float = 0.5, latency: float = 0.05,
                 max_qty: float = 1_000.0, reject_every: int = 0,
                 partial_threshold: float = 2.0) -> None:
        self.slippage_bps = slippage_bps
        self.latency = latency
        self.max_qty = max_qty
        self.reject_every = reject_every
        self.partial_threshold = partial_threshold
        self.book = BrokerBook()
        self.received = 0

        ctx = zmq.Context.instance()
        self.orders_in = socket(ctx, zmq.PULL, BROKER_ORDER_ENDPOINT, bind=True)
        self.reports_out = socket(ctx, zmq.PUB, BROKER_UPDATE_ENDPOINT, bind=True)
        logger.info("listening for orders on %s | reports on %s",
                    BROKER_ORDER_ENDPOINT, BROKER_UPDATE_ENDPOINT)

    # -----------------------------
    # Wire
    # -----------------------------
    def report(self, cl_ord_id: str, exec_type: str, last_qty: float = 0.0,
               last_px: Optional[float] = None, leaves_qty: float = 0.0, text: str = "") -> None:
        publish(self.reports_out, {
            "clOrdId": cl_ord_id,
            "execType": exec_type,
            "lastQty": last_qty,
            "lastPx": last_px,
            "leavesQty": leaves_qty,
            "text": text,
            "ts": now_iso(),
        }, topic=cl_ord_id)
        logger.info("-> %s %s%s", exec_type, cl_ord_id,
                    f" {last_qty:g} @ {last_px:.4f}" if last_qty else "")

    def run(self) -> None:
        logger.info("mock broker ready")
        while True:
            msg = recv_json(self.orders_in)
            if msg is None:
                continue
            try:
                self.handle(msg)
            except Exception:                      # never let one bad order kill the broker
                logger.exception("failed to handle order: %s", msg)

    # -----------------------------
    # Order handling
    # -----------------------------
    def handle(self, msg: dict) -> None:
        cl_ord_id = str(msg.get("clOrdId", ""))
        side = str(msg.get("side", "")).upper()
        qty = float(msg.get("quantity", 0))
        price = float(msg.get("limitPrice") or 0)
        instrument = str(msg.get("instrument", ""))
        self.received += 1
        logger.info("<- order %s %s %g %s @ %.4f", cl_ord_id, side, qty, instrument, price)

        reject = self.validate(cl_ord_id, side, qty, price)
        if reject:
            self.report(cl_ord_id, "REJECT", leaves_qty=qty, text=reject)
            return

        order = self.book.add(BrokerOrder(cl_ord_id=cl_ord_id, instrument=instrument,
                                          side=side, qty=qty, price=price))
        self.report(cl_ord_id, "ACK", leaves_qty=qty)
        time.sleep(self.latency)

        fill_price = self.fill_price(side, price)
        for last_qty in self.fill_sizes(qty):
            self.book.apply_fill(order, last_qty, fill_price)
            leaves = order.remaining
            self.report(cl_ord_id, "FILL" if leaves <= 1e-9 else "PARTIAL_FILL",
                        last_qty=last_qty, last_px=fill_price, leaves_qty=leaves)
            if leaves > 1e-9:
                time.sleep(self.latency)
        logger.info("position %+g | realised PnL %.2f",
                    self.book.position_qty(instrument), self.book.realised_pnl)

    def validate(self, cl_ord_id: str, side: str, qty: float, price: float) -> str:
        """Empty string means the order is accepted."""
        if not cl_ord_id:
            return "missing clOrdId"
        if side not in ("BUY", "SELL"):
            return f"unknown side {side!r}"
        if qty <= 0:
            return "quantity must be positive"
        if price <= 0:
            return "price must be positive"
        if qty > self.max_qty:
            return f"risk limit: quantity {qty:g} above {self.max_qty:g}"
        if self.reject_every and self.received % self.reject_every == 0:
            return "insufficient margin"
        return ""

    def fill_price(self, side: str, price: float) -> float:
        """Market orders pay the spread: buys fill a touch higher, sells a touch lower."""
        slip = price * self.slippage_bps / 10_000
        return price + slip if side == "BUY" else price - slip

    def fill_sizes(self, qty: float) -> list[float]:
        """Larger orders come back as two partial fills."""
        if qty >= self.partial_threshold:
            half = round(qty / 2, 8)
            return [half, round(qty - half, 8)]
        return [qty]


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Script C: mock broker")
    parser.add_argument("--slippage-bps", type=float, default=0.5, help="slippage in basis points (default 0.5)")
    parser.add_argument("--latency", type=float, default=0.05, help="seconds between ack and fill (default 0.05)")
    parser.add_argument("--max-qty", type=float, default=1000.0, help="reject orders above this size")
    parser.add_argument("--reject-every", type=int, default=0, help="reject every Nth order (0 = never)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s [C broker] %(message)s", datefmt="%H:%M:%S")

    broker = MockBroker(args.slippage_bps, args.latency, args.max_qty, args.reject_every)
    try:
        broker.run()
    except KeyboardInterrupt:
        logger.info("stopped by user")
        print(broker.book.dashboard("FINAL"), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
