from collections import deque
from enum import IntEnum
import itertools
import time

class Side(IntEnum):
    BUY = 0
    SELL = 1

class Order:
    def __init__(self, order_id, side, price, qty, is_market=False):
        self.order_id = order_id
        self.side = side
        self.price = price
        self.qty = qty
        self.is_market = is_market

    def __repr__(self):
        side = 'Buy' if self.side == Side.BUY else 'Sell'
        market = 'Market ' if self.is_market else ''
        return f"{market}{side} {self.qty}@{self.price if self.price is not None else 'MKT'} (ID: {self.order_id})"

class OrderBook:
    def __init__(self):
        self.bids = {}  
        self.asks = {}  
        self.trades = []
        self.order_id_counter = itertools.count()
        self.order_map = {}

        self.best_bid = None
        self.best_ask = None

    def add_order(self, side, price, qty, is_market=False):
        start = time.perf_counter()

        if qty <= 0:
            raise ValueError("Quantity must be positive.")
        if not is_market and price is None:
            raise ValueError("Price must be specified for limit orders.")

        order_id = next(self.order_id_counter)
        order = Order(order_id, side, price, qty, is_market)

        self.match_order(order)

        if order.qty > 0 and not is_market:
            book = self.bids if side == Side.BUY else self.asks
            queue = book.setdefault(price, deque())
            queue.append(order)
            self.order_map[order_id] = (order, queue)
            self._update_best_prices_after_add(side, price)

        end = time.perf_counter()
        print(f"Order {order_id} processed in {(end - start) * 1e6:.2f} µs")
        return order_id

    def _update_best_prices_after_add(self, side, price):
        if side == Side.BUY:
            if self.best_bid is None or price > self.best_bid:
                self.best_bid = price
        else:
            if self.best_ask is None or price < self.best_ask:
                self.best_ask = price

    def _update_best_prices_after_remove(self):
        self.best_bid = max(self.bids.keys(), default=None)
        self.best_ask = min(self.asks.keys(), default=None)

    def cancel_order(self, order_id):
        data = self.order_map.pop(order_id, None)
        if not data:
            print(f"Order ID {order_id} not found.")
            return False
        order, queue = data
        try:
            queue.remove(order)
            print(f"Canceled order ID {order_id}")
            self._update_best_prices_after_remove()
            return True
        except ValueError:
            print(f"Order ID {order_id} already executed or not found in queue.")
            return False

    def modify_order(self, order_id, new_qty=None, new_price=None):
        data = self.order_map.get(order_id)
        if not data:
            print(f"Cannot modify: order {order_id} not found.")
            return False
        order, _ = data
        side = order.side
        current_qty = order.qty
        current_price = order.price

        if not self.cancel_order(order_id):
            return False

        updated_qty = new_qty if new_qty is not None else current_qty
        updated_price = new_price if new_price is not None else current_price
        self.add_order(side, updated_price, updated_qty)
        return True

    def match_order(self, order):
        is_buy = order.side == Side.BUY
        book = self.asks if is_buy else self.bids
        comparator = (lambda o_price: order.price >= o_price) if is_buy else (lambda o_price: order.price <= o_price)

        while order.qty > 0 and book:
            best_price = min(book.keys()) if is_buy else max(book.keys())
            if not order.is_market and not comparator(best_price):
                break

            queue = book[best_price]
            while queue and order.qty > 0:
                top_order = queue[0]
                traded_qty = min(order.qty, top_order.qty)
                order.qty -= traded_qty
                top_order.qty -= traded_qty

                if is_buy:
                    self.trades.append((order.order_id, top_order.order_id, best_price, traded_qty))
                else:
                    self.trades.append((top_order.order_id, order.order_id, best_price, traded_qty))

                if top_order.qty == 0:
                    queue.popleft()
                    self.order_map.pop(top_order.order_id, None)

            if not queue:
                del book[best_price]
                self._update_best_prices_after_remove()

    def print_book(self):
        print("\n--- ORDER BOOK ---")
        print("BIDS:")
        for price in sorted(self.bids.keys(), reverse=True):
            print(f"  {price}: {[o.qty for o in self.bids[price]]}")
        print("ASKS:")
        for price in sorted(self.asks.keys()):
            print(f"  {price}: {[o.qty for o in self.asks[price]]}")

    def print_spread(self):
        print("\n--- SPREAD ---")
        if self.best_bid is not None and self.best_ask is not None:
            spread = self.best_ask - self.best_bid
            print(f"Best Bid: {self.best_bid}, Best Ask: {self.best_ask}, Spread: {spread}")
        else:
            print("Not enough data to compute spread.")

    def print_trades(self):
        print("\n--- TRADES ---")
        for buy_id, sell_id, price, qty in self.trades:
            print(f"Buy {buy_id} ↔ Sell {sell_id} @ {price} x {qty}")
