import uuid
import time
import threading
from collections import deque
from enum import IntEnum
import logging

logging.basicConfig(level=logging.INFO)

MAX_ORDER_QTY = 1_000_000 
MAX_QUEUE_SIZE_PER_PRICE = 1000  
PRICE_COLLAR = 100 

class Side(IntEnum):
    BUY = 0
    SELL = 1

class Order:
    def __init__(self, user_id, side, price, qty, is_market=False):
        self.order_id = str(uuid.uuid4())  
        self.user_id = user_id 
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
        self.order_map = {}

        self.best_bid = None
        self.best_ask = None

        self.lock = threading.Lock()  

    def add_order(self, user_id, side, price, qty, is_market=False):
        # Input validation first
        if not isinstance(side, Side):
            logging.warning("Invalid side type.")
            return None
        if not isinstance(qty, int) or qty <= 0 or qty > MAX_ORDER_QTY:
            logging.warning("Invalid quantity.")
            return None
        if not is_market and (price is None or not isinstance(price, (int, float))):
            logging.warning("Invalid price for limit order.")
            return None

        start = time.perf_counter()

        try:
            with self.lock:
                if is_market:
                    if side == Side.BUY and self.best_ask is not None:
                        price = self.best_ask + PRICE_COLLAR
                    elif side == Side.SELL and self.best_bid is not None:
                        price = self.best_bid - PRICE_COLLAR
                    else:
                        raise ValueError("Cannot execute market order with no opposing liquidity.")

                order = Order(user_id, side, price, qty, is_market)
                self.match_order(order)

                if order.qty > 0 and not is_market:
                    book = self.bids if side == Side.BUY else self.asks
                    queue = book.setdefault(price, deque())

                    if len(queue) >= MAX_QUEUE_SIZE_PER_PRICE:
                        raise OverflowError("Order queue at this price level is full.")

                    queue.append(order)
                    self.order_map[order.order_id] = (order, queue)
                    self._update_best_prices_after_add(side, price)

                end = time.perf_counter()
                logging.info(f"Order {order.order_id} added in {(end - start) * 1e3:.2f} ms")
                return order.order_id

        except Exception as e:
            logging.error(f"Order rejected: {e}")
            return None

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

    def cancel_order(self, user_id, order_id):
        with self.lock:
            data = self.order_map.get(order_id)
            if not data:
                logging.warning(f"Order ID {order_id} not found.")
                return False
            order, queue = data

            if order.user_id != user_id:
                logging.warning(f"User {user_id} not authorized to cancel order {order_id}.")
                return False

            try:
                queue.remove(order)
                del self.order_map[order_id]
                logging.info(f"Canceled order ID {order_id}")
                self._update_best_prices_after_remove()
                return True
            except ValueError:
                logging.warning(f"Order ID {order_id} already executed or not found.")
                return False

    def modify_order(self, user_id, order_id, new_qty=None, new_price=None):
        with self.lock:
            data = self.order_map.get(order_id)
            if not data:
                logging.warning(f"Cannot modify: order {order_id} not found.")
                return False

            order, _ = data
            if order.user_id != user_id:
                logging.warning(f"User {user_id} not authorized to modify order {order_id}.")
                return False

            if not self.cancel_order(user_id, order_id):
                return False

            updated_qty = new_qty if new_qty is not None else order.qty
            updated_price = new_price if new_price is not None else order.price

            new_id = self.add_order(user_id, order.side, updated_price, updated_qty)
            if new_id is None:
                logging.error(f"Failed to re-add modified order for user {user_id}.")
                return False

            return True

    def match_order(self, order):
        with self.lock:
            book = self.asks if order.side == Side.BUY else self.bids
            comparator = self._buy_comparator if order.side == Side.BUY else self._sell_comparator

            while order.qty > 0 and book:
                best_price = min(book.keys()) if order.side == Side.BUY else max(book.keys())
                if not order.is_market and not comparator(order.price, best_price):
                    break

                queue = book[best_price]
                while queue and order.qty > 0:
                    top_order = queue[0]
                    traded_qty = min(order.qty, top_order.qty)
                    order.qty -= traded_qty
                    top_order.qty -= traded_qty

                    assert order.qty >= 0 and top_order.qty >= 0, "Order quantity went negative!"

                    self.trades.append((order.order_id, top_order.order_id, best_price, traded_qty))

                    if top_order.qty == 0:
                        queue.popleft()
                        self.order_map.pop(top_order.order_id, None)

                if not queue:
                    del book[best_price]
                    self._update_best_prices_after_remove()

    def _buy_comparator(self, buy_price, ask_price):
        return buy_price >= ask_price

    def _sell_comparator(self, sell_price, bid_price):
        return sell_price <= bid_price

