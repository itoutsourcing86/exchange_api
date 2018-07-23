import hmac
import hashlib
import time
import requests
from decimal import Decimal
from urllib.parse import urlencode
from .base import Balance, Order, Trade
from datetime import datetime


class Binance(object):
    URL = 'https://api.binance.com/api/'

    def __init__(self, auth):
        self._secret = auth.get_secret()
        self._key = auth.get_key()

    def signed_request(self, method, path, params):
        query = urlencode(params)
        query += "&timestamp={}".format(int(time.time() * 1000))
        secret = bytes(self._secret.encode("utf-8"))
        signature = hmac.new(secret, query.encode("utf-8"),
                             hashlib.sha256).hexdigest()
        query += "&signature={}".format(signature)
        resp = requests.request(method,
                                self.URL + path + "?" + query,
                                headers={"X-MBX-APIKEY": self._key}).json()
        return resp

    def request(self, method, path, params=None):
        resp = requests.request(method, self.URL + path, params=params)
        data = resp.json()
        return data

    def apply_fee(self, order):
        if order.order_type == "sell":
            order.total = Decimal(order.total) * (Decimal(1.0) - order.exchange.taker_fee)
        elif order.order_type == "buy":
            order.total = Decimal(order.total) * (Decimal(1.0) + order.exchange.taker_fee)
        return order

    def _get_order_symbol(self, order_id):
        for order in self.get_open_orders():
            if order.number == order_id:
                return order.symbol

    def get_full_balance(self):
        balances = self.get_balance()

        result = []
        for balance in balances:
            amount = Decimal(balance['free']) + Decimal(balance['locked'])
            result.append(Balance(balance['asset'].upper(), amount, type='exchange'))
        return result

    def get_balance(self):
        data = self.signed_request("GET", "v3/account", {})
        result = []
        for balance in data['balances']:
            if Decimal(balance['free']) != 0 or Decimal(balance['locked']) != 0:
                result.append(balance)
        return result

    def get_tickers(self, currency=None):
        if not currency:
            return self.request('GET', 'v1/ticker/24hr', {})
        else:
            return self.request('GET', 'v1/ticker/24hr', {'symbol': currency})

    def new_order(self, rate, order_type, amount, symbol, market=False):
        params = {
            'symbol': symbol,
            'side': order_type,
            'quantity': "{}".format(amount),
            'newOrderRespType': 'FULL',
        }
        if market:
            params.update({'type': 'market'})
        else:
            params.update({'timeInForce': 'GTC', 'price': "{:f}".format(rate), 'type': 'limit'})  # Уточнить по поводу format(rate), rate это строка

        data = self.signed_request('POST', 'v3/order', params)

        if 'orderId' in data:
            if market:
                data['price'] = data['fills'][0]['price']
            print(data)
            return BinanceOrder.create_object_from_json(data)
        else:
            print(data['msg'], params)
            return None

    def get_orderbook(self, symbol):
        data = self.request('GET', 'v3/ticker/bookTicker', {'symbol': symbol})

        if 'symbol' in data:
            return data
        return None

    def get_filters(self):
        data = self.request('GET', 'v1/exchangeInfo', {})

        result = []
        if 'symbols' in data:
            for d in data["symbols"]:
                filters = {
                    "min_price": 0.00000001,
                    "min_amount": 0.00000001,
                    "min_lot": 0.00000001,
                    "pairs": d["symbol"],
                    "exchange": "Binance"
                }
                for f in d["filters"]:
                    if f["filterType"] == "PRICE_FILTER":
                        filters["min_price"] = Decimal("{}".format(float(f["minPrice"])))
                    elif f["filterType"] == "LOT_SIZE":
                        filters["min_amount"] = Decimal("{}".format(float(f["minQty"])))
                    elif f["filterType"] == "MIN_NOTIONAL":
                        filters["min_lot"] = Decimal("{}".format(float(f["minNotional"])))
                print(filters)
                result.append(filters)
            return result
        print(data)
        return None

    def get_feeinfo(self):
        data = self.signed_request('GET', 'v3/account', {})
        if "makerCommission" in data:
            return {"maker_fee": Decimal(data["makerCommission"])/10000, "taker_fee": Decimal(data["takerCommission"])/10000}
        return None

    def get_last_price(self, symbol, action, amount):
        pass

    def get_open_orders(self):
        data = self.signed_request('GET', 'v3/openOrders', {})

        result = []
        for order in data:
            result.append(BinanceOrder.create_object_from_json(order))
        return result

    def cancel_order(self, order):
        params = {
            'orderId': order.number,
            'symbol': order.symbol.name
        }
        print(params)
        result = self.signed_request('DELETE', 'v3/order', params)

        print(result)
        if 'clientOrderId' in result:
            return True
        return False

    def get_all_usdt_balance(self):
        btc_balance = Decimal(self.get_all_btc_balance())
        btc_usdt_price = Decimal(self.get_tickers(currency='BTCUSDT')['lastPrice'])
        return btc_balance * btc_usdt_price

    def get_all_btc_balance(self):
        balances = self.get_balance()
        btc_usdt_price = Decimal(self.get_tickers('BTCUSDT')['lastPrice'])
        all_tickers = self.get_tickers()

        result = Decimal('0.0')
        for balance in balances:
            if Decimal(balance['free']) != 0:
                if balance['asset'] == 'BTC':
                    result += Decimal(balance['free'])
                    continue
                if balance['asset'] == 'USDT':
                    result += Decimal(balance['free']) / btc_usdt_price
                    continue

                for ticker in all_tickers:
                    market = '{}BTC'.format(balance['asset'])
                    if market == ticker['symbol']:
                        price = Decimal(ticker['lastPrice'])
                        result += Decimal(balance['free']) * price
        return result

    def is_order_fulfilled(self, order):
        params = {
            'orderId': order.number,
            'symbol': self._get_order_symbol(order.number)
        }
        data = self.signed_request("GET", "/v3/order", params)
        if data['status'] == 'FILLED':
            return True
        return False

    def get_trade_history(self, start=None, end=None, limit=1000, pairs=None):
        if pairs is not None:
            data = self.signed_request('GET', 'v3/myTrades', {'symbol': pairs})
            result = []
            for trade in data:
                result.append(BinanceTrade.create_object_from_json(trade))
            return result
        else:
            return None

    def close_order(self, order):
        is_closed = self.cancel_order(order)
        if not is_closed:
            return False
        order = self.new_order(order.rate, order.order_type, order.amount, order.symbol.name, market=True)
        if order:
            return True
        return False

    def move_order(self, order, rate, amount):
        is_closed = self.cancel_order(order)
        if not is_closed:
            return False
        order = self.new_order(rate, order.order_type, amount, order.symbol.name)
        if order:
            return order.number
        return None


class BinanceOrder(Order):

    @classmethod
    def create_object_from_json(cls, data):
        return cls(
            data["orderId"], Decimal(data["price"]), data["side"].lower(),
            Decimal(data["origQty"]), Decimal(data["origQty"])*Decimal(data["price"]),
            data["symbol"]
        )


class BinanceTrade(Trade):

    @classmethod
    def create_object_from_json(cls, data):
        return cls(
            data["commissionAsset"], data["orderId"], data["orderId"],
            data["id"], 0 if data["isBuyer"] == "False" else 1,
            "", float(data["qty"]), float(data["price"]),
            float(data["commission"]), float(data["qty"]),
            datetime.fromtimestamp(int(float(data["time"]/1000)))
        )
