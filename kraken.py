import hmac
import hashlib
import requests
import time
import base64
from urllib.parse import urlencode
from decimal import Decimal
from .base import Balance, Order, Trade, MarginInfo, MarginPosition
from datetime import datetime


class Kraken(object):
    GET_URL = 'https://api.kraken.com/0/public/{}'
    POST_URL = 'https://api.kraken.com/0/private/{}'

    def __init__(self, auth):
        self._secret = auth.get_secret()
        self._key = auth.get_key()

    def sign_request(self, method, data):
        urlpath = "/0/private/{}".format(method)
        postdata = urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()
        signature = hmac.new(base64.b64decode(self._secret),
                             message, hashlib.sha512)
        sigdigest = base64.b64encode(signature.digest())
        return sigdigest.decode()

    def get_req_headers(self, method, data):
        headers = {
            'API-Key': self._key,
            'API-Sign': self.sign_request(method, data)
        }
        return headers

    def _nonce(self):
        return int(1000 * time.time())

    def get_balance(self):
        orders = self.get_open_orders()
        balances = self.get_full_balance()
        result = []
        for order in orders:
            for balance in balances:
                if order.type == 'sell':
                    symbol = order.symbol[:3]
                else:
                    symbol = order.symbol[3:]
                if balance.currency.find(symbol) != -1:
                    freeze = order.amount * order.rate
                    result.append({symbol: {'freeze': freeze, 'free': Decimal(balance.amount) - freeze}})
        return result

    def _get_all_balance(self, symbol):
        method = 'TradeBalance'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce(),
            'asset': symbol,
        }
        headers = self.get_req_headers(method, data)
        data = requests.post(url, data=data, headers=headers).json()
        return data['result']['eb']

    def apply_fee(self, order):
        if order.order_type == "sell":
            order.total = Decimal(order.total) * (Decimal(1.0) - order.exchange.taker_fee)
        elif order.order_type == "buy":
            order.total = Decimal(order.total) * (Decimal(1.0) + order.exchange.taker_fee)
        return order

    def get_all_usdt_balance(self):
        return self._get_all_balance('ZUSD')

    def get_all_btc_balance(self):
        return self._get_all_balance('XBT')

    def get_full_balance(self):
        method = 'Balance'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce(),
        }
        headers = self.get_req_headers(method, data)
        result = requests.post(url, data=data, headers=headers).json()

        print(result)
        if "error" in result and len(result["error"]) > 0:
            return []
        res = []
        for cur, vol in result['result'].items():
            if Decimal(vol) == Decimal(0):
                continue
            res.append(Balance(cur[1:].upper(), vol, type='exchange'))
        return res

    def get_orderbook(self, symbol):
        method = 'Depth'
        params = {
            'nonce': self._nonce(),
            'pair': symbol,
            'count': 1,
        }
        url = self.GET_URL.format(method)
        data = requests.get(url, params=params).json()
        if data['error']:
            return data['error']
        else:
            return data['result'][symbol]

    def get_last_price(self, symbol, action, amount):
        glass = self.get_orderbook(symbol)
        if glass:
            data = glass['asks'] if action == "buy" else glass['bids']
            for d in data:
                amount -= float(d[1])
                if amount <= 0:
                    return Decimal(d[0])
        else:
            return None

    def get_symbols(self):
        method = 'AssetPairs'
        url = self.GET_URL.format(method)
        params = {
            'nonce': self._nonce()
        }
        data = requests.get(url, params=params).json()
        result = []
        if data['error']:
            return data['error']
        else:
            print(data['result'])
            for key in data['result'].keys():
                result.append(data['result'][key]['altname'])
            return result

    def get_filters(self):
        method = 'AssetPairs'
        url = self.GET_URL.format(method)
        params = {
            'nonce': self._nonce()
        }
        data = requests.get(url, params=params).json()

        result = []
        print(data)
        if data['error']:
            print(data["error"])
            return None
        else:
            for d in data["result"]:
                result.append({
                    "min_price": 0.00000001,
                    "min_amount": 0.00000001, # 1/10**data["result"][d]["pair_decimals"],
                    "min_lot": 1/10**data["result"][d]["lot_decimals"],
                    "pairs": d,
                    "exchange": "Kraken"
                })
            return result

    def get_tickers(self, currency=None):
        method = 'Ticker'
        url = self.GET_URL.format(method)
        params = {
            'nonce': self._nonce(),
        }
        if currency is not None:
            params.update({'pair': currency})
        else:
            params.update({'pair': ', '.join(self.get_symbols())})

        data = requests.get(url, params=params).json()
        return data

    def get_feeinfo(self):
        '''method = 'AssetPairs'
        url = self.GET_URL.format(method)
        params = {
            'nonce': self._nonce(),
        }
        data = requests.get(url, params=params).json()
        return data['result']'''

        # Для каждой пары возвращает комиссию отдельно. Временное решение
        return {'maker_fee': Decimal(0.16) / Decimal(100),
                'taker_fee': Decimal(0.26) / Decimal(100)}

    def new_order(self, rate, order_type, amount, symbol, market=False):
        method = 'AddOrder'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce(),
            'pair': symbol,
            'type': 'buy' if order_type == 'buy' else 'sell',

        }
        if market:
            data.update({'ordertype': 'market', 'volume': float(amount)})
        else:
            data.update({'ordertype': 'limit', 'price': float(rate), 'volume': float(amount)})

        print(data)
        headers = self.get_req_headers(method, data)
        result = requests.post(url, data=data, headers=headers)
        print(result.status_code, result.content)
        result = result.json()
        if result['error']:
            print(result)
            return None
        else:
            print(result['result']['txid'][0])
            order = {'orderId': result['result']['txid'][0], 'rate': rate, 'type': order_type,
                     'amount': amount, 'symbol': symbol}
            return KrakenOrder.create_object_from_json(order)

    def get_open_orders(self):
        method = 'OpenOrders'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce()
        }
        headers = self.get_req_headers(method, data)
        result = requests.post(url, data=data, headers=headers).json()
        res = []
        for key in result['result']['open'].keys():
            order_info = result['result']['open'][key]
            order = {'orderId': key, 'rate': order_info['descr']['price'], 'type': order_info['descr']['type'],
                     'amount': order_info['vol'], 'symbol': order_info['descr']['pair']}
            res.append(KrakenOrder.create_object_from_json(order))
        return res

    def cancel_order(self, order):
        method = 'CancelOrder'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce(),
            'txid': order.number,
        }
        headers = self.get_req_headers(method, data)
        result = requests.post(url, data=data, headers=headers).json()
        if result['error']:
            return False
        return True

    def close_order(self, order):
        method = 'AddOrder'
        url = self.POST_URL.format(method)
        is_closed = self.cancel_order(order)
        if not is_closed:
            return False

        data = {
            'nonce': self._nonce(),
            'pair': order.symbol.name,
            'type': order.order_type,
            'ordertype': 'market',
            'volume': order.amount
        }

        headers = self.get_req_headers(method, data)
        result = requests.post(url, data=data, headers=headers).json()
        print(result)
        if not result['error']:
            return True
        return False

    def get_trade_history(self, start=None, end=None, limit=1000, pairs=None):
        method = 'TradesHistory'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce(),
        }
        headers = self.get_req_headers(method, data)
        data = requests.post(url, data=data, headers=headers).json()
        print(data)
        result = []
        for key in data['result']['trades'].keys():
            result.append(KrakenTrade.create_object_from_json(data['result']['trades'][key]))
        return result

    def move_order(self, order, rate, amount):
        is_canceled = self.cancel_order(order)
        if not is_canceled:
            return None

        method = 'AddOrder'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce(),
            'pair': order.symbol.name,
            'type': order.order_type,
            'ordertype': 'limit',
            'price': rate,
            'volume': amount,

        }
        headers = self.get_req_headers(method, data)
        result = requests.post(url, data=data, headers=headers).json()
        print(result)
        if not result['error']:
            try:
                return result['result']['txid'][0]
            except:
                print("Fuck cant move order, kraken {}".format(order.number))
        return None

    def get_margin_position(self):
        method = 'OpenPositions'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce(),
            'docalcs': 'true',
        }
        headers = self.get_req_headers(method, data)
        positions = requests.post(url, data=data, headers=headers).json()
        result = []
        for key in positions['result'].keys():
            result.append(KrakenMarginPosition.create_object_from_json(positions['result'][key]))
        return result

    def close_margin_position(self, symbol):
        positions = self.get_margin_position()
        for p in positions:
            if p.symbol == symbol:
                method = 'AddOrder'
                url = self.POST_URL.format(method)
                data = {
                    'nonce': self._nonce(),
                    'pair': symbol,
                    'type': 'sell' if p.side == 'long' else 'buy',
                    'leverage': 2,
                    'ordertype': 'market',
                    'volume': p.amount,
                }
                headers = self.get_req_headers(method, data)
                result = requests.post(url, data=data, headers=headers).json()
                if not result['error']:
                    return True
                else:
                    return False

    def get_margin_info(self):
        method = 'TradeBalance'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce(),
        }
        headers = self.get_req_headers(method, data)
        result = requests.post(url, data=data, headers=headers).json()
        if result['error']:
            return None
        else:
            return KrakenMarginInfo.create_object_from_json(result['result'])

    def open_margin_position(self, symbol, rate, amount, side):
        method = 'AddOrder'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce(),
            'pair': symbol,
            'type': 'sell' if int(side) == 0 else 'buy',
            'ordertype': 'limit',
            'price': rate,
            'volume': amount,
            'leverage': 2,

        }
        headers = self.get_req_headers(method, data)
        result = requests.post(url, data=data, headers=headers).json()
        if not result['error']:
            return self.get_margin_position()
        else:
            print(result['error'])
            return None

    def toggle_margin_positions(self, margin_position):
        self.close_margin_position(margin_position.symbol)
        side = 0 if margin_position.side == 'long' else 1
        return self.open_margin_position(
            margin_position.symbol, margin_position.base_price,
            margin_position.amount, side
        )

    def is_order_fulfilled(self, order):
        method = 'QueryOrders'
        url = self.POST_URL.format(method)
        data = {
            'nonce': self._nonce(),
            'txid': order.number,
        }
        headers = self.get_req_headers(method, data)
        res = requests.post(url, data=data, headers=headers).json()
        if not res['error'] and res['result'][order.number]['vol'] == res['result'][order.number]['vol_exec']:
            return True
        else:
            return False


class KrakenTrade(Trade):

    @classmethod
    def create_object_from_json(cls, data):
        return cls(
            data["pair"], data["ordertxid"], data["ordertxid"],
            data["ordertxid"], 0 if data["type"] == "sell" else 1,
            "", float(data["vol"]), float(data["price"]),
            float(data["fee"]), float(data["vol"]),
            datetime.fromtimestamp(int(data["time"])),
        )


class KrakenOrder(Order):

    @classmethod
    def create_object_from_json(cls, data):
        return cls(
            data["orderId"], Decimal(data["rate"]), data["type"],
            Decimal(data["amount"]), Decimal(data["rate"]) * Decimal(data['amount']), data["symbol"]
        )


class KrakenMarginPosition(MarginPosition):

    @classmethod
    def create_object_from_json(cls, data):
        return cls(
            data["vol"], data["net"], data['pair'],
            0, 'short' if data["type"] == 'sell' else 'long'
        )


class KrakenMarginInfo(MarginInfo):

    @classmethod
    def create_object_from_json(cls, data):
        return cls(
            Decimal(data["eb"]), data["e"], data["mf"], data["n"]
        )
