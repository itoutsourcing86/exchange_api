import hmac
import hashlib
import json
import base64
import urllib
import urllib.parse
import requests
import datetime
from decimal import Decimal
from .base import Trade, Balance, Order, MarginPosition, MarginInfo


class Huobi(object):
    MARKET_URL = "https://api.huobi.pro"
    TRADE_URL = "https://api.huobi.pro"

    def __init__(self, auth):
        self._secret = auth.get_secret()
        self._key = auth.get_key()

    def http_get_request(self, url, params, add_to_headers=None):
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
        }
        if add_to_headers:
            headers.update(add_to_headers)
        postdata = urllib.parse.urlencode(params)
        response = requests.get(url, postdata, headers=headers)
        return response

    def decimal_default(self, obj):
        if isinstance(obj, Decimal):
            return "{:f}".format(obj)
        raise TypeError

    def http_post_request(self, url, params, add_to_headers=None):
        headers = {
            "Accept": "application/json",
            'Content-Type': 'application/json'
        }
        if add_to_headers:
            headers.update(add_to_headers)
        postdata = json.dumps(params, default=self.decimal_default)
        response = requests.post(url, postdata, headers=headers)
        return response

    def api_key_get(self, params, request_path):
        method = 'GET'
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        params.update({'AccessKeyId': self._key,
                       'SignatureMethod': 'HmacSHA256',
                       'SignatureVersion': '2',
                       'Timestamp': timestamp})

        host_url = self.TRADE_URL
        host_name = urllib.parse.urlparse(host_url).hostname
        host_name = host_name.lower()
        params['Signature'] = self.createSign(params, method, host_name, request_path, self._secret)

        url = host_url + request_path
        return self.http_get_request(url, params)

    def api_key_post(self, params, request_path):
        method = 'POST'
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        params_to_sign = {'AccessKeyId': self._key,
                          'SignatureMethod': 'HmacSHA256',
                          'SignatureVersion': '2',
                          'Timestamp': timestamp}

        host_url = self.TRADE_URL
        host_name = urllib.parse.urlparse(host_url).hostname
        host_name = host_name.lower()
        params_to_sign['Signature'] = self.createSign(params_to_sign, method, host_name, request_path, self._secret)
        url = host_url + request_path + '?' + urllib.parse.urlencode(params_to_sign)
        return self.http_post_request(url, params)

    def createSign(self, pParams, method, host_url, request_path, secret_key):
        sorted_params = sorted(pParams.items(), key=lambda d: d[0], reverse=False)
        encode_params = urllib.parse.urlencode(sorted_params)
        payload = [method, host_url, request_path, encode_params]
        payload = '\n'.join(payload)
        payload = payload.encode(encoding='UTF8')
        secret_key = secret_key.encode(encoding='UTF8')

        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature

    def get_orderbook(self, symbol):
        params = {'symbol': symbol,
                  'type': 'step0'}
        url = self.MARKET_URL + '/market/depth'
        result = self.http_get_request(url, params)
        if result.status_code != 200:
            return None
        return result.json()

    def apply_fee(self, order):
        if order.order_type == "sell":
            order.total = Decimal(order.total) * (Decimal(1.0) - order.exchange.taker_fee)
        elif order.order_type == "buy":
            order.total = Decimal(order.total) * (Decimal(1.0) + order.exchange.taker_fee)
        return order

    def get_feeinfo(self):
        return {
            'maker_fee': 0.002,
            'taker_fee': 0.002
        }

    def get_filters(self):
        url = "https://api.huobi.pro/v1/common/symbols"
        resp = requests.get(url)
        if resp.status_code != 200:
            print(resp.json())
            return None

        result = []
        for f in resp.json()["data"]:
            result.append({
                "min_price": Decimal("{}".format(1/10**f["price-precision"])),
                "min_amount": Decimal("{}".format(1/10**f["amount-precision"])),
                "min_lot": Decimal("0.00000001"),
                "pairs": "{1}{0}".format(f["quote-currency"], f["base-currency"]),
                "exchange": "Huobi"
            })
        return result

    def get_last_price(self, symbol, action, amount):
        glass = self.get_orderbook(symbol)
        if glass:
            data = glass['tick']['asks'] if action == 'sell' else glass['tick']['bids']
            for d in data:
                amount -= Decimal(d[1])
                if amount <= 0:
                    return Decimal(d[0])
        return None

    def _get_order_info(self, order_id):
        params = {}
        url = "/v1/order/orders/{0}".format(order_id)
        data = self.api_key_get(params, url)
        if data.status_code != 200:
            print(data.json())
            return None
        return data.json()

    def new_order(self, rate, order_type, amount, symbol, market=False):
        accounts = self._get_accounts()
        acct_id = accounts['data'][0]['id']
        params = {"account-id": acct_id,
                  "amount": amount,
                  "symbol": symbol,
                  "source": 'api'}
        if market:
            params['type'] = 'buy-market' if order_type == 'buy' else 'sell-market'
            params["amount"] = amount*rate
        else:
            params["price"] = rate
            params['type'] = 'buy-limit' if order_type == 'buy' else 'sell-limit'

        print(params)
        url = '/v1/order/orders/place'
        result = self.api_key_post(params, url)
        print(result.json())
        if result.status_code != 200 or result.json()["status"] == "error":
            return None
        order_info = self._get_order_info(result.json()['data'])['data']
        return HuobiOrder.create_object_from_json(order_info)

    def move_order(self, order, rate, amount):
        is_closed = self.cancel_order(order)
        if not is_closed:
            return False
        order = self.new_order(rate, order.order_type, amount, order.symbol.name)
        if order:
            return order.number
        return None

    def get_open_orders(self, pairs=None):
        params = {'symbol': pairs,
                  'states': 'pre-submitted,submitted,partial-filled,partial-canceled'}

        path = '/v1/order/orders'
        data = self.api_key_get(params, path)

        if data.status_code != 200:
            return None
        result = []
        for order in data.json()['data']:
            result.append(HuobiOrder.create_object_from_json(order))
        return result

    def get_symbols(self):
        path = '/v1/common/symbols'
        symbols = self.api_key_get({}, path)
        if symbols.status_code != 200:
            return None
        result = []
        for symbol in symbols.json()['data']:
            result.append(symbol['base-currency'] + symbol['quote-currency'])
        return result

    def cancel_order(self, order):
        params = {}
        url = "/v1/order/orders/{0}/submitcancel".format(order.number)
        result = self.api_key_post(params, url)
        if result.status_code != 200:
            return False
        return True

    def close_order(self, order):
        is_canceled = self.cancel_order(order)
        if not is_canceled:
            return False
        order = self.new_order(order.symbol.rate, order.order_type, order.amount, order.symbol.name, market=True)
        if order:
            return True
        return False

    def get_full_balance(self):
        balances = self.get_balance()
        result = []
        print(balances)
        for balance in balances:
            result.append(Balance(balance['currency'], balance['balance'], balance['type']))
        return result

    def _get_accounts(self):
        path = "/v1/account/accounts"
        result = self.api_key_get({}, path)
        if result.status_code != 200:
            return None
        return result.json()

    def get_balance(self):
        accounts = self._get_accounts()
        acct_id = accounts['data'][0]['id']
        url = "/v1/account/accounts/{0}/balance".format(acct_id)
        params = {"account-id": acct_id}
        data = self.api_key_get(params, url)
        if data.status_code != 200:
            return None
        result = []
        for balance in data.json()['data']['list']:
            if balance["type"] == "frozen":
                continue
            if Decimal(balance['balance']) != Decimal(0):
                result.append(balance)
        return result

    def get_tickers(self, currency=None):
        url = self.MARKET_URL + '/market/detail/merged'
        params = {'symbol': currency}
        result = self.http_get_request(url, params)
        if result.status_code != 200:
            return None
        return result.json()

    def get_all_usdt_balance(self):
        btc = self.get_all_btc_balance()
        price = Decimal(self.get_tickers(currency='btcusdt')['tick']['close'])
        return price * btc

    def get_all_btc_balance(self):
        balances = self.get_balance()
        ticker = self.get_tickers(currency='btcusdt')
        result = 0.0
        for balance in balances:
            if balance['currency'] == 'btc' and balance['type'] == 'trade':
                result += Decimal(balance['balance'])
                continue
            if balance['currency'] == 'usdt' and balance['type'] == 'trade':
                result += Decimal(float(balance['balance'])) / Decimal(ticker['tick']['close'])
                continue
        return result

    def get_trade_history(self, start=None, end=None, limit=1000, pairs=None):
        params = {'symbol': pairs,
                  'states': 'pre-submitted,submitted,partial-filled,partial-canceled,filled,canceled'}

        if start:
            params['start_date'] = start
        if end:
            params['end_date'] = end
        path = '/v1/order/orders'
        data = self.api_key_get(params, path)

        if data.status_code != 200:
            return None
        result = []
        for trade in data.json()['data']:
            result.append(HuobiTrade.create_object_from_json(trade))
        return result

    def get_margin_position(self, pairs=None):
        params = {'symbol': pairs,
                  'states': 'pre-submitted,submitted,partial-filled,partial-canceled,canceled,filled'}
        url = "/v1/order/orders"
        data = self.api_key_get(params, url)
        if data.status_code != 200:
            return None
        result = []
        for position in data.json()['data']:
            if position['source'] == 'margin-api':
                result.append(position)
        return result

    def close_margin_position(self, symbol):
        pass

    def get_margin_info(self):
        url = "/v1/margin/accounts/balance"
        data = self.api_key_get({}, url)
        if data.status_code != 200:
            return None
        return HuobiMarginInfo.create_object_from_json(data.json())

    def _get_margin_account(self, symbol):
        accounts = self._get_accounts()['data']
        for acc in accounts:
            if acc['type'] == 'margin' and acc['subtype'] == symbol:
                return acc['id']

    # {'status': 'ok', 'data': '4976368728'}
    def open_margin_position(self, symbol, rate, amount, side):
        acct_id = self._get_margin_account(symbol)
        params = {"account-id": acct_id,
                  "amount": amount,
                  "symbol": symbol,
                  "price": rate,
                  "type": 'sell-ioc' if int(side) == 0 else 'buy-ioc',
                  "source": 'margin-api'}

        url = '/v1/order/orders/place'
        result = self.api_key_post(params, url)
        if result.status_code != 200:
            return None
        return result.json()

    def toggle_margin_positions(self, margin_position):
        pass

    def is_order_fulfilled(self, order):
        data = self._get_order_info(order.number)['data']
        if data['state'] == 'filled':
            return True
        return False


class HuobiTrade(Trade):

    @classmethod
    def create_object_from_json(cls, data):
        return cls(
            data["symbol"], data["id"], data["id"],
            str(data["id"]), 0 if str(data["type"]).find("sell") else 1,
            "exchange", float(data["price"]) * float(data["amount"]),
            float(data["price"]), float(data["field-fees"]), float(data["amount"]),
            datetime.datetime.fromtimestamp(int(float('1526977758444') / 1000))
        )


class HuobiOrder(Order):

    @classmethod
    def create_object_from_json(cls, data):
        return cls(
            data["id"], data['price'], data["type"].lower(),
            data["amount"], Decimal(data["field-cash-amount"])*Decimal(data["price"]),
            data["symbol"]
        )


class HuobiMarginPosition(MarginPosition):

    @classmethod
    def create_object_from_json(cls, data):
        # Нет явного получения открытых позиций и получения инфы по ним
        return cls(
            data["amount"], data["pl"], data['symbol'],
            data["price"], data["type"]
        )


class HuobiMarginInfo(MarginInfo):

    @classmethod
    def create_object_from_json(cls, data):
        print(data)
        '''return cls(
            data["margin_balance"], data["net_value"], data["tradable_balance"], data["unrealized_pl"]
        )'''
