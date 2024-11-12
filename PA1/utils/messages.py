# messages.py

class LookupMessage:
    def __init__(self, request_id, buyer_id, product_name, hop_count, search_path):
        self.type = 'lookup'
        self.request_id = request_id
        self.buyer_id = buyer_id
        self.product_name = product_name
        self.hop_count = hop_count
        self.search_path = search_path

    def to_dict(self):
        return self.__dict__

class ReplyMessage:
    def __init__(self, seller_id, reply_path, seller_addr, product_name, request_id):
        self.type = 'reply'
        self.seller_id = seller_id
        self.reply_path = reply_path
        self.seller_addr = seller_addr
        self.product_name = product_name
        self.request_id = request_id

    def to_dict(self):
        return self.__dict__

    @staticmethod
    def from_dict(d):
        return ReplyMessage(
            d['seller_id'],
            d['reply_path'],
            d['seller_addr'],
            d['product_name'],
            d['request_id']
        )

class BuyMessage:
    def __init__(self, request_id, buyer_id, seller_id, product_name):
        self.type = 'buy'
        self.request_id = request_id
        self.buyer_id = buyer_id
        self.seller_id = seller_id
        self.product_name = product_name

    def to_dict(self):
        return self.__dict__

class BuyConfirmationMessage:
    def __init__(self, request_id, product_name, buyer_id, seller_id, status):
        self.type = 'buy_confirmation'
        self.request_id = request_id
        self.product_name = product_name
        self.buyer_id = buyer_id
        self.seller_id = seller_id
        self.status = status

    def to_dict(self):
        return self.__dict__

    @staticmethod
    def from_dict(d):
        return BuyConfirmationMessage(
            d['request_id'],
            d['product_name'],
            d['buyer_id'],
            d['seller_id'],
            d['status']
        )
