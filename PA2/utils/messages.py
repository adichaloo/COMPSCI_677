# messages.py

class UpdateInventoryMessage:
    def __init__(self, seller_id, address, product_name, stock, vector_clock):
        self.type = "update_inventory"
        self.seller_id = seller_id
        self.address = address
        self.product_name = product_name
        self.stock = stock
        self.vector_clock = vector_clock

    def to_dict(self):
        return {
            "type": self.type,
            "seller_id": self.seller_id,
            "address": self.address,
            "product_name": self.product_name,
            "stock": self.stock,
            "vector_clock": self.vector_clock
        }

    @staticmethod
    def from_dict(data):
        return UpdateInventoryMessage(
            data["seller_id"],
            data["address"],
            data["product_name"],
            data["stock"],
            data["vector_clock"]
        )

class BuyMessage:
    def __init__(self, request_id, buyer_id, buyer_address, product_name, quantity, vector_clock):
        self.type = "buy"
        self.request_id = request_id
        self.buyer_id = buyer_id
        self.buyer_address = buyer_address
        self.product_name = product_name
        self.quantity = quantity
        self.vector_clock = vector_clock

    def to_dict(self):
        return {
            "type": self.type,
            "request_id": self.request_id,
            "buyer_id": self.buyer_id,
            "buyer_address": self.buyer_address,
            "product_name": self.product_name,
            "quantity": self.quantity,
            "vector_clock": self.vector_clock
        }

    @staticmethod
    def from_dict(data):
        return BuyMessage(
            data["request_id"],
            data["buyer_id"],
            data["buyer_address"],
            data["product_name"],
            data["quantity"],
            data["vector_clock"]
        )

class BuyConfirmationMessage:
    def __init__(self, request_id, buyer_id, product_name, status, quantity, vector_clock, seller_id= None, reason=None):
        self.type = "buy_confirmation"
        self.request_id = request_id
        self.buyer_id = buyer_id
        self.product_name = product_name
        self.status = status
        self.quantity = quantity
        self.vector_clock = vector_clock
        self.seller_id = seller_id
        self.reason = reason

    def to_dict(self):
        return {
            "type": self.type,
            "request_id": self.request_id,
            "buyer_id": self.buyer_id,
            "product_name": self.product_name,
            "status": self.status,
            "quantity": self.quantity,
            "vector_clock": self.vector_clock,
            "seller_id": self.seller_id,
            "reason":self.reason
        }

    @staticmethod
    def from_dict(data):
        return BuyConfirmationMessage(
            data["request_id"],
            data["buyer_id"],
            data["product_name"],
            data["status"],
            data["quantity"],
            data["vector_clock"],
            data['seller_id'],
            data["reason"],
        )

class SellConfirmationMessage:
    def __init__(self, request_id, buyer_id, product_name, status, quantity, vector_clock, payment_amount):
        self.type = "sell_confirmation"
        self.request_id = request_id
        self.buyer_id = buyer_id
        self.product_name = product_name
        self.status = status
        self.quantity = quantity
        self.vector_clock = vector_clock
        self.payment_amount = payment_amount

    def to_dict(self):
        return {
            "type": self.type,
            "request_id": self.request_id,
            "buyer_id": self.buyer_id,
            "product_name": self.product_name,
            "status": self.status,
            "quantity": self.quantity,
            "vector_clock": self.vector_clock,
            "payment_amount": self.payment_amount
        }

    @staticmethod
    def from_dict(data):
        return SellConfirmationMessage(
            data["request_id"],
            data["buyer_id"],
            data["product_name"],
            data["status"],
            data["quantity"],
            data["vector_clock"],
            data["payment_amount"]
        )
