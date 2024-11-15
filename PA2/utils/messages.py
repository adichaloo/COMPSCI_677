class BuyMessage:
    def __init__(self, request_id, buyer_id, address,  product_name, quantity):
        self.type = "buy"
        self.request_id = request_id
        self.buyer_id = buyer_id
        self.buyer_address = address
        self.product_name = product_name
        self.quantity = quantity

    def to_dict(self):
        return {
            "type": self.type,
            "request_id": self.request_id,
            "buyer_id": self.buyer_id,
            "buyer_address": self.buyer_address,
            "product_name": self.product_name,
            "quantity": self.quantity
        }

    @staticmethod
    def from_dict(data):
        return BuyMessage(
            data["request_id"],
            data["buyer_id"],
            data["buyer_address"],
            data["product_name"],
            data["quantity"]
        )


class BuyConfirmationMessage:
    def __init__(self, request_id, buyer_id, product_name, status, quantity):
        self.type = "buy_confirmation"
        self.request_id = request_id
        self.buyer_id = buyer_id
        self.product_name = product_name
        self.status = status  # True for success, False for failure
        self.quantity = quantity  # Quantity confirmed or rejected

    def to_dict(self):
        return {
            "type": self.type,
            "request_id": self.request_id,
            "buyer_id": self.buyer_id,
            "product_name": self.product_name,
            "status": self.status,
            "quantity": self.quantity
        }

    @staticmethod
    def from_dict(data):
        return BuyConfirmationMessage(
            data["request_id"],
            data["buyer_id"],
            data["product_name"],
            data["status"],
            data["quantity"]
        )

class SellConfirmationMessage:
    def __init__(self, request_id, buyer_id, product_name, status, quantity):
        self.type = "sell_confirmation"
        self.request_id = request_id
        self.buyer_id = buyer_id
        self.product_name = product_name
        self.status = status  # True for success, False for failure
        self.quantity = quantity  # Quantity confirmed or rejected

    def to_dict(self):
        return {
            "type": self.type,
            "request_id": self.request_id,
            "buyer_id": self.buyer_id,
            "product_name": self.product_name,
            "status": self.status,
            "quantity": self.quantity
        }

    @staticmethod
    def from_dict(data):
        return SellConfirmationMessage(
            data["request_id"],
            data["buyer_id"],
            data["product_name"],
            data["status"],
            data["quantity"]
        )

class UpdateInventoryMessage:
    def __init__(self, seller_id, address, product_name, stock):
        self.type = "update_inventory"
        self.seller_id = seller_id
        self.address = address
        self.product_name = product_name
        self.stock = stock

    

    def to_dict(self):
        return {
            "type": self.type,
            "seller_id": self.seller_id,
            "address": self.address,
            "product_name": self.product_name,
            "stock": self.stock
        }

    @staticmethod
    def from_dict(data):
        return UpdateInventoryMessage(
            data["seller_id"],
            data["address"],
            data["product_name"],
            data["stock"]
        )


class ElectionMessage:
    def __init__(self, sender_id,):
        self.type = "election"
        self.sender_id = sender_id

    def to_dict(self):
        return {
            "type": self.type,
            "sender_id": self.sender_id,
        }

    @staticmethod
    def from_dict(data):
        return ElectionMessage(
            data["sender_id"],
            data["election_round"]
        )


class OKMessage:
    def __init__(self, sender_id):
        self.type = "OK"
        self.sender_id = sender_id

    def to_dict(self):
        return {
            "type": self.type,
            "sender_id": self.sender_id
        }

    @staticmethod
    def from_dict(data):
        return OKMessage(
            data["sender_id"]
        )


class LeaderMessage:
    def __init__(self, leader_id, ip_address, port):
        self.type = "leader"
        self.leader_id = leader_id
        self.ip_address = ip_address
        self.port = port

    def to_dict(self):
        return {
            "type": self.type,
            "leader_id": self.leader_id,
            "ip_address": self.ip_address,
            "port": self.port
        }

    @staticmethod
    def from_dict(data):
        return LeaderMessage(
            data["leader_id"],
            data["ip_address"],
            data["port"]
        )

