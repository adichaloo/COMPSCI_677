
class LookupMessage:
    """Message sent by a buyer to look for sellers offering a specific product.
    
    Attributes:
        request_id (int): Unique identifier for the lookup request.
        buyer_id (int): ID of the buyer initiating the request.
        product_name (str): Name of the product being searched.
        hop_count (int): Number of hops remaining before the message is discarded.
        search_path (list): Path taken by the lookup message.
    """
    
    def __init__(self, request_id, buyer_id, product_name, hop_count, search_path=None):
        self.request_id = request_id
        self.buyer_id = buyer_id
        self.product_name = product_name
        self.hop_count = hop_count
        self.search_path = search_path if search_path else []

    def to_dict(self):
        """Convert the LookupMessage instance to a dictionary for serialization.

        Returns:
            dict: A dictionary representation of the LookupMessage instance.
        """
        return {
            "request_id": self.request_id,
            "type": "lookup",
            "buyer_id": self.buyer_id,
            "product_name": self.product_name,
            "hop_count": self.hop_count,
            "search_path": self.search_path
        }

    @staticmethod
    def from_dict(data):
        """Create a LookupMessage instance from a dictionary.

        Args:
            data (dict): A dictionary containing the message data.

        Returns:
            LookupMessage: An instance of LookupMessage populated with the provided data.
        """
        return LookupMessage(
            request_id=data["request_id"],
            buyer_id=data["buyer_id"],
            product_name=data["product_name"],
            hop_count=data["hop_count"],
            search_path=data.get("search_path", [])
        )

class ReplyMessage:
    """Message sent by a seller in response to a lookup request.

    Attributes:
        seller_id (int): ID of the seller responding.
        seller_addr (tuple): Address of the seller for direct communication.
        reply_path (list): Path taken to reach the seller.
        product_name (str): Name of the product in the seller's response.
        request_id (str): ID of the original lookup request.
    """
    
    def __init__(self, seller_id, reply_path, seller_addr, product_name, request_id):
        self.seller_id = seller_id
        self.seller_addr = seller_addr
        self.reply_path = reply_path
        self.product_name = product_name
        self.request_id = request_id

    def to_dict(self):
        """Convert the ReplyMessage instance to a dictionary for serialization.

        Returns:
            dict: A dictionary representation of the ReplyMessage instance.
        """
        return {
            "type": "reply",
            "seller_id": self.seller_id,
            "seller_addr": self.seller_addr,
            "reply_path": self.reply_path,
            "product_name": self.product_name, 
            "request_id": self.request_id
        }

    @staticmethod
    def from_dict(data):
        """Create a ReplyMessage instance from a dictionary.

        Args:
            data (dict): A dictionary containing the message data.

        Returns:
            ReplyMessage: An instance of ReplyMessage populated with the provided data.
        """
        return ReplyMessage(
            seller_id=data["seller_id"],
            reply_path=data.get("reply_path", []),
            seller_addr=data.get("seller_addr", None),  # Provide a default value (None) for missing keys
            product_name=data.get("product_name", ""),
            request_id=data.get("request_id", "")
        )


class BuyMessage:
    """Message sent by a buyer to request a purchase from a seller.

    Attributes:
        buyer_id (int): ID of the buyer making the purchase.
        seller_id (int): ID of the seller being contacted.
        product_name (str): Name of the product to purchase.
    """
    
    def __init__(self, request_id, buyer_id, seller_id, product_name):
        self.buyer_id = buyer_id
        self.seller_id = seller_id
        self.product_name = product_name
        self.request_id = request_id

    def to_dict(self):
        """Convert the BuyMessage instance to a dictionary for serialization.

        Returns:
            dict: A dictionary representation of the BuyMessage instance.
        """
        return {
            "type": "buy",
            "buyer_id": self.buyer_id,
            "seller_id": self.seller_id,
            "product_name": self.product_name, 
            "request_id": self.request_id
        }

    @staticmethod
    def from_dict(data):
        """Create a BuyMessage instance from a dictionary.

        Args:
            data (dict): A dictionary containing the message data.

        Returns:
            BuyMessage: An instance of BuyMessage populated with the provided data.
        """
        return BuyMessage(
            buyer_id=data["buyer_id"],
            seller_id=data["seller_id"],
            product_name=data["product_name"]
        )

class BuyConfirmationMessage:
    """Message confirming the successful (or unsuccessful) completion of a purchase.

    Attributes:
        request_id (int): Unique identifier for the buy request.
        product_name (str): Name of the purchased product.
        buyer_id (int): ID of the buyer who made the purchase.
        seller_id (int): ID of the seller who fulfilled the purchase.
        status (str): Status of the transaction (e.g., "success" or "failure").
        message (str): Optional message with additional information about the transaction.
    """
    
    def __init__(self, request_id, product_name, buyer_id, seller_id, status, message=None):
        self.request_id = request_id
        self.product_name = product_name
        self.buyer_id = buyer_id
        self.seller_id = seller_id
        self.status = status
        self.message = message

    def to_dict(self):
        """Convert the BuyConfirmationMessage instance to a dictionary for serialization.

        Returns:
            dict: A dictionary representation of the BuyConfirmationMessage instance.
        """
        return {
            "type": "buy_confirmation",
            "request_id": self.request_id,
            "product_name": self.product_name,
            "buyer_id": self.buyer_id,
            "seller_id": self.seller_id,
            "status": self.status,
            "message": self.message
        }

    @staticmethod
    def from_dict(data):
        """Create a BuyConfirmationMessage instance from a dictionary.

        Args:
            data (dict): A dictionary containing the message data.

        Returns:
            BuyConfirmationMessage: An instance of BuyConfirmationMessage populated with the provided data.
        """
        return BuyConfirmationMessage(
            request_id=data["request_id"],
            product_name=data["product_name"],
            buyer_id=data["buyer_id"],
            seller_id=data["seller_id"],
            status=data["status"],
            message=data.get("message", None)
        )