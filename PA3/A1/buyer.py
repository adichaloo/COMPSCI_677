import socket
import time
import random
import uuid
from datetime import datetime


class Buyer:
    def __init__(self, traders, buyer_id=1, goods=None, tb=5, max_transactions=10, p=0.8):
        """
        Initialize the buyer.
        :param traders: List of (host, port) tuples for available traders.
        :param buyer_id: Unique identifier for the buyer.
        :param goods: List of goods this buyer wants to buy.
        :param tb: Time interval in seconds between consecutive buy requests.
        :param max_transactions: Maximum number of buy transactions.
        :param p: Probability of making another buy request after a transaction.
        """
        self.traders = traders
        self.buyer_id = buyer_id
        self.goods = goods if goods else ["apple", "banana", "orange"]
        self.tb = tb
        self.max_transactions = max_transactions
        self.p = p
        self.completed_transactions = 0

    def generate_request_id(self):
        """Generate a unique request ID."""
        return str(uuid.uuid4())

    def timestamped_print(self, message):
        """Print a message with a timestamp."""
        timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
        print(f"{timestamp} Buyer {self.buyer_id}: {message}")

    def connect_to_trader(self, trader):
        """Establish a connection to the specified trader."""
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(trader)
            self.timestamped_print(f"Connected to trader at {trader[0]}:{trader[1]}")
            return client_socket
        except ConnectionError as e:
            self.timestamped_print(f"Failed to connect to the trader at {trader[0]}:{trader[1]}: {e}")
            return None

    def process_response(self, response, request_id, trader):
        """
        Process the response from the trader.
        :param response: Response received from the trader.
        :param request_id: ID of the associated request.
        :param trader: Tuple (host, port) of the trader.
        """
        if response.startswith("OK"):
            self.timestamped_print(f"Buy complete for request {request_id} (via {trader[0]}:{trader[1]}): {response}")
            self.completed_transactions += 1
        elif response.startswith("ERROR"):
            self.timestamped_print(f"Buy failure for request {request_id} (via {trader[0]}:{trader[1]}): {response}")
        else:
            self.timestamped_print(f"Unexpected response for request {request_id} (via {trader[0]}:{trader[1]}): {response}")

    def buy_goods(self, client_socket, product, quantity, request_id, trader):
        """
        Send a buy request to the trader.
        :param client_socket: Socket connected to the trader.
        :param product: Product to buy.
        :param quantity: Quantity to buy.
        :param request_id: Unique request ID for the buy request.
        :param trader: Tuple (host, port) of the trader.
        """
        command = f"buy|{product}|{quantity}|{request_id}"
        try:
            client_socket.send(command.encode())
            response = client_socket.recv(1024).decode()
            self.process_response(response, request_id, trader)
        except Exception as e:
            self.timestamped_print(f"Encountered an error while communicating with trader at {trader[0]}:{trader[1]}: {e}")

    def generate_and_send_requests(self):
        """Periodically generate buy requests and send them to random traders."""
        while self.completed_transactions < self.max_transactions:
            if random.random() > self.p:
                self.timestamped_print("Stopping due to probability threshold.")
                break

            product = random.choice(self.goods)
            quantity = random.randint(1, 10)  # Random quantity between 1 and 10
            request_id = self.generate_request_id()  # Generate a unique request ID
            trader = random.choice(self.traders)  # Select a random trader
            self.timestamped_print(f"Wants to buy {quantity} {product}(s) from trader at {trader[0]}:{trader[1]} with request ID {request_id}")

            # Connect to the selected trader
            client_socket = self.connect_to_trader(trader)
            if client_socket:
                self.buy_goods(client_socket, product, quantity, request_id, trader)
                client_socket.close()
            time.sleep(self.tb)

        self.timestamped_print(f"Completed {self.completed_transactions} transactions. Shutting down.")

    def run(self):
        """Start the buyer process."""
        try:
            self.generate_and_send_requests()
        except KeyboardInterrupt:
            print(f"Buyer {self.buyer_id} shutting down.")


if __name__ == "__main__":
    import argparse

    # Parse command-line arguments for buyer configuration
    parser = argparse.ArgumentParser(description="Buyer Process")
    parser.add_argument("--traders", nargs="+", required=True, help="List of trader host:port pairs (e.g., localhost:5001 localhost:5002)")
    parser.add_argument("--id", type=int, required=True, help="Buyer ID")
    parser.add_argument("--tb", type=int, default=5, help="Interval in seconds between buy requests")
    parser.add_argument("--goods", nargs="+", default=["apple", "banana", "orange"], help="List of goods to buy")
    parser.add_argument("--max_transactions", type=int, default=10, help="Maximum number of buy transactions")
    parser.add_argument("--p", type=float, default=0.8, help="Probability of making another buy request after a transaction")
    args = parser.parse_args()

    # Parse trader addresses
    traders = []
    for trader in args.traders:
        host, port = trader.split(":")
        traders.append((host, int(port)))

    # Create and run the buyer
    buyer = Buyer(
        traders=traders,
        buyer_id=args.id,
        goods=args.goods,
        tb=args.tb,
        max_transactions=args.max_transactions,
        p=args.p
    )
    buyer.run()
