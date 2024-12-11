import socket
import time
import random
import uuid
import threading
from datetime import datetime


class Buyer:
    def __init__(self, traders, buyer_id=1, goods=None, tb=5, max_transactions=10, p=0.8, port=7000):
        """
        Initialize the buyer.
        :param traders: List of (host, port) tuples for available traders.
        :param buyer_id: Unique identifier for the buyer.
        :param goods: List of goods this buyer wants to buy.
        :param tb: Time interval in seconds between consecutive buy requests.
        :param max_transactions: Maximum number of buy transactions.
        :param p: Probability of making another buy request after a transaction.
        :param port: Port where the buyer listens for incoming messages.
        """
        self.traders = traders
        self.buyer_id = buyer_id
        self.goods = goods if goods else ["apple", "banana", "orange"]
        self.tb = tb
        self.max_transactions = max_transactions
        self.p = p
        self.completed_transactions = 0
        self.backup_port = None
        self.port = port
        self.running = True

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

    def start_listener(self):
        """Start a socket to listen for incoming messages from traders."""
        listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener_socket.bind(("localhost", self.port))
        listener_socket.listen(5)
        self.timestamped_print(f"Listening for trader messages on port {self.port}")

        while self.running:
            try:
                client_socket, address = listener_socket.accept()
                threading.Thread(target=self.handle_message, args=(client_socket, address), daemon=True).start()
            except Exception as e:
                self.timestamped_print(f"Encountered an error in listener: {e}")
                break

        listener_socket.close()

    def handle_message(self, client_socket, address):
        """Handle an incoming message from a trader."""
        try:
            message = client_socket.recv(1024).decode()
            self.timestamped_print(f"Received message from trader {address}: {message}")

            if message.startswith("SOLOTRADER|"):
                self.backup_port = int(message.split("|")[1])
                print(f"Buyer {self.buyer_id} updated backup port to {self.backup_port}")
        except Exception as e:
            self.timestamped_print(f"Failed to process message from trader {address}: {e}")
        finally:
            client_socket.close()

    def process_response(self, response, request_id, trader):
        """Process the response from the trader."""
        if response.startswith("OK"):
            self.timestamped_print(f"Buy complete for request {request_id} (via {trader[0]}:{trader[1]}): {response}")
            self.completed_transactions += 1
        elif response.startswith("ERROR"):
            self.timestamped_print(f"Buy failure for request {request_id} (via {trader[0]}:{trader[1]}): {response}")
        else:
            self.timestamped_print(f"Unexpected response for request {request_id} (via {trader[0]}:{trader[1]}): {response}")

    def buy_goods(self, product, quantity, request_id):
        """Send a buy request to a trader, with retries and fallback to backup port."""
        retries = 0
        while retries < 3:
            if retries < 2:
                # First two retries attempt the same trader
                trader = random.choice(self.traders)
            else:
                # Third attempt uses the backup port
                if self.backup_port:
                    trader = ('localhost', self.backup_port)
                    self.timestamped_print(f"Switching to backup trader at {trader[0]}:{trader[1]}")
                else:
                    self.timestamped_print(f"No backup trader available. Aborting request.")
                    return

            self.timestamped_print(f"Attempting to buy {quantity} {product}(s) from trader at {trader[0]}:{trader[1]} (Retry {retries + 1})")
            client_socket = self.connect_to_trader(trader)
            if client_socket:
                try:
                    command = f"buy|{product}|{quantity}|{request_id}"
                    client_socket.send(command.encode())
                    response = client_socket.recv(1024).decode()
                    self.process_response(response, request_id, trader)
                    client_socket.close()
                    return  # Exit on success
                except Exception as e:
                    self.timestamped_print(f"Error during transaction: {e}")
                finally:
                    client_socket.close()
            retries += 1

        self.timestamped_print(f"Failed to complete transaction for {quantity} {product}(s) after 3 attempts.")

    def generate_and_send_requests(self):
        """Periodically generate buy requests and send them to random traders."""
        while self.completed_transactions < self.max_transactions and self.running:
            if random.random() > self.p:
                print(f"Buyer {self.buyer_id}: Stopping due to probability threshold.")
                break

            product = random.choice(self.goods)
            quantity = random.randint(1, 10)  # Random quantity between 1 and 10
            request_id = self.generate_request_id()  # Generate a unique request ID

            self.buy_goods(product, quantity, request_id)
            time.sleep(self.tb)

        self.timestamped_print(f"Completed {self.completed_transactions} transactions. Shutting down.")

    def run(self):
        """Start the buyer process."""
        listener_thread = threading.Thread(target=self.start_listener, daemon=True)
        listener_thread.start()

        try:
            self.generate_and_send_requests()
        except KeyboardInterrupt:
            self.timestamped_print(f"Shutting down.")
            self.running = False


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
    parser.add_argument("--port", type=int, required=True, help="Port for buyer to listen for trader messages")
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
        p=args.p,
        port=args.port
    )
    buyer.run()
