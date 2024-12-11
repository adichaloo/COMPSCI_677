import socket
import time
import random
import threading
import uuid
from datetime import datetime


class Seller:
    def __init__(self, traders, seller_id=1, goods=None, ng=5, tg=5, port=6000):
        """
        Initialize the seller.
        :param traders: List of (host, port) tuples for available traders.
        :param seller_id: Unique identifier for the seller.
        :param goods: List of goods this seller can sell.
        :param ng: Number of goods accrued every Tg seconds.
        :param tg: Time interval in seconds for accruing goods.
        :param port: Port where the seller listens for incoming trader messages.
        """
        self.traders = traders
        self.seller_id = seller_id
        self.goods = goods if goods else ["apple", "banana", "orange"]
        self.ng = ng
        self.tg = tg
        self.port = port
        self.running = True
        self.backup_port = None

    def generate_request_id(self):
        """Generate a unique request ID."""
        return str(uuid.uuid4())

    def timestamped_print(self, message):
        """Print a message with a timestamp."""
        timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
        print(f"{timestamp} Seller {self.seller_id}: {message}")

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
                self.timestamped_print(f"Updated backup port to {self.backup_port}")
        except Exception as e:
            self.timestamped_print(f"Failed to process message from trader {address}: {e}")
        finally:
            client_socket.close()

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

    def process_response(self, response, trader, request_id):
        """Process the response from the trader."""
        if response.startswith("OK"):
            self.timestamped_print(f"Sell complete for request {request_id} (via {trader[0]}:{trader[1]}): {response}")
        elif response.startswith("ERROR"):
            self.timestamped_print(f"Sell failure for request {request_id} (via {trader[0]}:{trader[1]}): {response}")
        else:
            self.timestamped_print(f"Unexpected response for request {request_id} (via {trader[0]}:{trader[1]}): {response}")

    def sell_goods(self, product, quantity, request_id):
        """Send a sell request to a trader, with retries and fallback to backup port."""
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

            self.timestamped_print(f"Attempting to sell {quantity} {product}(s) to trader at {trader[0]}:{trader[1]} (Retry {retries + 1})")
            client_socket = self.connect_to_trader(trader)
            if client_socket:
                try:
                    command = f"sell|{product}|{quantity}|{request_id}"
                    client_socket.send(command.encode())
                    response = client_socket.recv(1024).decode()
                    self.process_response(response, trader, request_id)
                    client_socket.close()
                    return  # Exit on success
                except Exception as e:
                    self.timestamped_print(f"Error during transaction: {e}")
                finally:
                    client_socket.close()
            retries += 1

        self.timestamped_print(f"Failed to complete transaction for {quantity} {product}(s) after 3 attempts.")

    def accrue_and_sell_goods(self):
        """Periodically accrue goods and send sell requests to random traders."""
        while self.running:
            product = random.choice(self.goods)
            quantity = self.ng  # Accrued goods
            request_id = self.generate_request_id()  # Generate a unique request ID

            self.sell_goods(product, quantity, request_id)
            time.sleep(self.tg)

    def run(self):
        """Start the seller process."""
        listener_thread = threading.Thread(target=self.start_listener, daemon=True)
        listener_thread.start()
        try:
            self.accrue_and_sell_goods()
        except KeyboardInterrupt:
            self.timestamped_print(f"Shutting down.")
            self.running = False


if __name__ == "__main__":
    import argparse

    # Parse command-line arguments for seller configuration
    parser = argparse.ArgumentParser(description="Seller Process")
    parser.add_argument("--traders", nargs="+", required=True, help="List of trader host:port pairs (e.g., localhost:5001 localhost:5002)")
    parser.add_argument("--id", type=int, required=True, help="Seller ID")
    parser.add_argument("--ng", type=int, default=5, help="Number of goods accrued per interval")
    parser.add_argument("--tg", type=int, default=5, help="Interval in seconds for accruing goods")
    parser.add_argument("--goods", nargs="+", default=["apple", "banana", "orange"], help="List of goods to sell")
    parser.add_argument("--port", type=int, default=6000, help="Port for seller to listen for trader messages")
    args = parser.parse_args()

    # Parse trader addresses
    traders = []
    for trader in args.traders:
        host, port = trader.split(":")
        traders.append((host, int(port)))

    # Create and run the seller
    seller = Seller(
        traders=traders,
        seller_id=args.id,
        goods=args.goods,
        ng=args.ng,
        tg=args.tg,
        port=args.port
    )
    seller.run()
