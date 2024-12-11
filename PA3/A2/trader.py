import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Queue
import json
import uuid
import time

class Trader:
    def __init__(self, host="localhost", port=5001, db_host="localhost", db_port=5000, trader_id=1, max_workers=10, use_cache = True, shipped_goods = None):
        """
        Initialize the trader.
        :param host: Host where the trader listens for client connections.
        :param port: Port where the trader listens for client connections.
        :param db_host: Host of the database server.
        :param db_port: Port of the database server.
        :param trader_id: Unique identifier for the trader.
        :param max_workers: Maximum number of threads in the thread pool.
        """
        self.host = host
        self.port = port
        self.db_host = db_host
        self.db_port = db_port
        self.trader_id = trader_id
        self.max_workers = max_workers
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.client_queues = {}
        self.use_cache = use_cache
        self.cache = {}
        self.cache_lock = threading.Lock()
        self.shipped_goods = shipped_goods
        if self.use_cache:
            threading.Thread(target=self.periodic_cache_sync, daemon=True).start()

        self.total_buy_requests = 0
        self.oversell_detected = 0
        self.report_host = 'localhost'
        self.report_port = 8888
        print(f"Trader {self.trader_id} listening on {self.host}:{self.port}")

    def connect_to_database(self):
        """Establish a persistent connection to the database server."""
        try:
            self.db_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.db_socket.connect((self.db_host, self.db_port))
            print(f"Trader {self.trader_id} connected to database server at {self.db_host}:{self.db_port}")
        except ConnectionError as e:
            print(f"Trader {self.trader_id} failed to connect to database server: {e}")
            self.db_socket = None

    def timestamped_print(self, message):
        """Print a message with a timestamp."""
        timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
        print(f"{timestamp} Trader {self.trader_id}: {message}")

    def periodic_cache_sync(self, interval=5):
        """Periodically fetch the entire inventory from the warehouse to sync cache."""
        while True:
            time.sleep(interval)
            if self.db_socket and self.use_cache:
                self.refresh_entire_inventory()

    def refresh_entire_inventory(self):
        """Fetch the entire inventory state from the warehouse and update the cache."""
        request_id = str(uuid.uuid4())
        command = f"fetch|inventory|0|{request_id}"
        try:
            self.db_socket.send(command.encode())
            response = self.db_socket.recv(4096).decode()
            # Expecting: OK|{...json...}|<request_id>
            if response.startswith("OK|"):
                parts = response.split("|", 2)
                if len(parts) >= 3:
                    inv_json = parts[1]
                    inv = json.loads(inv_json)
                    with self.cache_lock:
                        self.cache = inv
                    self.timestamped_print(f"Cache fully synced with warehouse.")
            else:
                self.timestamped_print(f" Failed to fetch inventory, response: {response}")
        except Exception as e:
            self.timestamped_print(f"Error fetching inventory: {e}")

    def forward_to_database(self, command, client_address):
        """
        Forward a command to the database server. This method also applies caching logic if use_cache=True.
        Command format: action|product|quantity|request_id
        """
        parts = command.split("|")
        if len(parts) != 4:
            # Invalid command format, just forward and hope DB handles it (or return error)
            if self.db_socket:
                self.db_socket.send(command.encode())
                response = self.db_socket.recv(1024).decode()
                self.forward_to_client(client_address, response)
            else:
                self.forward_to_client(client_address, "ERROR|No DB connection")
            return

        action, product, qty_str, request_id = parts
        quantity = int(qty_str)

        if self.use_cache and action in ["buy", "sell"]:
            self.handle_caching_logic(action, product, quantity, request_id, client_address)
        else:
            # Without caching, just forward directly to DB
            if self.db_socket:
                self.db_socket.send(command.encode())
                response = self.db_socket.recv(1024).decode()
                self.forward_to_client(client_address, response)
            else:
                self.forward_to_client(client_address, "ERROR|No DB connection")
    
    def compute_oversell_rate(self):
        """Compute the oversell rate."""
        return 0.0 if self.total_buy_requests == 0 else self.oversell_detected / self.total_buy_requests

    def report_oversell_rate(self):
        """Send oversell rate to the central collector."""
        rate = self.compute_oversell_rate()
        report_message = f"Trader {self.trader_id}|Oversell Rate: {rate:.2%}|{self.oversell_detected}|{self.total_buy_requests}"
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as report_socket:
                report_socket.connect((self.report_host, self.report_port))
                report_socket.send(report_message.encode())
        except Exception as e:
            self.timestamped_print(f"Error reporting oversell rate: {e}")

    def periodic_reporting(self, interval=10):
        """Periodically report oversell rate."""
        while True:
            time.sleep(interval)
            self.report_oversell_rate()

    def handle_caching_logic(self, action, product, quantity, request_id, client_address):
        """Handle buy/sell requests with caching logic to reduce load on warehouse."""
        if action == "buy":
            # Check cache first
            self.total_buy_requests += 1
            with self.cache_lock:
                current_stock = self.cache.get(product, 0)

            if current_stock >= quantity:
                # Send request to DB
                # self.shipped_goods.value += quantity
                if self.db_socket:
                    self.db_socket.send(f"{action}|{product}|{quantity}|{request_id}".encode())
                    response = self.db_socket.recv(1024).decode()
                    if response.startswith("OK|"):
                        # Confirmed by DB, update cache
                        with self.cache_lock:
                            self.cache[product] = self.cache.get(product, 0) - quantity
                        self.forward_to_client(client_address, response)
                    else:
                        # Over-sell occurred, refresh cache
                        self.oversell_detected += 1
                        self.timestamped_print(f"[OVER-SELL DETECTED] Warehouse rejected buy request for {quantity} {product}(s). Refreshing cache.")
                        self.refresh_entire_inventory()
                        self.forward_to_client(client_address, response)
                else:
                    self.forward_to_client(client_address, "ERROR|No DB connection")
            else:
                # Under-sell scenario: cache says not enough inventory.
                # We choose to trust cache and reject immediately for performance.
                print("UNDER SELLING DETECTED")
                response = f"ERROR|Insufficient inventory for {product}|{request_id}"
                self.forward_to_client(client_address, response)

        elif action == "sell":
            # For sells, we can optimistically update cache before confirmation
            with self.cache_lock:
                self.cache[product] = self.cache.get(product, 0) + quantity

            if self.db_socket:
                self.db_socket.send(f"{action}|{product}|{quantity}|{request_id}".encode())
                response = self.db_socket.recv(1024).decode()
                if response.startswith("OK|"):
                    # Cache already updated optimistically
                    self.forward_to_client(client_address, response)
                else:
                    # DB failed, revert cache
                    with self.cache_lock:
                        self.cache[product] = self.cache.get(product, 0) - quantity
                    self.forward_to_client(client_address, response)
            else:
                # No DB connection, revert cache immediately
                with self.cache_lock:
                    self.cache[product] = self.cache.get(product, 0) - quantity
                self.forward_to_client(client_address, "ERROR|No DB connection")

    def forward_to_client(self, client_address, response):
        """
        Forward the response from the database server to the appropriate client.
        :param client_address: Address of the client to forward the response to.
        :param response: Response string to send to the client.
        """
        if client_address in self.client_queues:
            self.client_queues[client_address].put(response)
        else:
            self.timestamped_print(f"Client {client_address} not found for response forwarding.")

    def handle_client(self, client_socket, address):
        """Handle a connection from a buyer or seller."""
        # print(f"Trader {self.trader_id} connected to client at {address}")
        self.client_queues[address] = Queue()

        try:
            while True:
                # Receive a command from the client
                data = client_socket.recv(1024).decode()
                if not data:
                    break
                self.timestamped_print(f"Received from {address}: {data}")

                # Forward the command to the database server
                self.forward_to_database(data, address)

                # Wait for the response from the database server
                response = self.client_queues[address].get()
                client_socket.send(response.encode())
        except ConnectionResetError:
            self.timestamped_print(f"Connection reset by client at {address}")
        except Exception as e:
            self.timestamped_print(f"Encountered an error with client {address}: {e}")
        finally:
            client_socket.close()
            del self.client_queues[address]
            # print(f"Trader {self.trader_id} disconnected from client at {address}")

    def run(self):
        """Start the trader process."""
        self.connect_to_database()
        if not self.db_socket:
            self.timestamped_print(f"Cannot start without a database connection.")
            return
            # Initial cache sync before starting if use_cache is True
        if self.use_cache:
            self.refresh_entire_inventory()

        self.timestamped_print(f"Running with a thread pool of {self.max_workers} workers.")
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                while True:
                    client_socket, address = self.server_socket.accept()
                    executor.submit(self.handle_client, client_socket, address)
            except KeyboardInterrupt:
                self.timestamped_print(f"Shutting down.")
                self.server_socket.close()
                if self.db_socket:
                    self.db_socket.close()


if __name__ == "__main__":
    import argparse

    # Parse command-line arguments for trader configuration
    parser = argparse.ArgumentParser(description="Trader Process")
    parser.add_argument("--host", type=str, default="localhost", help="Trader host")
    parser.add_argument("--port", type=int, required=True, help="Trader port")
    parser.add_argument("--db_host", type=str, default="localhost", help="Database server host")
    parser.add_argument("--db_port", type=int, default=5000, help="Database server port")
    parser.add_argument("--id", type=int, required=True, help="Trader ID")
    parser.add_argument("--max_workers", type=int, default=10, help="Maximum number of threads in the thread pool")
    args = parser.parse_args()

    # Create and run the trader
    trader = Trader(
        host=args.host,
        port=args.port,
        db_host=args.db_host,
        db_port=args.db_port,
        trader_id=args.id,
        max_workers=args.max_workers
    )
    trader.run()
