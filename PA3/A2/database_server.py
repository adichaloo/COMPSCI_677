import socket
import threading
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


class DatabaseServer:
    def __init__(self, shipped_goods = 0,  host="localhost", port=5000, inventory_file="inventory.json", max_workers=10):
        """
        Initialize the database server.
        :param host: Host for the database server.
        :param port: Port for the database server.
        :param inventory_file: Path to the JSON file that stores inventory data.
        :param max_workers: Maximum number of threads in the thread pool.
        """
        self.shipped_goods = shipped_goods
        self.host = host
        self.port = port
        self.inventory_file = inventory_file
        self.max_workers = max_workers
        self.inventory = {}
        self.locks = {}
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.shipped_goods_lock = threading.Lock()
        self.inventory = {}
        self.locks = {}
        self.inventory_lock = threading.Lock()  # A global lock for reading full inventory safely

    def timestamped_print(self, message):
        """Print a message with a timestamp."""
        timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
        print(f"{timestamp} Warehouse: {message}")
    def load_inventory(self):
        """Load inventory from the JSON file."""
        try:
            with open(self.inventory_file, "r") as f:
                self.inventory = json.load(f)
                self.timestamped_print("Inventory loaded successfully.")
        except FileNotFoundError:
            self.timestamped_print("Inventory file not found. Starting with an empty inventory.")
            self.inventory = {}
        except json.JSONDecodeError as e:
            self.timestamped_print(f"Failed to load inventory: {e}")
            self.inventory = {}

        # Initialize locks for each product
        for product in self.inventory:
            self.locks[product] = threading.Lock()

    def save_inventory(self):
        """Save inventory to the JSON file."""
        with open(self.inventory_file, "w") as f:
            json.dump(self.inventory, f, indent=4)

    def process_request(self, client_socket, address):
        """
        Handle a client request.
        :param client_socket: The client socket.
        :param address: The address of the client.
        """
        self.timestamped_print(f"DatabaseServer: Connected to trader at {address}")
        try:
            while True:
                data = client_socket.recv(1024).decode()
                if not data:
                    break
                self.timestamped_print(f"DatabaseServer received: {data} from {address}")

                response = self.handle_command(data)
                client_socket.send(response.encode())
        except Exception as e:
            self.timestamped_print(f"DatabaseServer encountered an error with trader {address}: {e}")
        finally:
            client_socket.close()
            self.timestamped_print(f"DatabaseServer: Disconnected from trader at {address}")

    def handle_command(self, command):
        """
        Handle a `buy` or `sell` command.
        :param command: Command string in the format `<action>|<product>|<quantity>|<request_id>`.
        :return: Response string in the format `OK|<message>` or `ERROR|<message>`.
        """
        try:
            parts = command.split("|")
            if len(parts) != 4:
                return "ERROR|Invalid command format"

            action, product, quantity, request_id = parts
            quantity = int(quantity)

            if action == "buy":
                return self.process_buy(product, quantity, request_id)
            elif action == "sell":
                return self.process_sell(product, quantity, request_id)
            elif action == "fetch" and product == "inventory":
                # Return entire inventory as JSON
                return self.process_fetch(request_id)
            else:
                return "ERROR|Unknown action"
        except ValueError:
            return "ERROR|Invalid quantity"

    def process_fetch(self, request_id):
        """Return the full inventory as a JSON string."""
        with self.inventory_lock:
            # Make a copy of the current inventory to avoid issues if someone else modifies it while encoding.
            current_inv = dict(self.inventory)
        import json
        inv_json = json.dumps(current_inv)
        return f"OK|{inv_json}|{request_id}"

    def process_buy(self, product, quantity, request_id):
        """
        Process a `buy` request.
        :param product: The product to buy.
        :param quantity: The quantity to buy.
        :param request_id: The unique ID for the request.
        :return: Response string.
        """
        if product not in self.inventory:
            return f"ERROR|Product {product} not available|{request_id}"

        with self.locks[product]:
            if self.inventory[product] >= quantity:
                self.inventory[product] -= quantity
                with self.shipped_goods_lock:
                    self.shipped_goods.value += quantity    
                self.save_inventory()
                return f"OK|Shipped {quantity} {product}(s)|{request_id}"
            else:
                return f"ERROR|Insufficient inventory for {product}|{request_id}"

    def process_sell(self, product, quantity, request_id):
        """
        Process a `sell` request.
        :param product: The product to sell.
        :param quantity: The quantity to sell.
        :param request_id: The unique ID for the request.
        :return: Response string.
        """
        if product not in self.inventory:
            with threading.Lock():
                self.inventory[product] = 0
                self.locks[product] = threading.Lock()

        with self.locks[product]:
            self.inventory[product] += quantity
            self.save_inventory()
            return f"OK|Stocked {quantity} {product}(s)|{request_id}"

    def run(self):
        """Start the database server."""
        self.load_inventory()
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"DatabaseServer is running on {self.host}:{self.port}")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                while True:
                    client_socket, address = self.server_socket.accept()
                    executor.submit(self.process_request, client_socket, address)
            except KeyboardInterrupt:
                print("DatabaseServer shutting down.")
            finally:
                self.server_socket.close()


if __name__ == "__main__":
    import argparse

    # Parse command-line arguments for database server configuration
    parser = argparse.ArgumentParser(description="Database Server")
    parser.add_argument("--host", type=str, default="localhost", help="Host for the database server")
    parser.add_argument("--port", type=int, default=5000, help="Port for the database server")
    parser.add_argument("--inventory_file", type=str, default="inventory.json", help="Path to the inventory file")
    parser.add_argument("--max_workers", type=int, default=10, help="Maximum number of threads in the thread pool")
    args = parser.parse_args()

    # Create and run the database server
    db_server = DatabaseServer(
        host=args.host,
        port=args.port,
        inventory_file=args.inventory_file,
        max_workers=args.max_workers
    )
    db_server.run()
