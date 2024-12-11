import socket
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Queue


class Trader:
    def __init__(self, host="localhost", port=5001, db_host="localhost", db_port=5000, trader_id=1, max_workers=10):
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

    def forward_to_database(self, command, client_address):
        """
        Forward a command to the database server and forward the response to the appropriate client.
        :param command: Command string in the format expected by the database server.
        :param client_address: The address of the client making the request.
        """
        try:
            self.db_socket.send(command.encode())
            response = self.db_socket.recv(1024).decode()
            self.forward_to_client(client_address, response)
        except Exception as e:
            error_response = f"ERROR|Failed to communicate with database server: {e}"
            self.forward_to_client(client_address, error_response)

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
            self.timestamped_print("Cannot start without a database connection.")
            return

        self.timestamped_print(f"Trader {self.trader_id} is running with a thread pool of {self.max_workers} workers.")
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                while True:
                    client_socket, address = self.server_socket.accept()
                    executor.submit(self.handle_client, client_socket, address)
            except KeyboardInterrupt:
                print(f"Trader {self.trader_id} shutting down.")
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
