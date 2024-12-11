import socket
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Queue
import time
import threading

HEARTBEAT_MESSAGE = "ARE YOU THERE?"
HEARTBEAT_RESPONSE = "YES, I'm Alive"

class Trader:
    def __init__(self, host="localhost", port=5001, db_host="localhost", db_port=5000, trader_id=1, max_workers=10, all_clients = None):
        """
        Initialize the trader.
        """
        self.host = host
        self.port = port
        self.db_host = db_host
        self.db_port = db_port
        self.trader_id = trader_id
        self.max_workers = max_workers
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.client_queues = {}
        self.db_socket = None
        print(f"Trader {self.trader_id} listening on {self.host}:{self.port}")

        # Indicates if the other trader is considered alive
        self.other_trader_alive = True
        self.running = True

        self.all_clients = all_clients

    def timestamped_print(self, message):
        """Print a message with a timestamp."""
        timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
        print(f"{timestamp} Trader {self.trader_id}: {message}")
    def broadcast(self, sole_trader_message="Solo Trader"):
        """
        Broadcast a message to all connected clients using their addresses.
        :param sole_trader_message: The message to send to all clients.
        """
        sole_trader_message = f"SOLOTRADER|{str(self.port)}"
        self.timestamped_print(f"Broadcasting solo trader message to all addresses: {sole_trader_message}")
        self.timestamped_print(f"Current clients: {self.all_clients}")

        for address in self.all_clients:
            try:
                self.timestamped_print(f"Attempting to connect to {address}")
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(address)
                    # print(f"Trader {self.trader_id}: Connected 
                    # 
                    #to {address}")
                    s.send(sole_trader_message.encode())
                    self.timestamped_print(f"Broadcasted message to {address}")
            except Exception as e:
                self.timestamped_print(f"Failed to broadcast message to {address}: {e}")



    def heartbeat(self, backup_host, backup_port):
        """Send periodic heartbeat messages to the other trader and check for responses."""
        self.timestamped_print(f"Starting Heartbeat")
        time.sleep(3)  # Wait a bit for both traders to set up

        while (self.running and self.other_trader_alive):
            # Attempt to connect to the other trader
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2)
                    s.connect((backup_host, backup_port))
                    # Send heartbeat message
                    s.send(HEARTBEAT_MESSAGE.encode())
                    self.timestamped_print(f"Sent heartbeat message.")

                    # Try to receive the response
                    try:
                        data = s.recv(1024).decode()
                        if data == HEARTBEAT_RESPONSE:
                            self.timestamped_print(f"Received heartbeat response {data}.")
                            pass
                        else:
                            # Unexpected response, but the trader responded, so it's alive
                            pass
                    except socket.timeout:
                        # No response received in time, other trader might be down
                        self.timestamped_print(f"No heartbeat response from {backup_host}:{backup_port}. Assuming down.")
                        self.other_trader_alive = False
                        self.broadcast()
                        # Implement failover logic if needed
            except ConnectionRefusedError:
                # Could not connect, other trader likely down
                self.timestamped_print(f"Cannot connect to {backup_host}:{backup_port}. Assuming down.")
                self.other_trader_alive = False
                self.broadcast()
                # Implement failover logic if needed
            except Exception as e:
                self.timestamped_print(f"Heartbeat encountered error: {e}")

            # Sleep before next heartbeat attempt
            time.sleep(2)

    def connect_to_database(self):
        """Establish a persistent connection to the database server."""
        try:
            self.db_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.db_socket.connect((self.db_host, self.db_port))
            self.timestamped_print(f"Connected to database server at {self.db_host}:{self.db_port}")
        except ConnectionError as e:
            self.timestamped_print(f"Failed to connect to database server: {e}")
            self.db_socket = None

    def forward_to_database(self, command, client_address):
        """Forward a command to the database server and forward the response to the appropriate client."""
        if not self.db_socket:
            error_response = "ERROR|No database connection"
            self.forward_to_client(client_address, error_response)
            return

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
        """
        if client_address in self.client_queues:
            self.client_queues[client_address].put(response)
        else:
            self.timestamped_print(f"Client {client_address} not found for response forwarding.")

    def handle_client(self, client_socket, address):
        """Handle a connection from a buyer, seller, or the other trader (for heartbeats)."""
        # print(f"Trader {self.trader_id} connected to {address}")
        self.client_queues[address] = Queue()

        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                data = data.decode()

                # Check if this is a heartbeat message from the other trader
                if data == HEARTBEAT_MESSAGE:
                    # Respond immediately
                    client_socket.send(HEARTBEAT_RESPONSE.encode())
                    self.timestamped_print(f"Received and responded to heartbeat message {data}.")
                    continue

                
                # Forward the command to the database server
                if data != HEARTBEAT_MESSAGE and data!= HEARTBEAT_RESPONSE:
                    self.timestamped_print(f"Received from {address}: {data}")
                    self.forward_to_database(data, address)

                # Wait for the response from the database server
                response = self.client_queues[address].get()
                client_socket.send(response.encode())
        except ConnectionResetError:
            self.timestamped_print(f"Connection reset by {address}")
        except Exception as e:
            self.timestamped_print(f"Encountered an error with {address}: {e}")
        finally:
            client_socket.close()
            del self.client_queues[address]
            # print(f"Trader {self.trader_id} disconnected from {address}")

    def run(self, backup_host=None, backup_port=None):
        """Start the trader process."""
        self.connect_to_database()
        if not self.db_socket:
            self.timestamped_print(f"Cannot start without a database connection.")
            return

        self.timestamped_print(f"Running with a thread pool of {self.max_workers} workers.")

        # Start the heartbeat thread if backup info is provided
        if backup_host and backup_port:
            heartbeat_thread = threading.Thread(target=self.heartbeat, args=(backup_host, backup_port), daemon=True)
            heartbeat_thread.start()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                while self.running:
                    client_socket, address = self.server_socket.accept()
                    executor.submit(self.handle_client, client_socket, address)
            except KeyboardInterrupt:
                self.timestamped_print(f"Shutting down.")
                self.running = False
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
    parser.add_argument("--backup_host", type=str, default=None, help="Backup trader host")
    parser.add_argument("--backup_port", type=int, default=None, help="Backup trader port")

    args = parser.parse_args()

    trader = Trader(
        host=args.host,
        port=args.port,
        db_host=args.db_host,
        db_port=args.db_port,
        trader_id=args.id,
        max_workers=args.max_workers
    )
    trader.run(args.backup_host, args.backup_port)
