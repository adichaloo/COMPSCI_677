import unittest
import threading
import socket
import time
import json
import uuid
from trader import Trader  # Adjust the import path as needed


class MockWarehouseServer(threading.Thread):
    """
    A mock warehouse server that simulates basic warehouse behavior.
    It supports:
    - fetch|inventory|0|<request_id> -> returns full inventory as JSON
    - buy|product|qty|<request_id> -> checks inventory and decrements if available, else error
    """

    def __init__(self, host="localhost", port=5000, inventory=None):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.inventory = inventory if inventory else {"apple": 100}
        self.lock = threading.Lock()
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def run(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                self.handle_client(client_socket)
            except:
                break

    def handle_client(self, client_socket):
        while True:
            data = client_socket.recv(1024).decode()
            if not data:
                break
            response = self.handle_command(data)
            client_socket.send(response.encode())
        client_socket.close()

    def handle_command(self, command):
        # Commands are in the form action|product|quantity|request_id
        parts = command.split("|")
        if len(parts) != 4:
            return "ERROR|Invalid command format"

        action, product, qty_str, req_id = parts
        qty = int(qty_str)

        if action == "fetch" and product == "inventory":
            with self.lock:
                inv_json = json.dumps(self.inventory)
            return f"OK|{inv_json}|{req_id}"

        if action == "buy":
            with self.lock:
                current_stock = self.inventory.get(product, 0)
                if current_stock >= qty:
                    self.inventory[product] = current_stock - qty
                    return f"OK|Shipped {qty} {product}(s)|{req_id}"
                else:
                    return f"ERROR|Insufficient inventory for {product}|{req_id}"

        # If other actions are needed (sell), add them here.
        return "ERROR|Unknown action"

    def stop(self):
        self.running = False
        # Trigger a connection to unblock accept()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        s.close()


class TestTraderCaching(unittest.TestCase):
    def setUp(self):
        # Start mock warehouse
        self.warehouse_port = 6000
        self.mock_inventory = {"apple": 100}
        self.warehouse = MockWarehouseServer(port=self.warehouse_port, inventory=self.mock_inventory)
        self.warehouse.start()

        # Start trader
        self.trader_port = 6001
        self.trader = Trader(
            host="localhost",
            port=self.trader_port,
            db_host="localhost",
            db_port=self.warehouse_port,
            trader_id=1,
            max_workers=2,
            use_cache=True
        )

        self.trader_thread = threading.Thread(target=self.trader.run, daemon=True)
        self.trader_thread.start()

        # Give some time for trader to connect and fetch initial inventory
        time.sleep(2)

    def tearDown(self):
        self.warehouse.stop()
        # Attempt a connection to the trader port to stop accept()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(("localhost", self.trader_port))
            s.close()
        except:
            pass

    def send_request_to_trader(self, command):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("localhost", self.trader_port))
        s.send(command.encode())
        response = s.recv(1024).decode()
        s.close()
        return response

    def test_cache_behavior(self):
        # Initially, the warehouse says apple=100.
        # The trader should have fetched this during startup.

        # Buy 10 apples - should succeed and now trader cache = 90 apples
        req_id = str(uuid.uuid4())
        response = self.send_request_to_trader(f"buy|apple|10|{req_id}")
        self.assertTrue("OK|Shipped 10 apple(s)" in response, "Trader should fulfill from cache/warehouse")

        # Now mock warehouse inventory is 90 apples left (since trader actually decremented it at warehouse)
        # Force over-sell: set warehouse inventory to a lower number so that next big request fails
        with self.warehouse.lock:
            self.warehouse.inventory["apple"] = 5  # now only 5 apples left at the warehouse

        # Trader thinks it has 90 apples in cache, let's try to buy 50
        req_id = str(uuid.uuid4())
        response = self.send_request_to_trader(f"buy|apple|50|{req_id}")
        # Should fail with ERROR and trigger a cache refresh
        self.assertTrue("ERROR|Insufficient inventory" in response, "Over-sell scenario should fail")

        # Give some time for the trader to refresh cache
        time.sleep(1)

        # Now the trader should have refreshed its cache to apple=5
        # Try buying 5 apples again
        req_id = str(uuid.uuid4())
        response = self.send_request_to_trader(f"buy|apple|5|{req_id}")
        self.assertTrue("OK|Shipped 5 apple(s)" in response,
                        "Trader should now have correct cache and fulfill the request")

        # Try to buy 1 more apple - should fail immediately due to cache (under-sell scenario)
        req_id = str(uuid.uuid4())
        response = self.send_request_to_trader(f"buy|apple|1|{req_id}")
        self.assertTrue("ERROR|Insufficient inventory" in response,
                        "Trader should trust cache and fail immediately for under-sell")


if __name__ == "__main__":
    unittest.main()
