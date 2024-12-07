import unittest
from unittest.mock import patch, MagicMock
import json
import threading
from database_server import * 

class TestDatabaseServer(unittest.TestCase):
    def setUp(self):
        """Set up the test environment."""
        self.test_inventory = {
            "apple": 100,
            "banana": 50,
            "orange": 75
        }
        self.mock_inventory_file = 'test_inventory.json'
        with open(self.mock_inventory_file, 'w') as f:
            json.dump(self.test_inventory, f)

        self.server = DatabaseServer(
            host='localhost',
            port=0,
            inventory_file=self.mock_inventory_file,
            max_workers=5
        )
        self.assigned_port = self.server.server_socket.getsockname()[1]  # Get the assigned port

    def tearDown(self):
        """Clean up the test environment."""
        import os
        try:
            self.server.server_socket.close()  # Ensure the server socket is closed
        except AttributeError:
            pass  # If the server was not properly initialized
        try:
            os.remove(self.mock_inventory_file)
        except FileNotFoundError:
            pass


    def test_load_inventory(self):
        """Test if the inventory is loaded correctly from the JSON file."""
        self.assertEqual(self.server.inventory, self.test_inventory)

    def test_save_inventory(self):
        """Test if the inventory is saved correctly to the JSON file."""
        # Modify inventory and save
        self.server.inventory['apple'] = 80
        self.server.save_inventory()

        # Reload inventory from the file
        with open(self.mock_inventory_file, 'r') as f:
            saved_inventory = json.load(f)
        self.assertEqual(saved_inventory['apple'], 80)

    def test_process_buy_success(self):
        """Test a successful buy operation."""
        response = self.server.process_buy("apple", 10)
        self.assertEqual(response, "OK|Shipped 10 apple(s)")
        self.assertEqual(self.server.inventory["apple"], 90)

    def test_process_buy_insufficient_inventory(self):
        """Test a buy operation with insufficient inventory."""
        response = self.server.process_buy("apple", 200)
        self.assertEqual(response, "ERROR|Insufficient inventory for apple")
        self.assertEqual(self.server.inventory["apple"], 100)

    def test_process_sell(self):
        """Test a successful sell operation."""
        response = self.server.process_sell("banana", 10)
        self.assertEqual(response, "OK|Stocked 10 banana(s)")
        self.assertEqual(self.server.inventory["banana"], 60)

    def test_process_command_buy(self):
        """Test the process_command method for a buy operation."""
        response = self.server.process_command("buy|orange|20")
        self.assertEqual(response, "OK|Shipped 20 orange(s)")
        self.assertEqual(self.server.inventory["orange"], 55)

    def test_process_command_sell(self):
        """Test the process_command method for a sell operation."""
        response = self.server.process_command("sell|apple|30")
        self.assertEqual(response, "OK|Stocked 30 apple(s)")
        self.assertEqual(self.server.inventory["apple"], 130)

    def test_process_command_invalid_format(self):
        """Test the process_command method with an invalid command format."""
        response = self.server.process_command("invalid|command")
        self.assertEqual(response, "ERROR|Invalid command format")

    def test_process_command_unknown_action(self):
        """Test the process_command method with an unknown action."""
        response = self.server.process_command("unknown|apple|10")
        self.assertEqual(response, "ERROR|Unknown action")

    def simulate_buy_request(self, product, quantity):
        """Simulate a buy request to the database server."""
        self.server.process_buy(product, quantity)

    def simulate_sell_request(self, product, quantity):
        """Simulate a sell request to the database server."""
        self.server.process_sell(product, quantity)

    def test_concurrent_buy_and_sell(self):
        """Test handling concurrent buy and sell requests."""
        threads = []
        product = "apple"
        initial_quantity = self.server.inventory[product]

        # Simulate 10 concurrent buy requests for 10 apples each
        for _ in range(10):
            thread = threading.Thread(target=self.simulate_buy_request, args=(product, 10))
            threads.append(thread)

        # Simulate 10 concurrent sell requests for 5 apples each
        for _ in range(10):
            thread = threading.Thread(target=self.simulate_sell_request, args=(product, 5))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Validate inventory consistency
        expected_quantity = initial_quantity - (10 * 10) + (10 * 5)
        self.assertEqual(
            self.server.inventory[product], 
            expected_quantity, 
            f"Expected {expected_quantity} apples in inventory, but got {self.server.inventory[product]}"
        )

    @patch('socket.socket')
    def test_handle_request(self, mock_socket):
        """Test the handle_request method with mock sockets."""
        mock_client_socket = MagicMock()
        self.server.handle_request("buy|apple|10", mock_client_socket, ('127.0.0.1', 5001))
        mock_client_socket.send.assert_called_with(b"OK|Shipped 10 apple(s)")

        self.server.handle_request("buy|banana|200", mock_client_socket, ('127.0.0.1', 5001))
        mock_client_socket.send.assert_called_with(b"ERROR|Insufficient inventory for banana")

    @patch('socket.socket')
    def test_handle_client(self, mock_socket):
        """Test the handle_client method with mock sockets."""
        mock_client_socket = MagicMock()
        mock_client_socket.recv.side_effect = [
            b"buy|apple|10",
            b"sell|orange|5",
            b"",
        ]
        self.server.thread_pool = MagicMock()
        client_thread = threading.Thread(target=self.server.handle_client, args=(mock_client_socket, ('127.0.0.1', 5001)))
        client_thread.start()
        client_thread.join()

        self.server.thread_pool.submit.assert_any_call(self.server.handle_request, "buy|apple|10", mock_client_socket, ('127.0.0.1', 5001))
        self.server.thread_pool.submit.assert_any_call(self.server.handle_request, "sell|orange|5", mock_client_socket, ('127.0.0.1', 5001))

if __name__ == "__main__":
    unittest.main()
