import unittest
import multiprocessing
import time
from network import NetworkSetup

class TestNetworkSetup(unittest.TestCase):
    def setUp(self):
        """Set up the test environment for the network."""
        self.network = NetworkSetup(num_buyers=2, num_sellers=2, num_traders=1, db_host='localhost', db_port=5000)

    def tearDown(self):
        """Clean up after the tests."""
        # Terminate all running processes
        if hasattr(self, 'db_process') and self.db_process.is_alive():
            self.db_process.terminate()
        if hasattr(self, 'trader_processes'):
            for process in self.trader_processes:
                if process.is_alive():
                    process.terminate()
        if hasattr(self, 'seller_processes'):
            for process in self.seller_processes:
                if process.is_alive():
                    process.terminate()
        if hasattr(self, 'buyer_processes'):
            for process in self.buyer_processes:
                if process.is_alive():
                    process.terminate()

    def test_database_server_initialization(self):
        """Test if the database server starts successfully."""
        self.db_process = self.network.start_database_server()
        time.sleep(1)  # Allow the server process to start
        self.assertTrue(self.db_process.is_alive(), "Database server process is not running.")

    def test_trader_initialization(self):
        """Test if traders start successfully."""
        self.db_process = self.network.start_database_server()  # Start the DB first
        self.trader_processes = self.network.start_traders()
        time.sleep(1)  # Allow trader processes to start
        for process in self.trader_processes:
            self.assertTrue(process.is_alive(), "Trader process is not running.")

    def test_seller_initialization(self):
        """Test if sellers start successfully."""
        self.db_process = self.network.start_database_server()  # Start the DB first
        self.trader_processes = self.network.start_traders()    # Start traders
        self.seller_processes = self.network.start_sellers()    # Start sellers
        time.sleep(1)  # Allow seller processes to start
        for process in self.seller_processes:
            self.assertTrue(process.is_alive(), "Seller process is not running.")

    def test_buyer_initialization(self):
        """Test if buyers start successfully."""
        self.db_process = self.network.start_database_server()  # Start the DB first
        self.trader_processes = self.network.start_traders()    # Start traders
        self.buyer_processes = self.network.start_buyers()      # Start buyers
        time.sleep(1)  # Allow buyer processes to start
        for process in self.buyer_processes:
            self.assertTrue(process.is_alive(), "Buyer process is not running.")

    def test_complete_network_setup(self):
        """Test the full network setup."""
        self.db_process, self.trader_processes, self.seller_processes, self.buyer_processes = self.network.setup_network()
        time.sleep(1)  # Allow all processes to start

        # Verify all components are running
        self.assertTrue(self.db_process.is_alive(), "Database server process is not running.")
        for process in self.trader_processes:
            self.assertTrue(process.is_alive(), "Trader process is not running.")
        for process in self.seller_processes:
            self.assertTrue(process.is_alive(), "Seller process is not running.")
        for process in self.buyer_processes:
            self.assertTrue(process.is_alive(), "Buyer process is not running.")

    def test_network_shutdown(self):
        """Test graceful shutdown of the network."""
        self.db_process, self.trader_processes, self.seller_processes, self.buyer_processes = self.network.setup_network()
        time.sleep(1)  # Allow all processes to start

        # Shutdown all processes
        self.db_process.terminate()
        for process in self.trader_processes + self.seller_processes + self.buyer_processes:
            process.terminate()
        time.sleep(1)  # Allow processes to terminate

        # Verify all components are terminated
        self.assertFalse(self.db_process.is_alive(), "Database server process did not terminate.")
        for process in self.trader_processes:
            self.assertFalse(process.is_alive(), "Trader process did not terminate.")
        for process in self.seller_processes:
            self.assertFalse(process.is_alive(), "Seller process did not terminate.")
        for process in self.buyer_processes:
            self.assertFalse(process.is_alive(), "Buyer process did not terminate.")

if __name__ == "__main__":
    unittest.main()
