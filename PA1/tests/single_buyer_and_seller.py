import unittest
import threading
import time
import random
import pickle
from main import *

class TestPeerToPeerNetwork(unittest.TestCase):

    def setUp(self):
        """Set up a network with a single buyer and a single seller."""
        # Resetting random seed for deterministic behavior in tests
        random.seed(0)

        # Create two peers: one buyer and one seller
        self.buyer = Peer(peer_id=0, role='buyer', neighbors=[], port=5000)
        self.seller = Peer(peer_id=1, role='seller', neighbors=[], port=5001, item='salt')

        # Set them as neighbors of each other
        self.buyer.neighbors = [self.seller]
        self.seller.neighbors = [self.buyer]

        # Start both peers in separate threads
        threading.Thread(target=self.buyer.start_peer).start()
        threading.Thread(target=self.seller.start_peer).start()

        # Allow peers to initialize
        time.sleep(1)

    def tearDown(self):
        """Shut down both peers."""
        self.buyer.shutdown_peer()
        self.seller.shutdown_peer()

    def test_lookup_and_buy(self):
        """Test a buyer sending a lookup for an item and successfully buying from a seller."""
        # Capture the output of both buyer and seller
        with self.assertLogs() as log:
            # Step 1: Buyer initiates a lookup for 'apple'
            self.buyer.lookup_item(product_name='salt', hopcount=3)

            # Give the network some time to process the messages
            time.sleep(3)

            # Step 2: Check that the seller replied and buyer bought the item
            logs = log.output
            self.assertIn(f"[0] Looking for salt with neighbor 1", logs)
            self.assertIn(f"[1] Sent reply to buyer 0 for item apple", logs)
            self.assertIn(f"[0] Deciding to buy item apple from seller 1", logs)
            self.assertIn(f"[1] Sold item to buyer 0. Remaining stock:", logs)
            self.assertIn(f"[0] Purchase of apple from seller 1 was successful", logs)

    def test_failed_purchase_due_to_stock(self):
        """Test a failed purchase when the seller is out of stock."""
        # Set the seller's stock to 0 manually
        self.seller.stock = 0

        # Capture the output of both buyer and seller
        with self.assertLogs() as log:
            # Step 1: Buyer initiates a lookup for 'apple'
            self.buyer.lookup_item(product_name='apple', hopcount=3)

            # Give the network some time to process the messages
            time.sleep(3)

            # Step 2: Check that the seller replied with a failure due to stock
            logs = log.output
            self.assertIn(f"[0] Looking for apple with neighbor 1", logs)
            self.assertIn(f"[1] Sent reply to buyer 0 for item apple", logs)
            self.assertIn(f"[1] Sent buy confirmation with failure", logs)
            self.assertIn(f"[0] Purchase of apple from seller 1 failed", logs)

if __name__ == '__main__':
    unittest.main()
