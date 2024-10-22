import unittest
import threading
import sys
import os
from datetime import datetime

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.insert(0, parent_dir)

from peer import Peer  # Absolute import


class TestSellerPeer(Peer):
    def __init__(self, peer_id, role, neighbors, port, item, stock=0):
        super().__init__(peer_id, role, neighbors, port)
        self.item = item
        self.stock = stock
        self.lock = threading.Lock()  # Initialize the lock for thread safety
        self.sale_log = []  # List to store sale events with timestamps

    def handle_buy(self, message, addr): # Same logic of handle_buy in peer.py [Since peer.py contains an evolvong
        """Handle a buy request from a buyer without restocking."""
        with self.lock:
            if self.stock > 0 and self.item == message["product_name"]:
                self.stock -= 1
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Format timestamp to milliseconds
                sale_event = {
                    'buyer_id': message['buyer_id'],
                    'timestamp': timestamp
                }
                self.sale_log.append(sale_event)
                print(f"[{self.peer_id}] Sold item to buyer {message['buyer_id']} at {timestamp}. Remaining stock: {self.stock}")
            else:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                print(f"[{self.peer_id}] Cannot fulfill request from buyer {message['buyer_id']} at {timestamp}. Stock: {self.stock}")


class TestConcurrency(unittest.TestCase):
    def test_handle_buy_concurrent_multiple_buyers(self):
        # Initialize the seller with 5 stocks of 'fish'
        seller = TestSellerPeer(peer_id=1, role='seller', neighbors=[], port=5001, item='fish', stock=5)

        # Define a list of unique buyer IDs
        buyer_ids = [2, 3, 4, 5, 6]

        # Function to simulate a buy request from a unique buyer
        def simulate_buy(buyer_id):
            message = {
                'request_id': f'test_{buyer_id}',
                'buyer_id': buyer_id,
                'product_name': 'fish',
                'seller_id': seller.peer_id,
            }
            seller.handle_buy(message, ('localhost', 5000))

        threads = []
        # Create and start a thread for each buyer
        for buyer_id in buyer_ids:
            t = threading.Thread(target=simulate_buy, args=(buyer_id,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Assert that stock is 0 after all purchases
        self.assertEqual(seller.stock, 0)

        # Assert that all 5 sale events have been recorded
        self.assertEqual(len(seller.sale_log), 5)
        for sale in seller.sale_log:
            self.assertIn('buyer_id', sale)
            self.assertIn('timestamp', sale)
            # Optionally, verify the timestamp format
            try:
                datetime.strptime(sale['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                self.fail(f"Timestamp {sale['timestamp']} is not in the correct format.")

        # Close the seller's socket
        seller.socket.close()


if __name__ == '__main__':
    unittest.main()
