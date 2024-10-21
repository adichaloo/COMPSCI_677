import unittest
import threading
import sys
import os

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.insert(0, parent_dir)

from peer import Peer  # Absolute import


class TestSellerPeer(Peer):
    def handle_buy(self, message, addr):
        """Handle a buy request from a buyer without restocking."""
        with self.lock:
            if self.stock > 0 and self.item == message["product_name"]:
                self.stock -= 1
                print(f"[{self.peer_id}] Sold item to buyer {message['buyer_id']}. Remaining stock: {self.stock}")
            else:
                print(f"[{self.peer_id}] Cannot fulfill request from buyer {message['buyer_id']}. Stock: {self.stock}")

class TestConcurrency(unittest.TestCase):
    def test_handle_buy_concurrent(self):
        seller = TestSellerPeer(1, 'seller', [], 5001, item='fish')
        seller.stock = 5

        def simulate_buy():
            message = {
                'request_id': 'test',
                'buyer_id': 0,
                'product_name': 'fish',
                'seller_id': 1,
            }
            seller.handle_buy(message, ('localhost', 5000))

        threads = []
        for _ in range(5):
            t = threading.Thread(target=simulate_buy)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Assert that stock is 0
        self.assertEqual(seller.stock, 0)

        # Close the seller's socket
        seller.socket.close()

if __name__ == '__main__':
    unittest.main()
