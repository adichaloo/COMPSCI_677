# inventory.py

import threading
import functools
import pickle

class Inventory:
    def __init__(self):
        self.inventory = {}  # Dictionary to store items and their seller info
        self.inventory_lock = threading.RLock()

    def add_inventory(self, seller_id, address, item_name, quantity, vector_clock):
        """Add or update inventory for a seller."""
        with self.inventory_lock:
            if item_name not in self.inventory:
                self.inventory[item_name] = []
            # Check if seller already exists
            for seller_info in self.inventory[item_name]:
                if seller_info['seller_id'] == seller_id:
                    # Update the existing quantity and vector clock
                    seller_info['quantity'] += quantity
                    seller_info['vector_clock'] = vector_clock
                    return
            # Add new seller entry
            self.inventory[item_name].append({
                'seller_id': seller_id,
                'address': address,
                'quantity': quantity,
                'vector_clock': vector_clock
            })

    def reduce_stock(self, item_name, quantity):
        """
        Reduce the stock of an item by choosing the seller with the earliest vector clock.
        Returns (seller_id, address, True) if successful, or (None, None, False) if not.
        """
        with self.inventory_lock:
            if item_name not in self.inventory or not self.inventory[item_name]:
                print(f"Error: Item '{item_name}' not found or out of stock.")
                return None, None, False

            # Filter out sellers with sufficient stock
            available_sellers = [seller_info for seller_info in self.inventory[item_name] if seller_info['quantity'] >= quantity]

            if not available_sellers:
                print(f"Error: No seller has enough stock of '{item_name}'.")
                return None, None, False

            # Sort sellers based on vector clocks
            available_sellers.sort(key=functools.cmp_to_key(compare_sellers))

            # Choose the seller with the earliest vector clock
            seller_info = available_sellers[0]
            seller_id = seller_info['seller_id']
            address = seller_info['address']
            stock = seller_info['quantity']

            # Reduce the stock for the chosen seller
            new_quantity = stock - quantity
            if new_quantity > 0:
                seller_info['quantity'] = new_quantity
            else:
                # Remove seller if the quantity becomes zero
                self.inventory[item_name].remove(seller_info)
            print(f"Stock reduced: {quantity} units of '{item_name}' sold by {seller_id} ({address}).")
            return seller_id, address, True

    def save_to_disk(self, earnings):
        data = {
            'inventory': self.inventory,
            'earnings': earnings
        }
        with open('market_state.pkl', 'wb') as f:
            pickle.dump(data, f)
        print("Inventory saved to disk.")

    def load_from_disk(self):
        with open('market_state.pkl', 'rb') as f:
            data = pickle.load(f)
            self.inventory = data['inventory']
            earnings = data.get('earnings', 0.0)
        print("Inventory loaded from disk.")
        return earnings

def compare_vector_clocks(vc1, vc2):
    """Compare two vector clocks."""
    less = False
    greater = False
    for v1, v2 in zip(vc1, vc2):
        if v1 < v2:
            less = True
        elif v1 > v2:
            greater = True
    if less and not greater:
        return -1
    elif greater and not less:
        return 1
    else:
        return 0  # Concurrent

def compare_sellers(seller1, seller2):
    """Compare two sellers based on their vector clocks."""
    comp = compare_vector_clocks(seller1['vector_clock'], seller2['vector_clock'])
    print(f"Comparing sellers {seller1['seller_id']} and {seller2['seller_id']} with vector clocks {seller1['vector_clock']} and {seller2['vector_clock']}: result {comp}")
    if comp == -1:
        return -1
    elif comp == 1:
        return 1
    else:
        # Break ties using seller IDs
        if seller1['seller_id'] < seller2['seller_id']:
            return -1
        elif seller1['seller_id'] > seller2['seller_id']:
            return 1
        else:
            return 0
