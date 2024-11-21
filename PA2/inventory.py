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

    # def save_to_disk(self, filename='market_state.pkl'):
    #     """Save the inventory to a file."""
    #     try:
    #         with open(filename, 'wb') as f:
    #             pickle.dump(self.inventory, f)
    #         print("Inventory saved to disk.")
    #     except Exception as e:
    #         print(f"Error saving inventory to disk: {e}")
    #
    # def load_from_disk(self, filename='market_state.pkl'):
    #     """Load the inventory from a file."""
    #     # with self.inventory_lock:
    #     try:
    #         with open(filename, 'rb') as f:
    #             self.inventory = pickle.load(f)
    #         print("Inventory loaded from disk.")
    #     except FileNotFoundError:
    #         print("No existing market state found. Starting with empty inventory.")
    #         self.inventory = {}

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




if __name__ == "__main__":
    # Example Usage
    inventory_manager = Inventory()

    # Initialize vector clocks for sellers
    # Assuming there are 3 sellers, vector clocks are lists of length 3
    seller_ids = ["seller_1", "seller_2", "seller_3"]
    vector_clocks = {
        "seller_1": [1, 0, 0],
        "seller_2": [0, 1, 0],
        "seller_3": [0, 0, 1]
    }

    # Adding inventory
    inventory_manager.add_inventory("seller_1", "192.168.1.1:5000", "fish", 10, vector_clocks["seller_1"])
    inventory_manager.add_inventory("seller_3", "192.168.1.3:5000", "fish", 5, vector_clocks["seller_3"])
    inventory_manager.add_inventory("seller_1", "192.168.1.1:5000", "salt", 5, vector_clocks["seller_1"])
    inventory_manager.add_inventory("seller_2", "192.168.1.2:5000", "salt", 7, vector_clocks["seller_2"])

    print("Initial Inventory:")
    print(inventory_manager)

    # Reducing stock
    print("\nReducing stock of 'fish' by 5 units.")
    seller_id, address, status = inventory_manager.reduce_stock("fish", 5)
    print("After Reducing Stock:")
    print(inventory_manager)

    print("\nReducing stock of 'salt' by 10 units (insufficient stock).")
    seller_id, address, status = inventory_manager.reduce_stock("salt", 10)

    print("\nReducing stock of 'fish' by 3 units (completely used up).")
    seller_id, address, status = inventory_manager.reduce_stock("fish", 3)
    print("After Reducing Stock:")
    print(inventory_manager)

    # Getting total stock of an item
    print("\nTotal stock of 'fish':", inventory_manager.get_item_stock("fish"))

    # Getting sellers for an item
    print("\nSellers for 'salt':", inventory_manager.get_sellers_for_item("salt"))

    # Get seller address
    address = inventory_manager.get_seller_address("seller_1")
    print("\nAddress of 'seller_1':", address)

    address = inventory_manager.get_seller_address("seller_2")
    print("Address of 'seller_2':", address)

    # Attempt to get address of a non-existent seller
    address = inventory_manager.get_seller_address("seller_4")
    print("Address of 'seller_4':", address)