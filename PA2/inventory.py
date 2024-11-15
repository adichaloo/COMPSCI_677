import random


class Inventory:
	def __init__(self):
		self.inventory = {}

	def add_inventory(self, seller_id, address, item_name, quantity):
		"""Add or update inventory for a seller."""
		if item_name not in self.inventory:
			self.inventory[item_name] = [(seller_id, address, quantity)]
		else:
			for i, (s_id, addr, qty) in enumerate(self.inventory[item_name]):
				if s_id == seller_id:
					# Update the existing quantity
					self.inventory[item_name][i] = (seller_id, addr, qty + quantity)
					return
			# Add new seller entry if not found
			self.inventory[item_name].append((seller_id, address, quantity))
		print(self.inventory)

	def update_inventory(self, seller_id, item_name, new_quantity):
		"""Update the quantity of an existing item for a specific seller."""
		if item_name not in self.inventory:
			print(f"Error: Item '{item_name}' not found in inventory.")
			return

		for i, (s_id, addr, qty) in enumerate(self.inventory[item_name]):
			if s_id == seller_id:
				if new_quantity > 0:
					self.inventory[item_name][i] = (seller_id, addr, new_quantity)
				else:
					self.inventory[item_name].pop(i)
				return

		print(f"Error: Seller '{seller_id}' not found for item '{item_name}'.")

	def reduce_stock(self, item_name, quantity):
		"""
		Reduce the stock of an item by choosing a random seller.
		Returns (seller_id, address, True) if successful, or (None, None, False) if not.
		"""
		if item_name not in self.inventory or not self.inventory[item_name]:
			print(f"Error: Item '{item_name}' not found or out of stock.")
			return None, None, False

		# Filter out sellers with sufficient stock
		available_sellers = [(s_id, addr, qty) for s_id, addr, qty in self.inventory[item_name] if qty >= quantity]

		if not available_sellers:
			print(f"Error: No seller has enough stock of '{item_name}'.")
			return None, None, False

		# Randomly choose a seller from the available sellers
		seller_id, address, stock = random.choice(available_sellers)

		# Reduce the stock for the chosen seller
		new_quantity = stock - quantity
		for i, (s_id, addr, qty) in enumerate(self.inventory[item_name]):
			if s_id == seller_id:
				if new_quantity > 0:
					self.inventory[item_name][i] = (seller_id, addr, new_quantity)
				else:
					# Remove seller if the quantity becomes zero
					self.inventory[item_name].pop(i)
				print(f"Stock reduced: {quantity} units of '{item_name}' sold by {seller_id} ({address}).")
				return seller_id, address, True

		return None, None, False

	def get_item_stock(self, item_name):
		"""Retrieve the total stock of an item across all sellers."""
		if item_name not in self.inventory:
			return 0

		return sum(qty for _, _, qty in self.inventory[item_name])

	def get_sellers_for_item(self, item_name):
		"""Get a list of sellers who have the item in stock."""
		if item_name not in self.inventory:
			return []

		return [(s_id, addr, qty) for s_id, addr, qty in self.inventory[item_name] if qty > 0]

	def remove_seller_inventory(self, seller_id, item_name):
		"""Remove a seller's stock of a particular item."""
		if item_name not in self.inventory:
			print(f"Error: Item '{item_name}' not found in inventory.")
			return

		for i, (s_id, addr, qty) in enumerate(self.inventory[item_name]):
			if s_id == seller_id:
				self.inventory[item_name].pop(i)
				return

		print(f"Error: Seller '{seller_id}' not found for item '{item_name}'.")

	def remove_item(self, item_name):
		"""Remove an entire item from the inventory."""
		if item_name in self.inventory:
			del self.inventory[item_name]
		else:
			print(f"Error: Item '{item_name}' not found in inventory.")

	def get_inventory(self):
		"""Get the entire inventory data."""
		return self.inventory

	def __str__(self):
		"""String representation of the inventory."""
		return str(self.inventory)
	
	def get_seller_address(self, seller_id):
		"""Get the address of a seller given the seller_id."""
		for item_list in self.inventory.values():
			for s_id, addr, qty in item_list:
				if s_id == seller_id:
					return addr
		print(f"Error: Address for seller '{seller_id}' not found.")
		return None


if __name__ == "__main__":
	# Example Usage
	inventory_manager = Inventory()

	# Adding inventory
	inventory_manager.add_inventory("seller_1", "192.168.1.1:5000", "fish", 10)
	inventory_manager.add_inventory("seller_3", "192.168.1.3:5000", "fish", 3)
	inventory_manager.add_inventory("seller_1", "192.168.1.1:5000", "salt", 5)
	inventory_manager.add_inventory("seller_2", "192.168.1.2:5000", "salt", 7)

	print("Initial Inventory:", inventory_manager)

	# Reducing stock
	print("\nReducing stock of 'fish' by seller_1 by 5 units.")
	inventory_manager.reduce_stock("seller_1", "fish", 5)
	print("After Reducing Stock:", inventory_manager)

	print("\nReducing stock of 'salt' by seller_2 by 10 units (insufficient stock).")
	inventory_manager.reduce_stock("seller_2", "salt", 10)

	print("\nReducing stock of 'fish' by seller_3 by 3 units (completely used up).")
	inventory_manager.reduce_stock("seller_3", "fish", 3)
	print("After Reducing Stock:", inventory_manager)

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
