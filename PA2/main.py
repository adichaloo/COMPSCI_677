import random
import sys
import threading
import time

from peer import Peer, Leader
# from utils.network_utils import graph_diameter


def main(N):
	num_peers = N  # Number of peers in the network
	peers = []
	ports = [5000 + i for i in range(num_peers)]  # Assign unique ports for all the peers
	roles = ["buyer", "seller"]
	items = ["fish", "salt", "boar"]

	buyers = []
	sellers = []

	# Randomly select one peer to be the leader (trader)
	leader_id = 0

	# Create peers with random roles and items, ensuring at least one buyer and one seller
	for i in range(num_peers):
		if i == leader_id:
			# Assign the leader role to this peer
			role = 'leader'
			item = None
			print(f"Peer {i} is assigned as the leader (trader).")
			leader = Leader(leader_id, 'localhost', ports[i])
		elif i == num_peers - 2 and len(buyers) == 0:
			# Ensure at least one buyer exists before the last peer
			role = 'buyer'
			item = None
		elif i == num_peers - 1 and len(sellers) == 0:
			# Ensure at least one seller exists
			role = 'seller'
			item = random.choice(items)
		else:
			role = random.choice(roles)
			item = random.choice(items) if role == "seller" else None

		peer = Peer(peer_id=i, role=role, neighbors=[], leader=leader, item=item, port=ports[i])
		peers.append(peer)
		if role == 'buyer':
			buyers.append(peer)
		elif role == 'seller':
			sellers.append(peer)

	# Fully connect the network: each peer is a neighbor of every other peer
	for i in range(num_peers):
		for j in range(num_peers):
			if i != j and peers[j] not in peers[i].neighbors:
				peers[i].neighbors.append(peers[j])
				peers[j].neighbors.append(peers[i])

	# Display the network structure
	print("Network structure initialized (Fully Connected):")
	for peer in peers:
		peer.display_network()

	# Start the peers to listen for messages
	for peer in peers:
		peer.start_peer()

	# Calculate network diameter
	# diameter = graph_diameter(peers)
	# print(f"Network diameter is {diameter}")

	# # Set the hopcount to be lower than the diameter
	# hopcount = max(1, diameter - 1)
	# for peer in peers:
	#     peer.max_distance = hopcount
	#     peer.hop_count = hopcount

	# Have every buyer initiate a lookup
	if sellers:
		for seller in sellers:
			print(f"Seller {seller.peer_id} is sending its inventory for {seller.item}")
			threading.Thread(target=seller.send_update_inventory, args=()).start()
	print("Inventory Established with Leader")
	time.sleep(2)
	if buyers:
		for buyer in buyers:
			item = random.choice(items)
			quantity = 1
			print(f"Buyer {buyer.peer_id} is initiating a buy for {item}")
			threading.Thread(target=buyer.buy_item, args=(item, quantity)).start()

	# Monitor buyers and shut down sellers when buyers are done
	while True:
		alive_buyers = [buyer for buyer in buyers if buyer.running]
		if not alive_buyers:
			print("All buyers have shut down. Shutting down sellers and exiting program.")
			# Shut down all seller peers
			for seller in sellers:
				seller.running = False
				seller.socket.close()
			break
		time.sleep(1)  # Sleep before checking again

	# Wait for all peer threads to finish
	for peer in peers:
		peer.thread.join()


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print("Usage: python main.py <number_of_peers>")
		sys.exit(1)
	N = int(sys.argv[1])
	main(N)
