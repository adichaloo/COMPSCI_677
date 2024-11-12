import sys
import threading
import time
import random
from peer import Peer
from utils.network_utils import graph_diameter


def main(num_peers, num_buyers, num_sellers, neighbors_map):
    peers = []
    ports = [5000 + i for i in range(num_peers)]  # Assign unique ports for all the peers
    items = ["fish", "salt", "boar"]

    buyers = []
    sellers = []

    # Create peers with specific number of buyers and sellers
    for i in range(num_peers):
        if len(buyers) < num_buyers:
            role = 'buyer'
        elif len(sellers) < num_sellers:
            role = 'seller'
        else:
            role = 'buyer' if i % 2 == 0 else 'seller'  # Alternate buyers and sellers if needed
        
        item = None
        if role == "seller":
            item = random.choice(items)
        peer = Peer(peer_id=i, role=role, neighbors=[], item=item, port=ports[i])
        peers.append(peer)
        if role == 'buyer':
            buyers.append(peer)
        else:
            sellers.append(peer)

    # Hardcode the neighbors for each peer
    # Ensure that the network remains connected and each peer has predefined neighbors
    

    # Assign neighbors according to the hardcoded map
    for peer_id, neighbor_ids in neighbors_map.items():
        for neighbor_id in neighbor_ids:
            peer = peers[peer_id]
            neighbor = peers[neighbor_id]
            if neighbor not in peer.neighbors:
                peer.neighbors.append(neighbor)
                neighbor.neighbors.append(peer)

    # Display the network structure
    print("Network structure initialized with hardcoded neighbors:")
    for peer in peers:
        peer.display_network()

    # Start the peers to listen for messages
    for peer in peers:
        peer.start_peer()

    # Calculate network diameter
    diameter = graph_diameter(peers)
    print(f"Network diameter is {diameter}")

    # Set the hopcount to be lower than the diameter
    hopcount = max(1, diameter - 1)
    for peer in peers:
        peer.max_distance = hopcount
        peer.hop_count = hopcount

    # Have every buyer initiate a lookup
    if buyers:
        for buyer in buyers:
            item = random.choice(items)
            print(f"Buyer {buyer.peer_id} is initiating a lookup for {item} with hopcount {hopcount}")
            threading.Thread(target=buyer.lookup_item, args=(item, hopcount)).start()

    # Monitor buyers and shut down sellers when buyers are done
    while True:
        alive_buyers = [buyer for buyer in buyers if buyer.running]
        if not alive_buyers:
            print("All buyers have shut down. Shutting down sellers and exiting program.")
            for seller in sellers:
                seller.running = False
                seller.socket.close()
            break
        time.sleep(1)  # Sleep before checking again

    # Wait for all peer threads to finish
    for peer in peers:
        peer.thread.join()



num_peers = 3
num_buyers = 2
num_sellers = 1
neighbors_map = {
        0: [1],
        1: [0, 2],
        2: [1],
    }

if num_buyers + num_sellers > num_peers:
	print("Error: The sum of buyers and sellers cannot exceed the total number of peers.")
	sys.exit(1)

main(num_peers, num_buyers, num_sellers,neighbors_map)
