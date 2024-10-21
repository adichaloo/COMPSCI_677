# main.py

import random
import sys
import threading
import time

from peer import Peer
from utils.network_utils import graph_diameter


def main(N):
    num_peers = N  # Number of peers in the network
    peers = []
    ports = [5000 + i for i in range(num_peers)]  # Assign unique ports for all the peers
    roles = ["buyer", "seller"]
    items = ["fish", "salt", "boar"]

    buyers = []
    sellers = []

    # Create peers with random roles and items, ensuring at least one buyer and one seller
    for i in range(num_peers):
        if i == num_peers - 2 and len(buyers) == 0:
            # Ensure at least one buyer exists before the last peer
            role = 'buyer'
        elif i == num_peers - 1 and len(sellers) == 0:
            # Ensure at least one seller exists
            role = 'seller'
        else:
            role = random.choice(roles)
        item = None
        if role == "seller":
            item = random.choice(items)
        peer = Peer(peer_id=i, role=role, neighbors=[], item=item, port=ports[i])
        peers.append(peer)
        if role == 'buyer':
            buyers.append(peer)
        else:
            sellers.append(peer)

    # Connect peers randomly with the motive of making a connected network
    for i in range(num_peers):
        next_peer = (i + 1) % num_peers
        if peers[next_peer] not in peers[i].neighbors:
            peers[i].neighbors.append(peers[next_peer])
            peers[next_peer].neighbors.append(peers[i])

    # Add random neighbors to ensure no more than 3 neighbors per peer
    max_neighbors = min(3, num_peers - 1)
    for peer in peers:
        retries = 0
        while len(peer.neighbors) < max_neighbors and retries < 10:
            neighbor = random.choice(peers)
            if neighbor.peer_id != peer.peer_id and neighbor not in peer.neighbors and len(neighbor.neighbors) < max_neighbors:
                peer.neighbors.append(neighbor)
                neighbor.neighbors.append(peer)
            else:
                retries += 1

    # Display the network structure
    print("Network structure initialized:")
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

    print("The peer-to-peer network has been set up successfully!")

    # Monitor buyers and shut down sellers when buyers are done [Incase if we do not have an evolving network]
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
