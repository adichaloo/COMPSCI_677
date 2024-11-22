# main.py

import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from inventory import *
from peer import Peer
import config

def monitor_leader():
    """Monitors the leader status and initiates a new election if the leader fails."""
    global peers, leader, previous_leaders, previous_leaders_lock

    time_quantum = config.TIME_QUANTUM
    while True:
        time.sleep(time_quantum)
        if leader and leader.running:
            # Simulate leader failure
            if random.random() < config.LEADER_FAILURE_PROBABILITY:
                print(f"[Leader {leader.peer_id}] Leader failed with probability {config.LEADER_FAILURE_PROBABILITY}.")
                # Simulate leader failure
                leader.leader_active = False
                leader.running = False
                leader.socket.close()
                print(f"Leader {leader.peer_id} has failed. Initiating new election.")
                with previous_leaders_lock:
                    previous_leaders.add(leader.peer_id)
                # Start a new election
                start_election()
        else:
            print("No leader found or leader is down. Initiating new election.")
            start_election()

def start_election():
    """Starts the election process for all peers."""
    global peers, leader, previous_leaders
    # Choose a random peer to initiate the election
    alive_peers = [peer for peer in peers if peer.running]
    if not alive_peers:
        print("No peers are alive to initiate an election.")
        return
    initiating_peer = random.choice(alive_peers)
    print(f"Peer {initiating_peer.peer_id} is initiating the election.")
    # Start the election process
    threading.Thread(target=initiating_peer.start_election, args=(previous_leaders,), daemon=True).start()
    # Wait for the election to complete
    time.sleep(2)
    while True:
        # Check if any peer has become the leader
        for peer in peers:
            if peer.is_leader and peer.running:
                leader = peer
                print(f"Election ended. New leader is Peer {leader.peer_id}.")
                # Update leader reference in all peers
                for p in peers:
                    if p.running:
                        p.leader = leader
                return  # Exit the function after updating the leader
        time.sleep(1)

def main(N):
    global peers, buyers, sellers, leader, previous_leaders, previous_leaders_lock
    num_peers = N

    ports = [5000 + i for i in range(num_peers)]
    roles = ["buyer", "seller"]
    items = ["fish", "salt", "boar"]

    leader_id = 0

    # Initialize previous leaders set and lock
    previous_leaders = set()
    previous_leaders_lock = threading.Lock()

    # Initialize peers list
    peers = []
    buyers = []
    sellers = []

    for i in range(num_peers):
        if i == leader_id:
            role = {'leader', 'seller'}
            item = random.choice(items)
            print(f"Peer {i} is assigned as the leader (trader).")
            peer = Peer(
                peer_id=i,
                role=role,
                neighbors=[],
                port=ports[i],
                ip_address='localhost',
                item=item,
                leader=None,  # Leader is self
                previous_leaders=previous_leaders,
                previous_leaders_lock=previous_leaders_lock
            )
            leader = peer  # The leader is also a peer
            peers.append(peer)
            sellers.append(peer)
            continue
        elif i == num_peers - 2:
            role = {'buyer'}
            item = None
        elif i == num_peers - 1:
            role = {'seller'}
            item = random.choice(items)
        else:
            role = {random.choice(roles)}
            item = random.choice(items) if 'seller' in role else None

        peer = Peer(
            peer_id=i,
            role=role,
            neighbors=[],
            port=ports[i],
            ip_address='localhost',
            item=item,
            leader=leader,
            previous_leaders=previous_leaders,
            previous_leaders_lock=previous_leaders_lock
        )
        peers.append(peer)
        if 'buyer' in role:
            buyers.append(peer)
        if 'seller' in role:
            sellers.append(peer)

    # Set neighbors (fully connected network)
    for i in range(num_peers):
        peers[i].neighbors = [peer for j, peer in enumerate(peers) if i != j]

    print("Network structure initialized (Fully Connected):")
    for peer in peers:
        peer.display_network()

    # Set total peers
    total_peers = len(peers)
    for peer in peers:
        peer.set_total_peers(total_peers)

    # Start peers
    for peer in peers:
        peer.start_peer()

    # Allow some time for peers to start
    time.sleep(2)

    start_time = time.time()

    # Sellers send initial inventory to leader
    for peer in peers:
        if 'seller' in peer.role:
            print(f"Seller {peer.peer_id} is sending its inventory for {peer.item}")
            threading.Thread(target=peer.send_update_inventory).start()

    # Buyers start buying
    for peer in peers:
        if 'buyer' in peer.role:
            print(f"Buyer {peer.peer_id} is starting to buy items.")
            threading.Thread(target=peer.buy_item).start()

    # Start monitoring the leader status with the time quantum
    threading.Thread(target=monitor_leader, daemon=True).start()

    # Wait for buyers to finish
    while True:
        alive_buyers = [buyer for buyer in buyers if buyer.running]
        if not alive_buyers:
            print("All buyers have completed their transactions. Shutting down sellers and exiting program.")
            for seller in sellers:
                seller.shutdown_peer()
            break
        time.sleep(1)
    end_time = time.time()
    print(f"Complete execution of marketplace take {end_time - start_time}")
    # Shutdown remaining peers
    for peer in peers:
        if peer.running:
            peer.shutdown_peer()




if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python main.py <number_of_peers>")
        sys.exit(1)
    N = int(sys.argv[1])
    main(N)
