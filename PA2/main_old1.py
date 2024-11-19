import random
import sys
import threading
import time
import pickle
import os

from inventory import *
from peer import Peer, Leader
import config

is_election_in_progress = threading.Event()
is_election_in_progress.clear()

peers = []
buyers = []
sellers = []
leader = None  # Reference to the current leader

# Helper function to persist leader state to disk
def persist_leader_state():
    with open('leader_state.pkl', 'wb') as f:
        pickle.dump(leader, f)
    print("Leader state persisted to disk.")

# Helper function to load leader state from disk
def load_leader_state():
    global leader
    if os.path.exists('leader_state.pkl'):
        with open('leader_state.pkl', 'rb') as f:
            leader = pickle.load(f)
        print("Leader state loaded from disk.")

# Monitor leader health and trigger election if needed
def monitor_leader(leader, time_quantum):
    global peers, buyers, sellers
    while True:
        time.sleep(time_quantum)
        if leader.leader_id is not None:
            if random.random() < config.LEADER_FAILURE_PROBABILITY:
                print(f"[Leader {leader.leader_id}] Leader failed with probability {config.LEADER_FAILURE_PROBABILITY}. Initiating new election.")
                persist_leader_state()  # Save current market state
                for peer in peers:
                    peer.halt_operations()
                    peer.leader = None  # Reset leader reference for all peers
                peers.remove(next(peer for peer in peers if peer.role == 'leader'))
                start_election()
                print("Election Ended")
        else:
            print("No leader found. Initiating new election.")
            start_election()

# Election process to choose a new leader
def start_election():
    global peers, leader
    peer = random.choice([p for p in peers if p.running])
    print(f"Peer with ID {peer.peer_id} started election.")
    threading.Thread(target=peer.start_election).start()
    while is_election_in_progress.is_set():
        time.sleep(1)
    leader = peer.current_leader
    load_leader_state()  # Load previous state if available
    for peer in peers:
        peer.resume_operations()
    print(f"New Leader elected with ID: {leader.leader_id}")

# Main function to initialize the network and start the market
def main(N):
    global peers, buyers, sellers, leader
    num_peers = N

    ports = [5000 + i for i in range(num_peers)]
    roles = ["buyer", "seller"]
    items = ["fish", "salt", "boar"]

    leader_id = 0

    for i in range(num_peers):
        if i == leader_id:
            role = 'leader'
            item = None
            print(f"Peer {i} is assigned as the leader (trader).")
            leader = Leader(leader_id, 'localhost', ports[i])
        elif i == num_peers - 2 and len(buyers) == 0:
            role = 'buyer'
            item = None
        elif i == num_peers - 1 and len(sellers) == 0:
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

    for i in range(num_peers):
        for j in range(num_peers):
            if i != j and peers[j] not in peers[i].neighbors:
                peers[i].neighbors.append(peers[j])
                peers[j].neighbors.append(peers[i])

    print("Network structure initialized (Fully Connected):")
    for peer in peers:
        peer.display_network()

    total_peers = len(peers)
    for peer in peers:
        peer.set_total_peers(total_peers)

    for peer in peers:
        peer.start_peer()

    if sellers:
        for seller in sellers:
            print(f"Seller {seller.peer_id} is sending its inventory for {seller.item}")
            threading.Thread(target=seller.send_update_inventory).start()
    print("Inventory Established with Leader")
    time.sleep(2)
    if buyers:
        for buyer in buyers:
            item = random.choice(items)
            quantity = 1
            print(f"Buyer {buyer.peer_id} is initiating a buy for {item}")
            threading.Thread(target=buyer.buy_item, args=(item, quantity)).start()

    time_quantum = config.TIME_QUANTUM
    threading.Thread(target=monitor_leader, args=(leader, time_quantum)).start()

    while True:
        alive_buyers = [buyer for buyer in buyers if buyer.running]
        if not alive_buyers:
            print("All buyers have shut down. Shutting down sellers and exiting program.")
            for seller in sellers:
                seller.running = False
                seller.socket.close()
            break
        time.sleep(1)

    for peer in peers:
        peer.thread.join()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python main.py <number_of_peers>")
        sys.exit(1)
    N = int(sys.argv[1])
    main(N)
