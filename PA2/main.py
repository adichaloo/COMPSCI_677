import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from inventory import *
from peer import Peer
import config

is_election_in_progress = threading.Event()
is_election_in_progress.clear()

peers = []
buyers = []
sellers = []
leader = None
previous_leaders = set()  # Keep track of previous leaders
previous_leaders_lock = threading.Lock()

def monitor_leader():
    """Monitors the leader status and initiates a new election if the leader fails."""
    global peers, leader

    time_quantum = config.TIME_QUANTUM
    while True:
        time.sleep(time_quantum)
        if leader and leader.running:
            # Check if the leader fails with a certain probability
            if random.random() < config.LEADER_FAILURE_PROBABILITY:
                print(f"[Leader {leader.peer_id}] Leader failed with probability {config.LEADER_FAILURE_PROBABILITY}.")
                # Simulate leader failure
                # leader.running = False
                # leader.shutdown_peer()
                # Reset leader reference in all peers
                leader.leader_active = False
                for peer in peers:
                    if peer != leader:
                        peer.leader = None
                # Remove the leader from the peers list if desired
                # peers.remove(leader)  # Optional
                print(f"Leader {leader.peer_id} has failed. Initiating new election.")
                with previous_leaders_lock:
                    # previous_leaders.add(leader.peer_id)
                    # Check if all peers have been leaders
                    # if len(previous_leaders) == len(peers):
                    #     print("All peers have been leaders. Resetting previous leaders list.")
                    #     previous_leaders.clear()
                    if previous_leaders:
                        previous_leaders.clear()
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
    initiating_peer = random.choice([peer for peer in peers if peer.running])
    print(f"Peer {initiating_peer.peer_id} is initiating the election.")
    # Start the election process
    threading.Thread(target=initiating_peer.start_election, args=(previous_leaders,)).start()
    # Wait for the election to complete
    time.sleep(2)
    while True:
        # Check if any peer has become the leader
        for peer in peers:
            if peer.is_leader:
                leader = peer
                print(f"Election ended. New leader is Peer {leader.peer_id}.")
                # Update leader reference in all peers
                for p in peers:
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

    # Initialize ThreadPoolExecutors
    election_executor = ThreadPoolExecutor(max_workers=5)
    inventory_executor = ThreadPoolExecutor(max_workers=10)
    buy_executor = ThreadPoolExecutor(max_workers=20)

    for i in range(num_peers):
        if i == leader_id:
            role = 'leader'
            item = None
            print(f"Peer {i} is assigned as the leader (trader).")
            print(f"Peer {i} is assigned as the leader (trader).")
            peer = Peer(peer_id=i, role=role, neighbors=[], port=ports[i], previous_leaders=previous_leaders, previous_leaders_lock= previous_leaders_lock, leader=None)
            leader = peer  # The leader is also a peer
            peers.append(peer)
            continue
        elif i == num_peers - 2 and len(buyers) == 0:
            role = 'buyer'
            item = None
        elif i == num_peers - 1 and len(sellers) == 0:
            role = 'seller'
            item = random.choice(items)
        else:
            role = random.choice(roles)
            item = random.choice(items) if role == "seller" else None

        peer = Peer(peer_id=i, role=role, neighbors=[], leader=leader, previous_leaders=previous_leaders, previous_leaders_lock=previous_leaders_lock, item=item, port=ports[i])
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

    for peer in peers:
        peer.start_peer()

    total_peers = len(peers)
    for peer in peers:
        peer.set_total_peers(total_peers)

    if sellers:
        for seller in sellers:
            print(f"Seller {seller.peer_id} is sending its inventory for {seller.item}")
            inventory_executor.submit(seller.send_update_inventory)
    print("Inventory Established with Leader")
    time.sleep(2)  # Wait for inventory updates to reach the leader

    # Initiate buy operations
    if buyers:
        for buyer in buyers:
            def initiate_buy(buyer_instance):
                while buyer_instance.running:
                    item = random.choice(buyer_instance.available_items)
                    quantity = random.randint(1, 5)
                    print(f"Buyer {buyer_instance.peer_id} is initiating a buy for {item} (Quantity: {quantity})")
                    buyer_instance.buy_item(item, quantity)
                    # Random wait before next buy
                    time.sleep(random.randint(5, 10))
            buy_executor.submit(initiate_buy, buyer)

    # Start monitoring the leader status with the time quantum
    threading.Thread(target=monitor_leader, daemon=True).start()

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