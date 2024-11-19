import random
import sys
import threading
import time

from inventory import *
from peer import Peer, Leader
import config

is_election_in_progress = threading.Event()
is_election_in_progress.clear()

peers = []
buyers = []
sellers = []


def monitor_leader(leader, time_quantum):
    """Monitors the leader status and initiates a new election if the leader fails."""
    global peers, buyers, sellers
    i = 0
    while True:
        print("printing", i)
        i += 1
        time.sleep(time_quantum)
        if leader.leader_id is not None:
            # Check if the leader dies with probability p
            if random.random() < config.LEADER_FAILURE_PROBABILITY:
                print(
                    f"[Leader {leader.leader_id}] Leader failed with probability {config.LEADER_FAILURE_PROBABILITY}. Initiating new election.")
                leader_failure_message = {}
                for peer in peers:
                    peer.halt_operations()
                    peer.leader == None  #reset the leader for all peers

                failed_leader = next(peer for peer in peers if peer.role == 'leader')
                inventory = failed_leader.inventory
                assert isinstance(inventory, Inventory)
                peers.remove(failed_leader)
                print("Number of peers", len(peers))
                _ = start_election()
                print("Election Ended")
        else:
            print("No leader found. Initiating new election.")
            start_election()


def start_election():
    """Starts the election process for all peers."""
    global peers
    peer = random.choice(peers)
    while peer.running == False:
        peer = random.choice(peer)
    print(f"Peer with {peer.peer_id} started election. ")
    threading.Thread(target=peer.start_election).start()
    return peer.leader


def main(N):
    global peers, buyers, sellers
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

    # Start monitoring the leader status with the time quantum
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
