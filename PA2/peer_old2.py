# peer.py

import threading
import socket
import pickle
import random
import time
import hashlib
import datetime
from utils.messages import *
import config
from inventory import Inventory, compare_vector_clocks
from utils.utils import persist_leader_state, is_election_in_progress, load_leader_state  # Import necessary functions and events

BUY_PROBABILITY = config.BUY_PROBABILITY
SELLER_STOCK = config.SELLER_STOCK
MAX_TRANSACTIONS = config.MAX_TRANSACTIONS
TIMEOUT = config.TIMEOUT
PRICE = config.PRICE
COMMISSION = config.COMMISSION

class Leader:
    def __init__(self, leader_id, ip_address, port):
        self.leader_id = leader_id
        self.ip_address = ip_address
        self.port = port
        self.address = (self.ip_address, self.port)
        self.is_active = True


class Peer:
    def __init__(self, peer_id, role, neighbors, port, leader, ip_address='localhost', item=None):
        self.peer_id = peer_id
        self.role = role  # 'buyer' or 'seller' or 'leader'
        self.neighbors = neighbors
        self.ip_address = ip_address
        self.port = port
        self.address = (ip_address, self.port)
        self.item = item
        self.stock = SELLER_STOCK if role == 'seller' else 0
        self.lock = threading.Lock()  # For thread safety
        self.pending_requests_lock = threading.Lock()  # Lock for pending_requests
        self.sell_confirmation_lock = threading.Lock()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.ip_address, port))
        self.running = True
        self.looked_up_items = set()
        self.available_items = ["fish", "salt", "boar"]
        self.items_bought = 0
        self.inventory = Inventory() if self.role == 'leader' else None
        self.leader = leader if self.role != 'leader' else None
        self.inventory_lock = threading.Lock()
        self.operations_lock = threading.Lock()
        self.total_peers = None
        self.vector_clock = []
        self.peer_index = None

        # For buyer timeout handling
        self.pending_requests = {}
        self.timeout = TIMEOUT  # seconds
        if self.role == 'buyer':
            self.start_time = None
            self.end_time = None
            self.average_rtt = time.time()
            self.max_transactions = MAX_TRANSACTIONS

        self.in_election = False  # Whether the peer is currently in an election
        self.is_leader = self.role == 'leader'  # Whether this peer is the leader
        self.current_leader = leader  # The current leader (initially set at the start)

        self.time_quantum = config.TIME_QUANTUM
        self.election_timer_thread = None

    def set_total_peers(self, total_peers):
        self.total_peers = total_peers
        self.vector_clock = [0] * total_peers
        self.peer_index = self.peer_id  # Assuming peer IDs are from 0 to N-1

    def display_network(self):
        """Print network structure for this peer."""
        neighbor_ids = [neighbor.peer_id for neighbor in self.neighbors]
        print(f"Peer {self.peer_id} ({self.role}) connected to peers {neighbor_ids}")

    def increment_vector_clock(self):
        """Increment the vector clock for this peer."""
        with self.lock:
            self.vector_clock[self.peer_index] += 1
            print(f"[{self.peer_id}] Incremented vector clock: {self.vector_clock}")

    def get_vector_clock(self):
        """Get a copy of the current vector clock."""
        with self.lock:
            return self.vector_clock.copy()

    def update_vector_clock(self, received_vector_clock):
        """Update vector clock upon receiving a message."""
        with self.lock:
            old_vector_clock = self.vector_clock.copy()
            for i in range(self.total_peers):
                self.vector_clock[i] = max(self.vector_clock[i], received_vector_clock[i])
            self.vector_clock[self.peer_index] += 1  # Increment own clock after merging
            print(f"[{self.peer_id}] Updated vector clock from {old_vector_clock} to {self.vector_clock} after receiving {received_vector_clock}")

    def halt_operations(self):
        """Halt all operations during an election."""
        print(f"[{self.peer_id}] Halting all operations due to election.")
        self.operations_lock.acquire()

    def resume_operations(self):
        """Resume all operations after the election."""
        if self.operations_lock.locked():
            self.operations_lock.release()
        print(f"[{self.peer_id}] Resuming all operations after election.")

    def start_peer(self):
        """Start listening for messages from other peers."""
        print(f"Peer {self.peer_id} ({self.role}) with item {self.item} listening on port {self.port}...")
        t = threading.Thread(target=self.listen_for_messages, daemon=True)
        t.start()
        self.thread = t

    def monitor_leader(self):
        """Monitor the leader's status and initiate election if leader fails."""
        while self.running:
            time.sleep(self.time_quantum)
            if self.leader and not self.is_leader:
                if not self.check_leader_alive():
                    print(f"[{self.peer_id}] Detected leader failure. Initiating election.")
                    self.start_election()
            elif not self.leader and not self.is_leader:
                print(f"[{self.peer_id}] No leader found. Initiating election.")
                self.start_election()

    def check_leader_alive(self):
        """Check if the leader is alive by attempting to send a heartbeat."""
        try:
            heartbeat_message = {'type': 'heartbeat'}
            self.send_message(self.leader.address, heartbeat_message)
            return True
        except Exception as e:
            print(f"[{self.peer_id}] Failed to send heartbeat to leader: {e}")
            return False

    def start_election(self):
        """Initiate the election process following the Bully algorithm."""
        if self.in_election or is_election_in_progress.is_set():
            print(f"[{self.peer_id}] Election already in progress.")
            return
        self.in_election = True
        is_election_in_progress.set()
        print(f"[{self.peer_id}] Initiating election.")
        # Send election message to all higher ID peers
        election_message = {'type': 'election', 'peer_id': self.peer_id}
        higher_peers = [peer for peer in self.neighbors if peer.peer_id > self.peer_id and peer.running]
        if not higher_peers:
            # No higher ID peers, declare self as leader
            self.declare_leader()
        else:
            for peer in higher_peers:
                self.send_message((peer.ip_address, peer.port), election_message)
            # Wait for OK messages
            threading.Thread(target=self.wait_for_ok, daemon=True).start()

    def wait_for_ok(self):
        """Wait for OK messages from higher ID peers."""
        wait_time = config.OK_TIMEOUT
        time.sleep(wait_time)
        if not self.ok_received:
            # No OK received, declare self as leader
            self.declare_leader()
        else:
            # OK received, wait for leader announcement
            print(f"[{self.peer_id}] OK received. Waiting for leader announcement.")
            # Election is ongoing; peer will respond when leader is announced

    def declare_leader(self):
        """Declare this peer as the new leader."""
        global leader
        self.is_leader = True
        self.role = 'leader'
        self.current_leader = self
        self.leader = None  # No leader above self
        print(f"[{self.peer_id}] Declared self as new leader.")
        self.inventory = Inventory()
        # self.inventory.load_inventory_state()
        leader_message = {
            'type': 'leader',
            'leader_id': self.peer_id,
            'ip_address': self.ip_address,
            'port': self.port
        }
        for peer in self.neighbors:
            if peer.running:
                self.send_message((peer.ip_address, peer.port), leader_message)
        # Update the global leader variable
        leader = self
        # Persist the state
        persist_leader_state(self.inventory)
        # Clear the election event
        is_election_in_progress.clear()
        self.in_election = False

    def handle_election(self, message):
        """Handle an election message."""
        sender_id = message['peer_id']
        print(f"[{self.peer_id}] Received election message from {sender_id}.")
        if self.peer_id > sender_id:
            # Respond with OK
            ok_message = {'type': 'OK', 'peer_id': self.peer_id}
            sender_peer = next((peer for peer in self.neighbors if peer.peer_id == sender_id and peer.running), None)
            if sender_peer:
                self.send_message((sender_peer.ip_address, sender_peer.port), ok_message)
                print(f"[{self.peer_id}] Sent OK to Peer {sender_id}.")
            # Initiate own election
            self.start_election()

    def handle_ok(self, message):
        """Handle an OK message."""
        sender_id = message['peer_id']
        print(f"[{self.peer_id}] Received OK from Peer {sender_id}.")
        self.ok_received = True

    def handle_leader(self, message):
        """Handle a leader announcement message."""
        global leader
        leader_id = message['leader_id']
        print(f"[{self.peer_id}] New leader is Peer {leader_id}.")
        self.is_leader = (self.peer_id == leader_id)
        if not self.is_leader:
            self.leader = next((peer for peer in self.neighbors if peer.peer_id == leader_id and peer.running), None)
            if self.leader:
                print(f"[{self.peer_id}] Leader set to Peer {self.leader.peer_id}")
            else:
                print(f"[{self.peer_id}] Leader Peer {leader_id} not found among neighbors.")
        else:
            self.leader = None
            self.inventory = Inventory()
            # self.inventory.load_inventory_state()
        leader = self.leader if not self.is_leader else self
        # Clear the election event if this peer is the new leader
        if self.is_leader:
            is_election_in_progress.clear()
        self.in_election = False
        self.ok_received = False

    def listen_for_messages(self):
        """Continuously listen for incoming messages."""
        while self.running:
            try:
                self.socket.settimeout(1.0)
                data, addr = self.socket.recvfrom(4096)
                message = pickle.loads(data)
                msg_type = message.get('type')
                print(f"[{self.peer_id}] Received message of type '{msg_type}' from {addr}")
                if msg_type == 'heartbeat':
                    continue  # Ignore heartbeat messages
                elif msg_type == 'election':
                    self.handle_election(message)
                elif msg_type == 'OK':
                    self.handle_ok(message)
                elif msg_type == 'leader':
                    self.handle_leader(message)
                elif msg_type == 'buy':
                    self.handle_buy(message)
                elif msg_type == 'buy_confirmation':
                    self.handle_buy_confirmation(message)
                elif msg_type == 'update_inventory':
                    self.handle_update_inventory(message)
                elif msg_type == 'sell_confirmation':
                    self.handle_sell_confirmation(message)
                # Handle other message types if necessary
            except socket.timeout:
                pass  # Timeout occurred
            except OSError:
                # Socket has been closed
                break
            if self.role == 'buyer':
                self.check_pending_requests()

    def check_pending_requests(self):
        current_time = time.time()
        to_remove = []
        with self.pending_requests_lock:
            for request_id, (product_name, timestamp) in self.pending_requests.items():
                if current_time - timestamp > self.timeout:
                    print(f"[{self.peer_id}] No response received for {product_name}. Timing out and selecting another item.")
                    to_remove.append(request_id)
                    remaining_items = [item for item in self.available_items if item != product_name]
                    if not remaining_items:
                        print(f"[{self.peer_id}] No other items to look up besides {product_name}. Shutting down.")
                        self.shutdown_peer()
                        return
                    new_product = random.choice(remaining_items)
                    quantity = 1
                    print(f"[{self.peer_id}] Searching for a new product: {new_product}")
                    threading.Thread(target=self.buy_item, args=(new_product, quantity), daemon=True).start()
            for request_id in to_remove:
                del self.pending_requests[request_id]

    def send_message(self, addr, message):
        """Send a message to a specific address."""
        try:
            # self.increment_vector_clock()
            message['vector_clock'] = self.get_vector_clock()
            serialized_message = pickle.dumps(message)
            self.socket.sendto(serialized_message, addr)
            print(f"[{self.peer_id}] Sent '{message['type']}' message to {addr}")
        except Exception as e:
            print(f"[{self.peer_id}] Error sending message to {addr}: {e}")

    def send_update_inventory(self):
        """Seller creates this message and sends to the leader."""
        if self.role != 'seller':
            return

        with self.operations_lock:
            self.increment_vector_clock()
            update_inventory_message = UpdateInventoryMessage(
                self.peer_id,
                self.address,
                self.item,
                self.stock,
                self.get_vector_clock()
            )

            if self.leader:
                leader_addr = (self.leader.ip_address, self.leader.port)
                self.send_message(leader_addr, update_inventory_message.to_dict())
                print(f"[{self.peer_id}] Sent inventory update to leader [{self.leader.leader_id}]")
            else:
                print(f"[{self.peer_id}] No leader available to send inventory update.")

    def handle_update_inventory(self, message):
        """When seller sends an update inventory message."""
        if not self.is_leader:
            return
        message = UpdateInventoryMessage.from_dict(message)
        seller_vector_clock = message.vector_clock
        print(f"[{self.peer_id}] Received inventory update from Seller {message.seller_id} with vector clock {seller_vector_clock}")
        # Update vector clock after logging
        self.update_vector_clock(seller_vector_clock)
        with self.inventory_lock:
            self.inventory.add_inventory(
                message.seller_id,
                message.address,
                message.product_name,
                message.stock,
                message.vector_clock
            )
            # self.inventory.persist_inventory_state()
        print(f"[{self.peer_id}] Inventory updated with seller {message.seller_id}")
        print(f"Current Inventory: {self.inventory}")

    def buy_item(self, product_name=None, quantity=None):

        """Buyer will inititate a buy for an item with the trader. similar to lookup_item function from PA1"""

        if self.role != 'buyer':
            return

        with self.operations_lock:

            if product_name is None:
                remaining_items = [item for item in self.available_items if item not in self.looked_up_items]
                if not remaining_items:  # Incase the buyer can not find any sellers for any products [In this case would not happen]
                    print(f"[{self.peer_id}] No more items to look up. Shutting down.")
                    self.shutdown_peer()
                    return
                product_name = random.choice(remaining_items)
                self.looked_up_items.add(product_name)
            else:
                self.looked_up_items.add(product_name)

            if quantity is None:
                quantity = random.randint(1, 5)

            id_string = str(self.peer_id) + product_name + str(time.time())
            if self.role == 'buyer':
                request_id = hashlib.sha256(id_string.encode('utf-8')).hexdigest()
                self.increment_vector_clock()
                buy_message = BuyMessage(request_id, self.peer_id, self.address, product_name, quantity, self.get_vector_clock())

                timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
                print(f"{timestamp} [{self.peer_id}] Initiating buy with trader for {product_name}")
                # print(f"[{self.peer_id} Lookup Message: {look}]")
                if self.start_time is None:
                    self.start_time = time.time()
                # for neighbor in self.neighbors:
                # 	print(f"[{self.peer_id}] Looking for {product_name} with neighbor {neighbor.peer_id}")
                # 	self.send_message((neighbor.ip_address, neighbor.port), lookup_message)
                print(self.leader.address)
                self.send_message(self.leader.address, buy_message.to_dict())
                # # Add to pending requests with a timestamp
                with self.pending_requests_lock:
                    self.pending_requests[request_id] = (product_name, time.time())

    # def handle_buy(self, message):
    #     """Handle a buy request from a buyer."""
    #     if not self.is_leader:
    #         return
    #
    #     message = BuyMessage.from_dict(message)
    #     buyer_vector_clock = message.vector_clock
    #     trader_vector_clock = self.get_vector_clock()
    #
    #     # Log vector clocks before update
    #     print(f"[{self.peer_id}] Trader vector clock before update: {trader_vector_clock}")
    #     print(f"[{self.peer_id}] Buyer {message.buyer_id} vector clock: {buyer_vector_clock}")
    #
    #     # Compare vector clocks
    #     comparison = compare_vector_clocks(trader_vector_clock, buyer_vector_clock)
    #     print(f"[{self.peer_id}] Comparing trader's vector clock with buyer's vector clock: {comparison}")
    #
    #     # Update vector clock after comparison
    #     self.update_vector_clock(buyer_vector_clock)
    #     trader_vector_clock_after = self.get_vector_clock()
    #     print(f"[{self.peer_id}] Trader vector clock after update: {trader_vector_clock_after}")
    #
    #     if comparison == -1:
    #         print(f"[{self.peer_id}] Trader's timestamp is smaller. Proceeding with buy request.")
    #         with self.sell_confirmation_lock:
    #             seller_id, seller_address, status = self.inventory.reduce_stock(message.product_name, message.quantity)
    #             if status:
    #                 print(f"[{self.peer_id}] Sold item to buyer {message.buyer_id}.")
    #                 # Send confirmations
    #                 buy_confirmation_reply = BuyConfirmationMessage(
    #                     message.request_id,
    #                     message.buyer_id,
    #                     message.product_name,
    #                     status,
    #                     message.quantity,
    #                     self.get_vector_clock()
    #                 ).to_dict()
    #                 self.send_message(message.buyer_address, buy_confirmation_reply)
    #                 sell_confirmation_reply = SellConfirmationMessage(
    #                     message.request_id,
    #                     message.buyer_id,
    #                     message.product_name,
    #                     status,
    #                     message.quantity,
    #                     self.get_vector_clock()
    #                 ).to_dict()
    #                 self.send_message(seller_address, sell_confirmation_reply)
    #                 # self.inventory.persist_inventory_state()
    #             else:
    #                 print(f"[{self.peer_id}] Not enough {message.product_name} for Buyer {message.buyer_id}")
    #                 # Send failure confirmation
    #                 buy_confirmation_reply = BuyConfirmationMessage(
    #                     message.request_id,
    #                     message.buyer_id,
    #                     message.product_name,
    #                     False,
    #                     message.quantity,
    #                     self.get_vector_clock()
    #                 ).to_dict()
    #                 self.send_message(message.buyer_address, buy_confirmation_reply)
    #     else:
    #         print(f"[{self.peer_id}] Trader's timestamp is greater than or concurrent. Ignoring buy request.")
    #         # Send failure confirmation
    #         buy_confirmation_reply = BuyConfirmationMessage(
    #             message.request_id,
    #             message.buyer_id,
    #             message.product_name,
    #             False,
    #             message.quantity,
    #             self.get_vector_clock()
    #         ).to_dict()
    #         self.send_message(message.buyer_address, buy_confirmation_reply)

    def handle_buy(self, message: BuyMessage):
        """Handle a buy request from a buyer."""
        #[(message.seller_id, message.address, message.product_name, message.stock), ... ]product_list structure tuple
        if self.role != 'leader':
            return

        message = BuyMessage.from_dict(message)
        with self.sell_confirmation_lock:
            print(self.inventory.inventory)
            seller_id, seller_address, status = self.inventory.reduce_stock(message.product_name, message.quantity)
            if status:
                print(f"[{self.peer_id}] Sold item to buyer {message.buyer_id}.")

        buy_confirmation_reply = BuyConfirmationMessage(
            message.request_id,
            message.buyer_id,
            message.product_name,
            status,
            message.quantity,
            self.get_vector_clock()
        ).to_dict()

        sell_confirmation_reply = SellConfirmationMessage(
            message.request_id,
            message.buyer_id,
            message.product_name,
            status,
            message.quantity,
            self.get_vector_clock()
        ).to_dict()
        self.send_message(message.buyer_address, buy_confirmation_reply)
        if status != False:
            self.send_message(seller_address, sell_confirmation_reply)

    def handle_buy_confirmation(self, message):
        """Handle the buy confirmation from the leader."""
        confirmation_message = BuyConfirmationMessage.from_dict(message)
        if confirmation_message.buyer_id == self.peer_id:
            buyer_vector_clock_before = self.get_vector_clock()
            print(f"[{self.peer_id}] Buyer vector clock before update: {buyer_vector_clock_before}")

            # Update vector clock with the received message
            self.update_vector_clock(confirmation_message.vector_clock)

            buyer_vector_clock_after = self.get_vector_clock()
            print(f"[{self.peer_id}] Buyer vector clock after update: {buyer_vector_clock_after}")

            if confirmation_message.status:
                # Purchase was successful
                self.items_bought += confirmation_message.quantity
                timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
                print(f"{timestamp} [{self.peer_id}] bought product {confirmation_message.product_name} from trader.")
                if self.items_bought >= self.max_transactions:
                    self.end_time = time.time()
                    average_rtt = (self.end_time - self.start_time) / self.max_transactions
                    print(f"[{self.peer_id}] Max transactions reached with average RTT {average_rtt:.4f}. Shutting down peer.")
                    self.average_rtt = average_rtt
                    self.shutdown_peer()
                elif random.random() < BUY_PROBABILITY:
                    print(f"[{self.peer_id}] Buyer decided to continue looking for another item.")
                    remaining_items = [item for item in self.available_items if item != confirmation_message.product_name]
                    if remaining_items:
                        new_product = random.choice(remaining_items)
                        quantity = random.randint(1, 5)
                        threading.Thread(target=self.buy_item, args=(new_product, quantity), daemon=True).start()
                    else:
                        print(f"[{self.peer_id}] No other items to buy. Shutting down.")
                        self.shutdown_peer()
                else:
                    print(f"[{self.peer_id}] Buyer is satisfied and stops buying.")
                    self.shutdown_peer()
            else:
                print(f"[{self.peer_id}] Purchase of {confirmation_message.product_name} from trader failed.")
                # Increment vector clock and retry
                self.increment_vector_clock()
                print(f"[{self.peer_id}] Buyer vector clock after increment: {self.get_vector_clock()}")
                threading.Thread(target=self.buy_item, args=(confirmation_message.product_name, confirmation_message.quantity)).start()
            # Remove from pending requests
            with self.pending_requests_lock:
                if confirmation_message.request_id in self.pending_requests:
                    del self.pending_requests[confirmation_message.request_id]
        else:
            print(f"[{self.peer_id}] Received buy confirmation not intended for this peer.")

    def handle_sell_confirmation(self, message):
        """Handle the sell confirmation received by the seller."""
        confirmation_message = SellConfirmationMessage.from_dict(message)
        if confirmation_message.product_name != self.item or not confirmation_message.status:
            return

        seller_vector_clock_before = self.get_vector_clock()
        print(f"[{self.peer_id}] Seller vector clock before update: {seller_vector_clock_before}")
        # Update vector clock with the received message
        self.update_vector_clock(confirmation_message.vector_clock)
        seller_vector_clock_after = self.get_vector_clock()
        print(f"[{self.peer_id}] Seller vector clock after update: {seller_vector_clock_after}")

        self.stock -= confirmation_message.quantity

        if self.stock <= 0:
            remaining_items = [item for item in self.available_items if item != confirmation_message.product_name]
            if not remaining_items:
                print(f"[{self.peer_id}] No other items to restock. Shutting down.")
                self.shutdown_peer()
                return
            self.item = random.choice(remaining_items)
            self.stock = SELLER_STOCK
            print(f"[{self.peer_id}] Stock reached 0. Restocking and sending new product {self.item} to trader.")
            threading.Thread(target=self.send_update_inventory).start()

    def shutdown_peer(self):
        """Shutdown the peer."""
        print(f"[{self.peer_id}] Shutting down peer.")
        self.running = False
        self.socket.close()
