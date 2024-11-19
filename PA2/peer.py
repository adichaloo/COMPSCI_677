# peer.py
import datetime
import threading
import socket
import pickle
import random
import time
import hashlib
import math

from utils.messages import *
import config
from inventory import *

BUY_PROBABILITY = config.BUY_PROBABILITY
SELLER_STOCK = config.SELLER_STOCK
MAX_TRANSACTIONS = config.MAX_TRANSACTIONS
TIMEOUT = config.TIMEOUT
PRICE = config.PRICE
COMMISSION = config.COMMISSION  ## add logic for comission
'''
MESSAGES
1) UPDATE INVENTORY MESSAGE seller -> Trader
2) BUY MESSAGE buyer-> Trader
3) BUY CONFIRMATION MESSAGE Trader -> Buyer and Trader -> Seller

TODO: Implement thread pool or smth for parallelizability.

BULLY ALGORITHM MESSAGE
4) ELECTION MESSAGE Nodei -> Nodej s.t j > i
5) OK MESSAGE
6) LEADER MESSAGE

1) Start Network, leader initialized at start
2) Seller will send the item it's selling to the trader (UPDATE INVENTORY MESSAGE)
3) Trader will update its inventory
	If multiple sellers for the same item pick one at random.
4) Buyer will initiate a buy (no lookup required)
5) If item not available then return false with "item not available" else True
6) if buy sucessful send buy sucess message to buyer and seller
	if seller.sock == 0, resend inventory update message to trader.
7)

'''


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
            print(
                f"[{self.peer_id}] Updated vector clock from {old_vector_clock} to {self.vector_clock} after receiving {received_vector_clock}")

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
        t = threading.Thread(target=self.listen_for_messages)
        t.start()
        self.thread = t

    # self.start_election_timer()

    def listen_for_messages(self):
        """Continuously listen for incoming messages."""
        while self.running:
            try:
                self.socket.settimeout(1.0)
                data, addr = self.socket.recvfrom(1024)
                message = pickle.loads(data)
                if 'vector_clock' in message:
                    self.update_vector_clock(message['vector_clock'])

                if message.get('type') == 'buy':
                    self.handle_buy(message)
                elif message.get('type') == 'buy_confirmation':
                    self.handle_buy_confirmation(message)
                elif message.get('type') == 'update_inventory':
                    self.handle_update_inventory(message)
                elif message.get('type') == 'sell_confirmation':
                    self.handle_sell_confirmation(message)
                elif message.get('type') == 'election':
                    self.handle_election(message)
                elif message.get('type') == 'OK':
                    self.handle_election_OK(message)
                elif message.get('type') == 'leader':
                    self.handle_leader(message)
            except socket.timeout:
                pass  # Timeout occurred
            except OSError:
                # Socket has been closed
                break
            # For buyers, check for timeouts on pending requests
            if self.role == 'buyer':
                self.check_pending_requests()

    def check_pending_requests(self):
        current_time = time.time()
        to_remove = []
        with self.pending_requests_lock:
            for request_id, (product_name, timestamp) in self.pending_requests.items():
                if current_time - timestamp > self.timeout:
                    print(
                        f"[{self.peer_id}] No response received for {product_name}. Timing out and selecting another item.")
                    to_remove.append(request_id)
                    remaining_items = [item for item in self.available_items if item != product_name]
                    if not remaining_items:
                        print(f"[{self.peer_id}] No other items to look up besides {product_name}. Shutting down.")
                        self.shutdown_peer()
                        return
                    new_product = random.choice(remaining_items)
                    quantity = 1
                    print(f"[{self.peer_id}] Searching for a new product: {new_product}")
                    threading.Thread(target=self.buy_item, args=(new_product, quantity)).start()
            for request_id in to_remove:
                del self.pending_requests[request_id]

    def send_message(self, addr, message):
        """Send a message to a specific address."""
        try:
            # self.increment_vector_clock()
            message['vector_clock'] = self.get_vector_clock()
            if isinstance(addr, str):
                addr = addr
            serialized_message = pickle.dumps(message)
            self.socket.sendto(serialized_message, addr)
        except Exception as e:
            print(f"[{self.peer_id}] Error sending message to {addr}: {e}")

    def send_update_inventory(self):
        ''' Seller creates this message and send to the leader'''
        if self.role != 'seller':
            return

        with self.operations_lock:
            self.increment_vector_clock()
            update_inventory_message = UpdateInventoryMessage(self.peer_id, self.address, self.item, self.stock, self.get_vector_clock())

            leader_addr = (self.leader.ip_address, self.leader.port)
            self.send_message(leader_addr, update_inventory_message.to_dict())
            print(f"[{self.peer_id}] Sent inventory update to leader [{self.leader.leader_id}]")

    def handle_update_inventory(self, message: UpdateInventoryMessage):
        '''When seller sends a update inventory message'''
        if self.role != 'leader':
            return
        print("Update Inventory Message", message)
        message = UpdateInventoryMessage.from_dict(message)
        with self.inventory_lock:
            self.inventory.add_inventory(message.seller_id, message.address, message.product_name, message.stock, message.vector_clock)
        print("Inventory is ", self.inventory)

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
        """Handle the buy confirmation from a seller."""
        confirmation_message = BuyConfirmationMessage.from_dict(message)

        # Check if the confirmation is for the current buyer
        if confirmation_message.buyer_id == self.peer_id:
            if confirmation_message.status:
                # Purchase was successful
                self.items_bought += confirmation_message.quantity
                timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
                print(f"{timestamp} [{self.peer_id}] bought product {confirmation_message.product_name} from trader.")
                self.update_vector_clock(confirmation_message.vector_clock)

                if self.items_bought == self.max_transactions:
                    self.end_time = time.time()
                    # average_rtt =  (self.end_time - self.start_time)/self.max_transactions
                    average_rtt = (self.end_time - self.start_time) / self.max_transactions
                    print(
                        f"[{self.peer_id}] Max transactions reached with average rtt {average_rtt:.4f}.\nShutting down peer.")
                    self.average_rtt = average_rtt
                    self.shutdown_peer()
                elif random.random() < BUY_PROBABILITY:
                    print(f"[{self.peer_id}] Buyer decided to continue looking for another item.")
                    remaining_items = [item for item in self.available_items if
                                       item != confirmation_message.product_name]
                    new_product = random.choice(remaining_items)
                    quantity = random.randint(1, 5)
                    threading.Thread(target=self.buy_item, args=(new_product, quantity)).start()
                else:
                    print(f"[{self.peer_id}] Buyer is satisfied and stops buying.")
                    self.shutdown_peer()
            else:
                # Purchase failed
                print(f"[{self.peer_id}] Purchase of {confirmation_message.product_name} from trader failed.")
                remaining_items = [item for item in self.available_items if item != confirmation_message.product_name]
                new_product = random.choice(remaining_items)
                quantity = random.randint(1, 5)
                print(f"[{self.peer_id}] Buyer will search for another item({new_product}).")

                threading.Thread(target=self.buy_item, args=(new_product, quantity)).start()
            # Remove from pending requests
            with self.pending_requests_lock:
                if confirmation_message.request_id in self.pending_requests:
                    del self.pending_requests[confirmation_message.request_id]
        else:
            print(f"[{self.peer_id}] Received buy confirmation not intended for this peer.")

    def handle_sell_confirmation(self, message):
        ''''''
        confirmation_message = SellConfirmationMessage.from_dict(message)

        if confirmation_message.product_name != self.item or confirmation_message.status == False:
            return

        self.stock -= confirmation_message.quantity
        self.update_vector_clock(confirmation_message.vector_clock)

        if self.stock <= 0:
            remaining_items = [item for item in self.available_items if item != confirmation_message.product_name]
            self.item = random.choice(remaining_items)
            quantity = SELLER_STOCK
            print(f"[{self.peer_id}] Stock reached 0. Restocking and sending new product {self.item} to trader..")
            update_inventory_msg = UpdateInventoryMessage(self.peer_id, self.address, self.item, quantity, self.get_vector_clock()).to_dict()
            self.send_message(self.leader.address, update_inventory_msg)

    def start_election(self):
        """Initiate the election process."""
        if self.in_election:
            return

        print(f"[{self.peer_id}] Initiating election...")
        self.in_election = True
        # self.halt_operations()  # Pause all operations during the election
        self.ok_received = False  # Flag to track if an OK message is received
        self.send_election_messages()

        # Start a timer to wait for OK messages
        threading.Thread(target=self.wait_for_ok_response).start()

    def wait_for_ok_response(self):
        """Wait for OK messages and declare leader if no response is received."""
        time.sleep(config.OK_TIMEOUT)  # Wait for a specified timeout
        if not self.ok_received:
            print(f"[{self.peer_id}] No OK response received. Declaring self as leader.")
            self.declare_leader()
        else:
            print(f"[{self.peer_id}] OK response received. Election process will continue.")

    def send_election_messages(self):
        """Send election message to peers with higher IDs."""
        election_message = {
            'type': 'election',
            'peer_id': self.peer_id
        }
        for neighbor in self.neighbors:
            if neighbor.peer_id > self.peer_id:
                self.send_message((neighbor.ip_address, neighbor.port), election_message)

    def handle_election(self, message):
        """Handle an election message."""
        sender_id = message['peer_id']
        print(f"[{self.peer_id}] Received election message from {sender_id}.")
        if self.peer_id > sender_id:
            self.send_ok_message(sender_id)
            self.start_election()

    def send_ok_message(self, sender_id):
        """Send OK message to the peer who initiated the election."""
        ok_message = {
            'type': 'OK',
            'peer_id': self.peer_id
        }
        sender_addr = None
        for neighbor in self.neighbors:
            if neighbor.peer_id == sender_id:
                sender_addr = (neighbor.ip_address, neighbor.port)
                break
        if sender_addr:
            print(f"[{self.peer_id}] Sending OK message to {sender_id}.")
            self.send_message(sender_addr, ok_message)

    def handle_election_OK(self, message):
        """Handle an OK message."""
        print(f"[{self.peer_id}] Received OK message from {message['peer_id']}.")
        self.ok_received = True
        self.in_election = False  # Stop the election as a higher peer exists

    # self.resume_operations()  # Resume normal operations

    def declare_leader(self):
        """Declare this peer as the new leader."""
        print(f"[{self.peer_id}] Declaring itself as the new leader.")
        self.is_leader = True
        self.current_leader = Leader(self.peer_id, self.ip_address, self.port)
        leader_message = {
            'type': 'leader',
            'leader_id': self.peer_id,
            'ip_address': self.ip_address,
            'port': self.port
        }
        for neighbor in self.neighbors:
            self.send_message((neighbor.ip_address, neighbor.port), leader_message)

    def handle_leader(self, message):
        """Handle a leader message."""
        leader_id = message['leader_id']
        print(f"[{self.peer_id}] New leader announced: {leader_id}.")
        self.current_leader = Leader(leader_id, message['ip_address'], message['port'])
        self.leader = self.current_leader
        self.is_leader = (self.peer_id == leader_id)
        self.in_election = False

    def start_election_timer(self):
        """Start a timer thread to monitor leader status."""
        self.election_timer_thread = threading.Thread(target=self.election_timer)
        self.election_timer_thread.daemon = True
        self.election_timer_thread.start()

    def election_timer(self):
        """Trigger an election if the leader fails after the time quantum."""
        while self.running:
            time.sleep(self.time_quantum)
            if self.role == 'leader':
                # Leader decides whether to fail based on probability p
                if random.random() < config.LEADER_FAILURE_PROBABILITY:
                    print(
                        f"[{self.peer_id}] Leader has failed with probability {config.LEADER_FAILURE_PROBABILITY}. Initiating new election.")
                    self.role = 'peer'  # Demote to regular peer
                    self.start_election()
            elif not self.in_election:
                print(f"[{self.peer_id}] Time quantum expired. Checking leader status.")
                self.start_election()

    def update_leader(self, new_leader):
        """Update the leader reference after an election."""
        self.leader = new_leader
        if self.role != 'leader':
            print(f"Peer {self.peer_id} updated its leader to Peer {new_leader.leader_id}.")

    def display_network(self):
        """Print network structure for this peer."""
        neighbor_ids = [neighbor.peer_id for neighbor in self.neighbors]
        print(f"Peer {self.peer_id} ({self.role}) connected to peers {neighbor_ids}")

    def shutdown_peer(self):
        """Shutdown the peer."""
        print(f"[{self.peer_id}] Shutting down peer.")
        self.running = False
        self.socket.close()

    # The thread will exit when the method returns

    def handle_no_seller(self, message):
        """Handle the 'no_seller' message, which should not occur since we discard messages at hopcount zero."""
        print(f"[{self.peer_id}] Received unexpected 'no_seller' message.")







############################################################################


# import datetime
# import threading
# import socket
# import pickle
# import random
# import time
# import hashlib
# import math
# import os
#
# from utils.messages import *
# import config
# from inventory import *
#
# BUY_PROBABILITY = config.BUY_PROBABILITY
# SELLER_STOCK = config.SELLER_STOCK
# MAX_TRANSACTIONS = config.MAX_TRANSACTIONS
# TIMEOUT = config.TIMEOUT
# PRICE = config.PRICE
# COMMISSION = config.COMMISSION
#
# class Leader:
#     def __init__(self, leader_id, ip_address, port):
#         self.leader_id = leader_id
#         self.ip_address = ip_address
#         self.port = port
#         self.address = (self.ip_address, self.port)
#         self.is_active = True
#         self.inventory = Inventory()
#         if os.path.exists('inventory_state.pkl'):
#             self.load_inventory_state()
#
#     def persist_inventory_state(self):
#         with open('inventory_state.pkl', 'wb') as f:
#             pickle.dump(self.inventory, f)
#         print("Inventory state persisted to disk.")
#
#     def load_inventory_state(self):
#         with open('inventory_state.pkl', 'rb') as f:
#             self.inventory = pickle.load(f)
#         print("Inventory state loaded from disk.")
#
# class Peer:
#     def __init__(self, peer_id, role, neighbors, port, leader, ip_address='localhost', item=None):
#         self.peer_id = peer_id
#         self.role = role  # 'buyer' or 'seller' or 'leader'
#         self.neighbors = neighbors
#         self.ip_address = ip_address
#         self.port = port
#         self.address = (ip_address, self.port)
#         self.item = item
#         self.stock = SELLER_STOCK if role == 'seller' else 0
#         self.lock = threading.Lock()  # For thread safety
#         self.pending_requests_lock = threading.Lock()  # Lock for pending_requests
#         self.sell_confirmation_lock = threading.Lock()
#         self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#         self.socket.bind((self.ip_address, port))
#         self.running = True
#         self.looked_up_items = set()
#         self.available_items = ["fish", "salt", "boar"]
#         self.items_bought = 0
#         self.inventory = Inventory() if self.role == 'leader' else None
#         self.leader = leader if self.role != 'leader' else None
#         self.inventory_lock = threading.Lock()
#         self.operations_lock = threading.Lock()
#         self.total_peers = None
#         self.vector_clock = []
#         self.peer_index = None
#
#         # For buyer timeout handling
#         self.pending_requests = {}
#         self.timeout = TIMEOUT  # seconds
#         if self.role == 'buyer':
#             self.start_time = None
#             self.end_time = None
#             self.average_rtt = time.time()
#             self.max_transactions = MAX_TRANSACTIONS
#
#         self.in_election = False  # Whether the peer is currently in an election
#         self.is_leader = self.role == 'leader'  # Whether this peer is the leader
#         self.current_leader = leader  # The current leader (initially set at the start)
#
#         self.time_quantum = config.TIME_QUANTUM
#         self.election_timer_thread = None
#
#     def set_total_peers(self, total_peers):
#         self.total_peers = total_peers
#         self.vector_clock = [0] * total_peers
#         self.peer_index = self.peer_id  # Assuming peer IDs are from 0 to N-1
#
#     def display_network(self):
#         """Print network structure for this peer."""
#         neighbor_ids = [neighbor.peer_id for neighbor in self.neighbors]
#         print(f"Peer {self.peer_id} ({self.role}) connected to peers {neighbor_ids}")
#
#     def increment_vector_clock(self):
#         """Increment the vector clock for this peer."""
#         with self.lock:
#             self.vector_clock[self.peer_index] += 1
#             print(f"[{self.peer_id}] Incremented vector clock: {self.vector_clock}")
#
#     def get_vector_clock(self):
#         """Get a copy of the current vector clock."""
#         with self.lock:
#             return self.vector_clock.copy()
#
#     def update_vector_clock(self, received_vector_clock):
#         """Update vector clock upon receiving a message."""
#         with self.lock:
#             old_vector_clock = self.vector_clock.copy()
#             for i in range(self.total_peers):
#                 self.vector_clock[i] = max(self.vector_clock[i], received_vector_clock[i])
#             self.vector_clock[self.peer_index] += 1  # Increment own clock after merging
#             print(
#                 f"[{self.peer_id}] Updated vector clock from {old_vector_clock} to {self.vector_clock} after receiving {received_vector_clock}")
#
#     def halt_operations(self):
#         """Halt all operations during an election."""
#         print(f"[{self.peer_id}] Halting all operations due to election.")
#         self.operations_lock.acquire()
#
#     def resume_operations(self):
#         """Resume all operations after the election."""
#
#         if self.operations_lock.locked():
#             self.operations_lock.release()
#         print(f"[{self.peer_id}] Resuming all operations after election.")
#
#     def start_peer(self):
#         """Start listening for messages from other peers."""
#         print(f"Peer {self.peer_id} ({self.role}) with item {self.item} listening on port {self.port}...")
#         t = threading.Thread(target=self.listen_for_messages)
#         t.start()
#         self.thread = t
#
#     def listen_for_messages(self):
#         """Continuously listen for incoming messages."""
#         while self.running:
#             try:
#                 self.socket.settimeout(1.0)
#                 data, addr = self.socket.recvfrom(1024)
#                 message = pickle.loads(data)
#                 if 'vector_clock' in message:
#                     self.update_vector_clock(message['vector_clock'])
#
#                 if message.get('type') == 'buy':
#                     self.handle_buy(message)
#                 elif message.get('type') == 'buy_confirmation':
#                     self.handle_buy_confirmation(message)
#                 elif message.get('type') == 'update_inventory':
#                     self.handle_update_inventory(message)
#                 elif message.get('type') == 'sell_confirmation':
#                     self.handle_sell_confirmation(message)
#                 elif message.get('type') == 'election':
#                     self.handle_election(message)
#                 elif message.get('type') == 'OK':
#                     self.handle_election_OK(message)
#                 elif message.get('type') == 'leader':
#                     self.handle_leader(message)
#             except socket.timeout:
#                 pass  # Timeout occurred
#             except OSError:
#                 # Socket has been closed
#                 break
#             # For buyers, check for timeouts on pending requests
#             if self.role == 'buyer':
#                 self.check_pending_requests()
#
#     def check_pending_requests(self):
#         current_time = time.time()
#         to_remove = []
#         with self.pending_requests_lock:
#             for request_id, (product_name, timestamp) in self.pending_requests.items():
#                 if current_time - timestamp > self.timeout:
#                     print(
#                         f"[{self.peer_id}] No response received for {product_name}. Timing out and selecting another item.")
#                     to_remove.append(request_id)
#                     remaining_items = [item for item in self.available_items if item != product_name]
#                     if not remaining_items:
#                         print(f"[{self.peer_id}] No other items to look up besides {product_name}. Shutting down.")
#                         self.shutdown_peer()
#                         return
#                     new_product = random.choice(remaining_items)
#                     quantity = 1
#                     print(f"[{self.peer_id}] Searching for a new product: {new_product}")
#                     threading.Thread(target=self.buy_item, args=(new_product, quantity)).start()
#             for request_id in to_remove:
#                 del self.pending_requests[request_id]
#
#     def send_message(self, addr, message):
#         """Send a message to a specific address."""
#         try:
#             message['vector_clock'] = self.get_vector_clock()
#             serialized_message = pickle.dumps(message)
#             self.socket.sendto(serialized_message, addr)
#         except Exception as e:
#             print(f"[{self.peer_id}] Error sending message to {addr}: {e}")
#
#     def send_update_inventory(self):
#         ''' Seller creates this message and send to the leader'''
#         if self.role != 'seller':
#             return
#
#         with self.operations_lock:
#             self.increment_vector_clock()
#             update_inventory_message = UpdateInventoryMessage(self.peer_id, self.address, self.item, self.stock, self.get_vector_clock())
#
#             leader_addr = (self.leader.ip_address, self.leader.port)
#             self.send_message(leader_addr, update_inventory_message.to_dict())
#             print(f"[{self.peer_id}] Sent inventory update to leader [{self.leader.leader_id}]")
#
#     def handle_update_inventory(self, message: UpdateInventoryMessage):
#         '''When seller sends an update inventory message'''
#         if self.role != 'leader':
#             return
#         print("Update Inventory Message", message)
#         message = UpdateInventoryMessage.from_dict(message)
#         with self.inventory_lock:
#             self.inventory.add_inventory(message.seller_id, message.address, message.product_name, message.stock, message.vector_clock)
#         print("Inventory is ", self.inventory)
#         self.persist_inventory_state()  # Save the updated inventory
#
#     def buy_item(self, product_name=None, quantity=None):
#         """Buyer will initiate a buy for an item with the trader."""
#         if self.role != 'buyer':
#             return
#
#         with self.operations_lock:
#
#             if product_name is None:
#                 remaining_items = [item for item in self.available_items if item not in self.looked_up_items]
#                 if not remaining_items:
#                     print(f"[{self.peer_id}] No more items to look up. Shutting down.")
#                     self.shutdown_peer()
#                     return
#                 product_name = random.choice(remaining_items)
#                 self.looked_up_items.add(product_name)
#             else:
#                 self.looked_up_items.add(product_name)
#
#             if quantity is None:
#                 quantity = random.randint(1, 5)
#
#             id_string = str(self.peer_id) + product_name + str(time.time())
#             if self.role == 'buyer':
#                 request_id = hashlib.sha256(id_string.encode('utf-8')).hexdigest()
#                 self.increment_vector_clock()
#                 buy_message = BuyMessage(request_id, self.peer_id, self.address, product_name, quantity, self.get_vector_clock())
#
#                 timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
#                 print(f"{timestamp} [{self.peer_id}] Initiating buy with trader for {product_name}")
#                 if self.start_time is None:
#                     self.start_time = time.time()
#                 self.send_message(self.leader.address, buy_message.to_dict())
#                 with self.pending_requests_lock:
#                     self.pending_requests[request_id] = (product_name, time.time())
#
#     def handle_buy(self, message: BuyMessage):
#         """Handle a buy request from a buyer."""
#         if self.role != 'leader':
#             return
#
#         message = BuyMessage.from_dict(message)
#         with self.sell_confirmation_lock:
#             print(self.inventory.inventory)
#             seller_id, seller_address, status = self.inventory.reduce_stock(message.product_name, message.quantity)
#             if status:
#                 print(f"[{self.peer_id}] Sold item to buyer {message.buyer_id}.")
#
#         buy_confirmation_reply = BuyConfirmationMessage(
#             message.request_id,
#             message.buyer_id,
#             message.product_name,
#             status,
#             message.quantity
#         ).to_dict()
#
#         sell_confirmation_reply = SellConfirmationMessage(
#             message.request_id,
#             message.buyer_id,
#             message.product_name,
#             status,
#             message.quantity
#         ).to_dict()
#         self.send_message(message.buyer_address, buy_confirmation_reply)
#         if status != False:
#             self.send_message(seller_address, sell_confirmation_reply)
#         self.persist_inventory_state()  # Save inventory state after buy
#
#     def handle_buy_confirmation(self, message):
#         """Handle the buy confirmation from a seller."""
#         confirmation_message = BuyConfirmationMessage.from_dict(message)
#
#         if confirmation_message.buyer_id == self.peer_id:
#             if confirmation_message.status:
#                 self.items_bought += confirmation_message.quantity
#                 timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
#                 print(f"{timestamp} [{self.peer_id}] bought product {confirmation_message.product_name} from trader.")
#
#                 if self.items_bought == self.max_transactions:
#                     self.end_time = time.time()
#                     average_rtt = (self.end_time - self.start_time) / self.max_transactions
#                     print(
#                         f"[{self.peer_id}] Max transactions reached with average rtt {average_rtt:.4f}.\nShutting down peer.")
#                     self.average_rtt = average_rtt
#                     self.shutdown_peer()
#                 elif random.random() < BUY_PROBABILITY:
#                     print(f"[{self.peer_id}] Buyer decided to continue looking for another item.")
#                     remaining_items = [item for item in self.available_items if
#                                        item != confirmation_message.product_name]
#                     new_product = random.choice(remaining_items)
#                     quantity = random.randint(1, 5)
#                     threading.Thread(target=self.buy_item, args=(new_product, quantity)).start()
#                 else:
#                     print(f"[{self.peer_id}] Buyer is satisfied and stops buying.")
#                     self.shutdown_peer()
#             else:
#                 print(f"[{self.peer_id}] Purchase of {confirmation_message.product_name} from trader failed.")
#                 remaining_items = [item for item in self.available_items if item != confirmation_message.product_name]
#                 new_product = random.choice(remaining_items)
#                 quantity = random.randint(1, 5)
#                 print(f"[{self.peer_id}] Buyer will search for another item({new_product}).")
#
#                 threading.Thread(target=self.buy_item, args=(new_product, quantity)).start()
#             with self.pending_requests_lock:
#                 if confirmation_message.request_id in self.pending_requests:
#                     del self.pending_requests[confirmation_message.request_id]
#         else:
#             print(f"[{self.peer_id}] Received buy confirmation not intended for this peer.")
#
#     def handle_sell_confirmation(self, message):
#         '''Handle the sell confirmation received by the seller.'''
#         confirmation_message = SellConfirmationMessage.from_dict(message)
#
#         if confirmation_message.product_name != self.item or confirmation_message.status == False:
#             return
#
#         self.stock -= confirmation_message.quantity
#
#         if self.stock <= 0:
#             remaining_items = [item for item in self.available_items if item != confirmation_message.product_name]
#             self.item = random.choice(remaining_items)
#             quantity = SELLER_STOCK
#             print(f"[{self.peer_id}] Stock reached 0. Restocking and sending new product {self.item} to trader..")
#             update_inventory_msg = UpdateInventoryMessage(self.peer_id, self.address, self.item, quantity, self.get_vector_clock()).to_dict()
#             self.send_message(self.leader.address, update_inventory_msg)
#
#     def start_election(self):
#         """Initiate the election process."""
#         if self.in_election:
#             return
#
#         print(f"[{self.peer_id}] Initiating election...")
#         self.in_election = True
#         self.ok_received = False
#         self.send_election_messages()
#         threading.Thread(target=self.wait_for_ok_response).start()
#
#     def wait_for_ok_response(self):
#         """Wait for OK messages and declare leader if no response is received."""
#         time.sleep(config.OK_TIMEOUT)
#         if not self.ok_received:
#             print(f"[{self.peer_id}] No OK response received. Declaring self as leader.")
#             self.declare_leader()
#         else:
#             print(f"[{self.peer_id}] OK response received. Election process will continue.")
#
#     def send_election_messages(self):
#         """Send election message to peers with higher IDs."""
#         election_message = {
#             'type': 'election',
#             'peer_id': self.peer_id
#         }
#         for neighbor in self.neighbors:
#             if neighbor.peer_id > self.peer_id:
#                 self.send_message((neighbor.ip_address, neighbor.port), election_message)
#
#     def handle_election(self, message):
#         """Handle an election message."""
#         sender_id = message['peer_id']
#         print(f"[{self.peer_id}] Received election message from {sender_id}.")
#         if self.peer_id > sender_id:
#             self.send_ok_message(sender_id)
#             self.start_election()
#
#     def send_ok_message(self, sender_id):
#         """Send OK message to the peer who initiated the election."""
#         ok_message = {
#             'type': 'OK',
#             'peer_id': self.peer_id
#         }
#         sender_addr = None
#         for neighbor in self.neighbors:
#             if neighbor.peer_id == sender_id:
#                 sender_addr = (neighbor.ip_address, neighbor.port)
#                 break
#         if sender_addr:
#             print(f"[{self.peer_id}] Sending OK message to {sender_id}.")
#             self.send_message(sender_addr, ok_message)
#
#     def handle_election_OK(self, message):
#         """Handle an OK message."""
#         print(f"[{self.peer_id}] Received OK message from {message['peer_id']}.")
#         self.ok_received = True
#         self.in_election = False
#
#     def declare_leader(self):
#         """Declare this peer as the new leader."""
#         print(f"[{self.peer_id}] Declaring itself as the new leader.")
#         self.is_leader = True
#         self.current_leader = Leader(self.peer_id, self.ip_address, self.port)
#         self.current_leader.persist_inventory_state()  # Persist the initial inventory state
#         leader_message = {
#             'type': 'leader',
#             'leader_id': self.peer_id,
#             'ip_address': self.ip_address,
#             'port': self.port
#         }
#         for neighbor in self.neighbors:
#             self.send_message((neighbor.ip_address, neighbor.port), leader_message)
#
#     def handle_leader(self, message):
#         """Handle a leader message."""
#         leader_id = message['leader_id']
#         print(f"[{self.peer_id}] New leader announced: {leader_id}.")
#         self.current_leader = Leader(leader_id, message['ip_address'], message['port'])
#         self.leader = self.current_leader
#         self.is_leader = (self.peer_id == leader_id)
#         self.in_election = False
#
#     def shutdown_peer(self):
#         """Shutdown the peer."""
#         print(f"[{self.peer_id}] Shutting down peer.")
#         self.running = False
#         self.socket.close()
