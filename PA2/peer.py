# peer.py

import datetime
import threading
import socket
import pickle
import random
import time
import hashlib
import functools
from concurrent.futures import ThreadPoolExecutor

from utils.messages import *
import config
from inventory import *

BUY_PROBABILITY = config.BUY_PROBABILITY
SELLER_STOCK = config.SELLER_STOCK
TIMEOUT = config.TIMEOUT
PRICE = config.PRICE
COMMISSION = config.COMMISSION
OK_TIMEOUT = config.OK_TIMEOUT
MAX_THREADS = 20  # Maximum number of threads in the pool

class Peer:
    def __init__(self, peer_id, role, neighbors, port, leader=None, ip_address='localhost', item=None, previous_leaders= None, previous_leaders_lock=None):
        self.peer_id = peer_id
        self.role = role  # Now a set: {'buyer'}, {'seller'}, {'buyer', 'seller'}, or {'leader'}
        self.neighbors = neighbors  # List of other Peer instances
        self.ip_address = ip_address
        self.port = port
        self.address = (ip_address, self.port)
        self.item = item
        self.stock = SELLER_STOCK if 'seller' in role else 0
        self.lock = threading.RLock()  # For thread safety
        self.inventory_lock = threading.RLock()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.ip_address, port))
        self.running = True
        self.looked_up_items = set()
        self.available_items = ["fish", "salt", "boar"]
        self.items_bought = 0
        self.is_leader = 'leader' in role
        self.leader = leader if not self.is_leader else self
        self.operations_lock = threading.Lock()
        self.total_peers = None
        self.vector_clock = []
        self.peer_index = None
        self.pending_requests_lock = threading.Lock()
        self.leader_active = True
        self.previous_leaders = previous_leaders
        self.previous_leaders_lock = previous_leaders_lock
        self.thread_pool = ThreadPoolExecutor(max_workers=MAX_THREADS)
        # if self.previous_leaders is None:
        #     self.previous_leaders = set()

        # Initialize buyer-specific attributes
        if 'buyer' in self.role:
            self.pending_requests = {}
            self.timeout = TIMEOUT  # seconds
            self.start_time = None
            self.end_time = None

        if 'seller' in self.role:
            self.earnings = 0.0  # Initialize earnings to zero

        if self.is_leader:
            self.earnings = 0.0  # Leader's earnings from commissions

        # Initialize leader-specific attributes
        if self.is_leader:
            self.inventory = Inventory()
            self.pending_buy_requests = []
            self.pending_buy_requests_lock = threading.Lock()
            self.inventory_lock = threading.Lock()
            # Load market state from disk
            self.load_market_state()
            # Start the pending buy requests processing thread
            threading.Thread(target=self.process_pending_buy_requests_thread, daemon=True).start()
        else:
            self.inventory = None

        # Election attributes
        self.in_election = False
        self.ok_received = False
        self.current_leader = leader

    def set_total_peers(self, total_peers):
        self.total_peers = total_peers
        self.vector_clock = [0] * total_peers
        self.peer_index = self.peer_id  # Assuming peer IDs are from 0 to N-1

    def save_market_state(self):
        """Save the market state to disk."""
        if self.is_leader:
            # No need to acquire inventory_lock here; it's already held
            try:
                self.inventory.save_to_disk()
                print(f"[{self.peer_id}] Market state saved to disk.")
            except Exception as e:
                print(f"[{self.peer_id}] Error saving market state: {e}")

    def load_market_state(self):
        """Load the market state from disk."""
        if self.is_leader:
            try:
                self.inventory.load_from_disk()
                print(f"[{self.peer_id}] Market state loaded from disk.")
            except Exception as e:
                print(f"[{self.peer_id}] Error loading market state: {e}")

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

    def start_peer(self):
        """Start listening for messages from other peers."""
        print(f"Peer {self.peer_id} ({self.role}) with item {self.item} listening on port {self.port}...")
        t = threading.Thread(target=self.listen_for_messages)
        t.start()
        self.thread = t

    def listen_for_messages(self):
        """Continuously listen for incoming messages."""
        while self.running:
            try:
                self.socket.settimeout(1.0)
                data, addr = self.socket.recvfrom(4096)
                message = pickle.loads(data)
                self.thread_pool.submit(self.handle_message, message, addr)
            except socket.timeout:
                pass  # Timeout occurred
            except OSError:
                # Socket has been closed
                break

    def handle_message(self, message, addr):
        """Handle incoming messages using thread pool."""
        message_type = message.get('type')
        if 'vector_clock' in message:
            self.update_vector_clock(message['vector_clock'])

        if message_type == 'buy':
            self.handle_buy(message)
        elif message_type == 'buy_confirmation':
            self.handle_buy_confirmation(message)
        elif message_type == 'update_inventory':
            self.handle_update_inventory(message)
        elif message_type == 'sell_confirmation':
            self.handle_sell_confirmation(message)
        elif message_type == 'election':
            self.handle_election(message)
        elif message_type == 'OK':
            self.handle_election_OK(message)
        elif message_type == 'leader':
            self.handle_leader(message)


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
                    quantity = random.randint(1, 5)
                    print(f"[{self.peer_id}] Searching for a new product: {new_product}")
                    threading.Thread(target=self.buy_item, args=(new_product, quantity)).start()
            for request_id in to_remove:
                del self.pending_requests[request_id]

    def send_message(self, addr, message):
        """Send a message to a specific address."""
        try:
            # self.increment_vector_clock()
            message['vector_clock'] = self.get_vector_clock()
            serialized_message = pickle.dumps(message)
            self.socket.sendto(serialized_message, addr)
        except Exception as e:
            print(f"[{self.peer_id}] Error sending message to {addr}: {e}")

    def send_update_inventory(self):
        """Seller sends inventory update to the leader."""
        if 'seller' not in self.role:
            return

        with self.operations_lock:
            # Wait for leader to be available
            if self.leader is None or not self.leader.leader_active:
                print(f"[{self.peer_id}] No leader available. Waiting for a leader to be elected.")
                while (self.leader is None or not self.leader.leader_active) and self.running:
                    time.sleep(1)
                if not self.running:
                    return

            self.increment_vector_clock()
            update_inventory_message = UpdateInventoryMessage(
                self.peer_id, self.address, self.item, self.stock, self.get_vector_clock()
            )

            leader_addr = (self.leader.ip_address, self.leader.port)

            # Print the message contents
            print(f"[{self.peer_id}] Sending UpdateInventoryMessage to leader {self.leader.peer_id}:")
            print(f"    Seller ID: {update_inventory_message.seller_id}")
            print(f"    Address: {update_inventory_message.address}")
            print(f"    Product Name: {update_inventory_message.product_name}")
            print(f"    Stock: {update_inventory_message.stock}")
            print(f"    Vector Clock: {update_inventory_message.vector_clock}")

            self.send_message(leader_addr, update_inventory_message.to_dict())
            print(f"[{self.peer_id}] Sent inventory update to leader [{self.leader.peer_id}]")
    #
    def handle_update_inventory(self, message: UpdateInventoryMessage):
        """Handle inventory update from a seller."""
        if not self.is_leader:
            return
        message = UpdateInventoryMessage.from_dict(message)
        with self.inventory_lock:
            self.inventory.add_inventory(
                message.seller_id, message.address, message.product_name, message.stock, message.vector_clock
            )
            print(f"[{self.peer_id}] Updated inventory with seller {message.seller_id} for product {message.product_name}")
            # Save market state
            self.save_market_state()
            # Print current inventory
            print(f"[{self.peer_id}] Current Inventory:")
            for product, sellers_info in self.inventory.inventory.items():
                print(f"    Product: {product}")
                for seller_info in sellers_info:
                    print(f"        Seller {seller_info['seller_id']}: Stock {seller_info['quantity']}, Address {seller_info['address']}, Vector Clock {seller_info['vector_clock']}")
        # After updating inventory, process pending buy requests
        self.thread_pool.submit(self.process_pending_buy_requests)

    #
    def process_pending_buy_requests_thread(self):
        """Background thread to process pending buy requests periodically."""
        while self.running and self.leader_active:
            self.process_pending_buy_requests()
            time.sleep(1)  # Adjust the sleep time as needed
    #
    def process_pending_buy_requests(self):
        """Process pending buy requests by selecting the earliest based on vector clocks."""
        while True:
            with self.pending_buy_requests_lock:
                if not self.pending_buy_requests:
                    break
                # Sort the pending buy requests based on vector clocks
                self.pending_buy_requests.sort(key=functools.cmp_to_key(self.compare_buy_requests))
                # Pop the first buy request
                message = self.pending_buy_requests.pop(0)
            # Process the buy request
            self.thread_pool.submit(self.execute_buy_request, message)
    #
    def execute_buy_request(self, message: BuyMessage):
        """Execute a buy request."""
        with self.inventory_lock:
            seller_id, seller_address, status = self.inventory.reduce_stock(message.product_name, message.quantity)
            if status:
                print(f"[{self.peer_id}] Sold item to buyer {message.buyer_id}.")
                # Calculate payment and commission
                payment_amount = PRICE * message.quantity
                commission = COMMISSION * payment_amount
                seller_payment = payment_amount - commission
                # Update leader's earnings
                self.earnings += commission
            else:
                print(f"[{self.peer_id}] Could not fulfill the order for buyer {message.buyer_id}.")

        buy_confirmation_reply = BuyConfirmationMessage(
            message.request_id,
            message.buyer_id,
            message.product_name,
            status,
            message.quantity,
            self.get_vector_clock()
        ).to_dict()

        self.send_message(message.buyer_address, buy_confirmation_reply)
        if status:
            sell_confirmation_reply = SellConfirmationMessage(
                message.request_id,
                message.buyer_id,
                message.product_name,
                status,
                message.quantity,
                self.get_vector_clock(),
                seller_payment,
            ).to_dict()
            self.send_message(seller_address, sell_confirmation_reply)
    #
    def buy_item(self, product_name=None, quantity=None):
        """Buyer initiates a buy for an item with the trader."""
        if 'buyer' not in self.role:
            return

        with self.operations_lock:
            # Wait for leader to be available
            while (self.leader is None or not self.leader.leader_active) and self.running:
                print(f"[{self.peer_id}] No leader available. Waiting for a leader to be elected.")
                time.sleep(1)
            if not self.running:
                return

            if product_name is None:
                product_name = random.choice(self.available_items)
            if quantity is None:
                quantity = random.randint(1, 5)

            id_string = str(self.peer_id) + product_name + str(time.time())
            request_id = hashlib.sha256(id_string.encode('utf-8')).hexdigest()
            self.increment_vector_clock()
            buy_message = BuyMessage(
                request_id, self.peer_id, self.address, product_name, quantity, self.get_vector_clock()
            )

            timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
            print(f"{timestamp} [{self.peer_id}] Initiating buy with trader for {product_name}")

            if self.start_time is None:
                self.start_time = time.time()

            self.send_message((self.leader.ip_address, self.leader.port), buy_message.to_dict())

            # Add to pending requests with a timestamp
            with self.pending_requests_lock:
                self.pending_requests[request_id] = (product_name, time.time())

    def handle_buy(self, message: BuyMessage):
        """Handle a buy request from a buyer."""
        if not self.is_leader or not self.leader_active:
            return

        message = BuyMessage.from_dict(message)
        with self.pending_buy_requests_lock:
            self.pending_buy_requests.append(message)
        # Buy requests will be processed by the background thread

    def delayed_buy(self, wait_time):
        """Wait for a specified time before initiating another buy."""
        time.sleep(wait_time)
        self.buy_item()

    def handle_buy_confirmation(self, message):
        """Handle the buy confirmation from the trader."""
        confirmation_message = BuyConfirmationMessage.from_dict(message)

        # Check if the confirmation is for the current buyer
        if confirmation_message.buyer_id == self.peer_id:
            if confirmation_message.status:
                # Purchase was successful
                self.items_bought += confirmation_message.quantity
                timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
                print(f"{timestamp} [{self.peer_id}] bought {confirmation_message.quantity} {confirmation_message.product_name}(s) from trader.")
                self.update_vector_clock(confirmation_message.vector_clock)

                # Decide whether to continue buying
                wait_time = random.randint(5, 10)
                self.thread_pool.submit(self.delayed_buy, wait_time)
                print(f"[{self.peer_id}] Buyer will make another purchase in {wait_time} seconds.")
            else:
                # Purchase failed
                print(f"[{self.peer_id}] Purchase of {confirmation_message.product_name} from trader failed.")
                # Try buying a different item after a delay
                wait_time = random.randint(5, 10)
                self.thread_pool.submit(self.delayed_buy, wait_time)
                print(f"[{self.peer_id}] Buyer will try a different item in {wait_time} seconds.")
            # Remove from pending requests
            with self.pending_requests_lock:
                if confirmation_message.request_id in self.pending_requests:
                    del self.pending_requests[confirmation_message.request_id]
        else:
            print(f"[{self.peer_id}] Received buy confirmation not intended for this peer.")

    # def wait_and_send_update_inventory(self):
    #     """Wait until the leader is active and then send inventory update."""
    #     while (self.leader is None or not self.leader.leader_active) and self.running:
    #         print(f"[{self.peer_id}] Waiting for an active leader to send inventory update.")
    #         time.sleep(1)
    #         # Optionally, initiate an election if desired
    #         # self.start_election()
    #     if not self.running:
    #         return
    #     self.send_update_inventory()
    #
    def handle_sell_confirmation(self, message):
        """Handle sell confirmation from the leader."""
        confirmation_message = SellConfirmationMessage.from_dict(message)

        if confirmation_message.product_name != self.item or confirmation_message.status == False:
            return

        self.stock -= confirmation_message.quantity
        self.update_vector_clock(confirmation_message.vector_clock)

        # Update seller's earnings
        self.earnings += confirmation_message.payment_amount
        print(f"[{self.peer_id}] Received payment of {confirmation_message.payment_amount} for selling {confirmation_message.quantity} {confirmation_message.product_name}(s). Total earnings: {self.earnings}")

        if self.stock <= 0:
            self.stock = SELLER_STOCK  # Restock
            print(f"[{self.peer_id}] Stock reached 0. Restocking {self.item} and sending update to leader.")
            self.thread_pool.submit(self.send_update_inventory)


    def display_network(self):
        """Print network structure for this peer."""
        neighbor_ids = [neighbor.peer_id for neighbor in self.neighbors]
        print(f"Peer {self.peer_id} ({self.role}) connected to peers {neighbor_ids}")

    def shutdown_peer(self):
        """Shutdown the peer."""
        print(f"[{self.peer_id}] Shutting down peer.")
        self.running = False
        self.socket.close()
        self.thread_pool.shutdown(wait=False)

    # Comparison functions for vector clocks

    @staticmethod
    def compare_vector_clocks(vc1, vc2):
        """Compare two vector clocks."""
        less = False
        greater = False
        for v1, v2 in zip(vc1, vc2):
            if v1 < v2:
                less = True
            elif v1 > v2:
                greater = True
        if less and not greater:
            return -1
        elif greater and not less:
            return 1
        else:
            return 0  # Concurrent

    @staticmethod
    def compare_buy_requests(buy_request1, buy_request2):
        comp = Peer.compare_vector_clocks(buy_request1.vector_clock, buy_request2.vector_clock)
        if comp == -1:
            return -1
        elif comp == 1:
            return 1
        else:
            # If concurrent, break ties using buyer IDs
            if buy_request1.buyer_id < buy_request2.buyer_id:
                return -1
            elif buy_request1.buyer_id > buy_request2.buyer_id:
                return 1
            else:
                return 0

    def start_election(self, previous_leaders):
        """Initiate the election process."""
        if self.in_election:
            return

        print(f"[{self.peer_id}] Initiating election...")
        self.in_election = True
        self.ok_received = False  # Flag to track if an OK message is received
        self.previous_leaders = previous_leaders  # Reference to the global previous_leaders set
        self.send_election_messages()

        # Start a timer to wait for OK messages
        threading.Thread(target=self.wait_for_ok_response).start()

    def wait_for_ok_response(self):
        """Wait for OK messages and declare leader if no response is received."""
        time.sleep(OK_TIMEOUT)  # Wait for a specified timeout
        # print("################################")
        # print(self.ok_received)
        # print(self.peer_id)
        # print(self.previous_leaders)
        # print("################################")
        if not self.ok_received and self.peer_id not in self.previous_leaders:
        # if not self.ok_received and self.peer_id == self.previous_leaders:
            print(f"[{self.peer_id}] No OK response received. Declaring self as leader.")
            self.declare_leader()
        else:
            print(f"[{self.peer_id}] OK response received or not eligible. Election process will continue.")
            self.in_election = False

    def send_election_messages(self):
        """Send election message to peers with higher IDs."""
        election_message = {
            'type': 'election',
            'peer_id': self.peer_id
        }
        for neighbor in self.neighbors:
            if neighbor.peer_id > self.peer_id and neighbor.running:
                self.send_message((neighbor.ip_address, neighbor.port), election_message)

    # def handle_election(self, message):
    #     """Handle an election message."""
    #     sender_id = message['peer_id']
    #     print(f"[{self.peer_id}] Received election message from {sender_id}.")

        # if self.previous_leaders is None:
        #     # Log an error or warning if this should not happen
        #     print(f"[{self.peer_id}] Warning: previous_leaders is None. Initializing to empty set.")
        #     self.previous_leaders = set()

        # with self.previous_leaders_lock:
        #     # Check if self is eligible to be leader (not in previous leaders)
        #     if self.peer_id not in self.previous_leaders:
        #         if self.peer_id > sender_id:
        #             self.send_ok_message(sender_id)
        #             self.start_election(self.previous_leaders)
        #         elif self.peer_id == sender_id:
        #             # Tie-breaker: higher peer_id wins
        #             if self.peer_id > sender_id:
        #                 self.send_ok_message(sender_id)
        #                 self.start_election(self.previous_leaders)
        #     else:
        #         # If self is not eligible, forward the election message
        #         self.send_ok_message(sender_id)

    def handle_election(self, message):
        """Handle an election message."""
        sender_id = message['peer_id']
        print(f"[{self.peer_id}] Received election message from {sender_id}.")

        with self.previous_leaders_lock:
            if self.peer_id not in self.previous_leaders:
            # if self.peer_id != self.previous_leaders:
                if self.peer_id > sender_id:
                    # Eligible and higher ID
                    self.send_ok_message(sender_id)
                    self.start_election(self.previous_leaders)
                # If lower ID, do nothing
            else:
                # Ineligible peers should not respond
                print(f"[{self.peer_id}] Ineligible to be leader. Ignoring election message from {sender_id}.")

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

    def declare_leader(self):
        """Declare this peer as the new leader."""
        print(f"[{self.peer_id}] Declaring itself as the new leader.")
        self.is_leader = True
        self.leader = self
        self.leader_active = True
        # Initialize leader-specific attributes
        self.inventory = Inventory()
        self.pending_buy_requests = []
        self.pending_buy_requests_lock = threading.Lock()
        self.inventory_lock = threading.Lock()
        # Load market state
        self.load_market_state()
        # Start the pending buy requests processing thread
        threading.Thread(target=self.process_pending_buy_requests_thread, daemon=True).start()
        leader_message = {
            'type': 'leader',
            'leader_id': self.peer_id,
            'ip_address': self.ip_address,
            'port': self.port
        }
        for neighbor in self.neighbors:
            if neighbor.running:
                self.send_message((neighbor.ip_address, neighbor.port), leader_message)
        self.in_election = False


    def handle_leader(self, message):
        """Handle a leader message."""
        leader_id = message['leader_id']
        print(f"[{self.peer_id}] New leader announced: {leader_id}.")
        if leader_id != self.peer_id:
            self.is_leader = False
            self.leader_active = False # Leader is inactive
            # Update leader reference
            # self.leader = next((peer for peer in self.neighbors if peer.peer_id == leader_id), None)
            # if self.leader is None:
            #     # Create a new Peer instance for the leader if not found
            #     self.leader = Peer(leader_id, {'leader'}, [], message['port'], ip_address=message['ip_address'], previous_leaders=self.previous_leaders)
            new_leader = next((peer for peer in self.neighbors if peer.peer_id == leader_id and peer.running), None)
            if new_leader:
                self.leader = new_leader
                print(f"[{self.peer_id}] Updated leader to Peer {leader_id}.")
            else:
                print(f"[{self.peer_id}] Leader {leader_id} not found among neighbors.")
                self.leader = None
        else:
            self.is_leader = True
            self.leader = self
            self.leader_active = True  # Leader is active
            # Initialize leader-specific attributes
            self.inventory = Inventory()
            self.pending_buy_requests = []
            self.pending_buy_requests_lock = threading.Lock()
            self.inventory_lock = threading.Lock()
            # Load market state
            self.load_market_state()
            # Start the pending buy requests processing thread
            threading.Thread(target=self.process_pending_buy_requests_thread, daemon=True).start()
            # threading.Thread(target=self.start_election(self.previous_leaders), daemon=True).start()
        self.in_election = False
