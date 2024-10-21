# peer.py
import datetime
import threading
import socket
import pickle
import random
import time
import hashlib
import math

from utils.messages import LookupMessage, ReplyMessage, BuyMessage, BuyConfirmationMessage
import config

BUY_PROBABILITY = config.BUY_PROBABILITY
SELLER_STOCK = config.SELLER_STOCK
MAX_TRANSACTIONS = config.MAX_TRANSACTIONS
TIMEOUT = config.TIMEOUT

class Peer:
    def __init__(self, peer_id, role, neighbors, port, ip_address='localhost', item=None, cache_size=math.inf, hop_count=3, max_distance=3):
        self.cache = {}
        self.cache_size = cache_size
        self.peer_id = peer_id
        self.role = role  # 'buyer' or 'seller'
        self.neighbors = neighbors
        self.ip_address = ip_address
        self.port = port
        self.item = item
        self.stock = SELLER_STOCK if role == 'seller' else 0
        self.lock = threading.Lock()  # For thread safety
        self.pending_requests_lock = threading.Lock()  # Lock for pending_requests
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.ip_address, port))
        self.running = True
        self.looked_up_items = set()
        self.hop_count = hop_count
        self.max_distance = max_distance
        self.available_items = ["fish", "salt", "boar"]
        self.items_bought = 0

        # For buyer timeout handling
        self.pending_requests = {}
        self.timeout = TIMEOUT  # seconds
        if self.role == 'buyer':
            self.start_time = None
            self.end_time = None
            self.average_rtt = time.time()
            self.max_transactions = MAX_TRANSACTIONS



    def start_peer(self):
        """Start listening for messages from other peers."""
        print(f"Peer {self.peer_id} ({self.role}) with item {self.item} listening on port {self.port}...")
        t = threading.Thread(target=self.listen_for_messages)
        t.start()
        self.thread = t  # Keep a reference to the thread

    def listen_for_messages(self):
        """Continuously listen for incoming messages."""
        while self.running:
            try:
                self.socket.settimeout(1.0)
                data, addr = self.socket.recvfrom(1024)
                message = pickle.loads(data)
                # print(f"[{self.peer_id}] Received Message: {message}")
                if message.get('type') == 'lookup':
                    self.handle_lookup(message, addr)
                elif message.get('type') == 'reply':
                    self.handle_reply(message)
                elif message.get('type') == 'buy':
                    self.handle_buy(message, addr)
                elif message.get('type') == 'buy_confirmation':
                    self.handle_buy_confirmation(message)
                elif message.get('type') == 'no_seller':
                    self.handle_no_seller(message)
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
                    print(f"[{self.peer_id}] No response received for {product_name}. Timing out and selecting another item.")
                    to_remove.append(request_id)
                    remaining_items = [item for item in self.available_items if item != product_name]
                    if not remaining_items:
                        print(f"[{self.peer_id}] No other items to look up besides {product_name}. Shutting down.")
                        self.shutdown_peer()
                        return
                    new_product = random.choice(remaining_items)
                    print(f"[{self.peer_id}] Searching for a new product: {new_product}")
                    threading.Thread(target=self.lookup_item, args=(new_product, self.max_distance)).start()
            for request_id in to_remove:
                del self.pending_requests[request_id]

    def send_message(self, addr, message):
        """Send a message to a specific address."""
        try:
            serialized_message = pickle.dumps(message)
            self.socket.sendto(serialized_message, addr)
        except Exception as e:
            print(f"[{self.peer_id}] Error sending message to {addr}: {e}")

    def handle_lookup(self, message, addr):
        """Handle a lookup request from a buyer or peer."""
        req_id = message['request_id']
        if req_id not in self.cache:
            if len(self.cache) > self.cache_size:
                # Evict the first item
                first_key = next(iter(self.cache))
                del self.cache[first_key]

            self.cache[req_id] = message
            buyer_id = message['buyer_id']
            product_name = message['product_name']
            hopcount = message['hop_count']
            search_path = message['search_path']
    
            # If this peer is a seller and has the requested product, reply to the buyer
            if self.role == 'seller' and self.item == product_name and self.stock > 0:
                next_peer_info = search_path[-1]
                addr = (next_peer_info[1], next_peer_info[2])
                reply_message = ReplyMessage(
                    self.peer_id,
                    reply_path=search_path[:-1],
                    seller_addr=(self.ip_address, self.port),
                    product_name=product_name,
                    request_id=req_id
                ).to_dict()
    
                self.send_message(addr, reply_message)
                print(f"[{self.peer_id}] Sent reply for {req_id} to peer {next_peer_info[0]} for item {product_name}")
    
            # If hopcount > 0, propagate the lookup to neighbors
            elif hopcount > 0:
                search_path.append((self.peer_id, self.ip_address, self.port))
                hopcount_new = hopcount - 1
                for neighbor in self.neighbors:
                    # Avoid sending the message back to the peer it came from
                    if neighbor.peer_id != message.get('last_peer_id', -1):
                        lookup_message = LookupMessage(
                            req_id,
                            buyer_id,
                            product_name,
                            hopcount_new,
                            search_path.copy()
                        ).to_dict()
                        lookup_message['last_peer_id'] = self.peer_id
                        print(f"[{self.peer_id}] Forwarding lookup for {product_name} to Peer {neighbor.peer_id}")
                        self.send_message((neighbor.ip_address, neighbor.port), lookup_message)
            elif hopcount == 0:
                print(f"[{self.peer_id}] Hopcount 0 reached for request {req_id}. Discarding message.")
                    # Discard the message without sending 'no_seller' back

    def handle_reply(self, message):
        """Handle a reply recursively."""
        reply_message = message
        reply_path = reply_message["reply_path"]

        if len(reply_path) != 0:
            next_peer_info = reply_path[-1]
            addr = (next_peer_info[1], next_peer_info[2])
            reply_message["reply_path"] = reply_path[:-1]
            self.send_message(addr, reply_message)
            print(f"[{self.peer_id}] Sent reply to peer {next_peer_info[0]} for item {reply_message['product_name']} with id {reply_message['request_id']}")
        else:
            if self.role == 'buyer':
                # As a buyer, decide to buy the item
                print(f"[{self.peer_id}] Deciding to buy item {reply_message['product_name']} from seller {reply_message['seller_id']}")
                buy_message = BuyMessage(
                    reply_message["request_id"],
                    self.peer_id,
                    reply_message['seller_id'],
                    reply_message['product_name']
                ).to_dict()
                seller_addr = reply_message['seller_addr']
                self.send_message(seller_addr, buy_message)
                # Remove from pending requests
                with self.pending_requests_lock:
                    if reply_message["request_id"] in self.pending_requests:
                        del self.pending_requests[reply_message["request_id"]]
            else:
                print(f"[{self.peer_id}] Received reply but not the buyer.")

    def handle_buy(self, message, addr):
        """Handle a buy request from a buyer."""
        with self.lock:
            if self.stock > 0 and self.item == message["product_name"]:
                self.stock -= 1
                print(f"[{self.peer_id}] Sold item to buyer {message['buyer_id']}. Remaining stock: {self.stock}")
                buy_confirmation_reply = BuyConfirmationMessage(
                    message["request_id"],
                    message["product_name"],
                    message["buyer_id"],
                    message["seller_id"],
                    status=True
                ).to_dict()
                self.send_message(addr, buy_confirmation_reply)
            if self.stock == 0:
                # Seller picks another item at random
                previous_item = self.item
                self.item = random.choice(self.available_items)
                self.stock = SELLER_STOCK  # Reset stock to SELLER_STOCK
                print(f"[{self.peer_id}] Sold out of {previous_item}. Now selling {self.item}")
            else:
                buy_confirmation_reply = BuyConfirmationMessage(
                    message["request_id"],
                    message["product_name"],
                    message["buyer_id"],
                    message["seller_id"],
                    status=False
                ).to_dict()
                self.send_message(addr, buy_confirmation_reply)

    def handle_buy_confirmation(self, message):
        """Handle the buy confirmation from a seller."""
        confirmation_message = BuyConfirmationMessage.from_dict(message)

        # Check if the confirmation is for the current buyer
        if confirmation_message.buyer_id == self.peer_id:
            if confirmation_message.status:
                # Purchase was successful
                self.items_bought += 1
                timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
                print(f"{timestamp} [{self.peer_id}] bought product {confirmation_message.product_name} from seller {confirmation_message.seller_id}")

                if self.items_bought == self.max_transactions:
                    self.end_time = time.time()
                    # average_rtt =  (self.end_time - self.start_time)/self.max_transactions
                    average_rtt = (self.end_time - self.start_time)/self.max_transactions
                    print(f"[{self.peer_id}] Max transactions reached with average rtt {average_rtt:.4f}.\nShutting down peer.")
                    self.average_rtt = average_rtt
                    self.shutdown_peer()
                elif random.random() < BUY_PROBABILITY:
                    print(f"[{self.peer_id}] Buyer decided to continue looking for another item.")
                    remaining_items = [item for item in self.available_items if item != confirmation_message.product_name]
                    new_product = random.choice(remaining_items)
                    threading.Thread(target=self.lookup_item, args=(new_product, self.max_distance)).start()
                else:
                    print(f"[{self.peer_id}] Buyer is satisfied and stops buying.")
                    self.shutdown_peer()
            else:
                # Purchase failed
                print(f"[{self.peer_id}] Purchase of {confirmation_message.product_name} from seller {confirmation_message.seller_id} failed.")
                print(f"[{self.peer_id}] Buyer will search for another seller for {confirmation_message.product_name}.")
                threading.Thread(target=self.lookup_item, args=(confirmation_message.product_name, self.max_distance)).start()
            # Remove from pending requests
            with self.pending_requests_lock:
                if confirmation_message.request_id in self.pending_requests:
                    del self.pending_requests[confirmation_message.request_id]
        else:
            print(f"[{self.peer_id}] Received buy confirmation not intended for this peer.")

    def lookup_item(self, product_name=None, hopcount=3):
        """Buyers can send lookup messages to their neighbors."""
        if product_name is None:
            remaining_items = [item for item in self.available_items if item not in self.looked_up_items]
            if not remaining_items: # Incase the buyer can not find any sellers for any products [In this case would not happen]
                print(f"[{self.peer_id}] No more items to look up. Shutting down.")
                self.shutdown_peer()
                return
            product_name = random.choice(remaining_items)
            self.looked_up_items.add(product_name)
        else:
            self.looked_up_items.add(product_name)

        id_string = str(self.peer_id) + product_name + str(time.time())
        if self.role == 'buyer':
            request_id = hashlib.sha256(id_string.encode('utf-8')).hexdigest()
            lookup_message = {
                'request_id': request_id,
                'type': 'lookup',
                'buyer_id': self.peer_id,
                'product_name': product_name,
                'hop_count': hopcount,
                'search_path': [(self.peer_id, self.ip_address, self.port)],
                'last_peer_id': self.peer_id
            }

            timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
            print(f"{timestamp} [{self.peer_id}] Initiating lookup for {product_name}")
            if self.start_time is None:
                self.start_time = time.time()
            for neighbor in self.neighbors:
                print(f"[{self.peer_id}] Looking for {product_name} with neighbor {neighbor.peer_id}")
                self.send_message((neighbor.ip_address, neighbor.port), lookup_message)
            # Add to pending requests with a timestamp
            with self.pending_requests_lock:
                self.pending_requests[request_id] = (product_name, time.time())

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
