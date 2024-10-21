import random
import socket
import sys
import threading
import pickle
import time
import hashlib
import math
from collections import deque

from utils.messages import *

ROOT_PEER_PORT = 6000  # The port where the root peer listens
BUY_PROBABILITY = 0.5  # Probability (50%) that a buyer will continue to buy after a successful purchase


class Neighbour:

    def __init__(self):
        pass

class Peer:
    def __init__(self, peer_id, role, neighbors,  port, ip_address = 'localhost', item=None, test = True, cache_size = math.inf, hop_count=3, max_distance=3):
        self.peer_id = peer_id
        self.role = role  # buyer or seller
        self.neighbors = neighbors
        self.ip_address = ip_address
        self.port = port
        self.item = item
        self.stock = random.randint(1, 5) if role == 'seller' else 0
        self.lock = threading.Lock()  # For thread safety
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.ip_address, port))
        # self.root_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # For sending updates to root
        self.running = True
        self.test = test
        self.cache = {}
        self.looked_up_items = set()
        self.max_distance = max_distance
        self.hop_count = hop_count
        self.available_items = ["fish","salt","boar"]
        self.cache_size = cache_size


    def start_peer(self):
        """Start listening for messages from other peers."""
        print(f"Peer {self.peer_id} ({self.role})  with item {self.item} listening on port {self.port}...")
        threading.Thread(target=self.listen_for_messages).start()
        # self.send_status_to_root()

    def listen_for_messages(self):
        """Continuously listen for incoming messages."""
        while self.running:
            data, addr = self.socket.recvfrom(1024)
            message = pickle.loads(data)
            print("Rcv Msg", message)
            # if message.get('type') == 'shutdown':
            #     pass
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

    def send_message(self, neighbor, message):
        """Send a message to a neighbor."""
        data = pickle.dumps(message)
        self.socket.sendto(data, (neighbor.ip_address, neighbor.port))

    # def send_status_to_root(self):
    #     """Send current status to the root peer."""
    #     status_message = {
    #         "peer_id": self.peer_id,
    #         "role": self.role,
    #         "neighbors": [neighbor.peer_id for neighbor in self.neighbors],
    #         "item": self.item,
    #         "stock": self.stock if self.role == 'seller' else None,
    #         "status": "update"
    #     }
    #     self.root_socket.sendto(pickle.dumps(status_message), (self.ip_address, ROOT_PEER_PORT))
    #     print(f"[{self.peer_id}] Sent status update to Root Peer")

    def handle_lookup(self, message, addr):
        """Handle a lookup request from a buyer or peer.
        Checks if request id is in cache, if yes, does nothing (we assume that the forwarding has been served)
        if not, logic below
        """
        import random
        import socket
        import sys
        import threading
        import pickle
        import time
        import hashlib
        import math
        from collections import deque

        # Message classes
        class LookupMessage:
            def __init__(self, request_id, buyer_id, product_name, hop_count, search_path):
                self.type = 'lookup'
                self.request_id = request_id
                self.buyer_id = buyer_id
                self.product_name = product_name
                self.hop_count = hop_count
                self.search_path = search_path

            def to_dict(self):
                return self.__dict__

        class ReplyMessage:
            def __init__(self, seller_id, reply_path, seller_addr, product_name, request_id):
                self.type = 'reply'
                self.seller_id = seller_id
                self.reply_path = reply_path
                self.seller_addr = seller_addr
                self.product_name = product_name
                self.request_id = request_id

            def to_dict(self):
                return self.__dict__

            @staticmethod
            def from_dict(d):
                return ReplyMessage(d['seller_id'], d['reply_path'], d['seller_addr'], d['product_name'],
                                    d['request_id'])

        class BuyMessage:
            def __init__(self, request_id, buyer_id, seller_id, product_name):
                self.type = 'buy'
                self.request_id = request_id
                self.buyer_id = buyer_id
                self.seller_id = seller_id
                self.product_name = product_name

            def to_dict(self):
                return self.__dict__

        class BuyConfirmationMessage:
            def __init__(self, request_id, product_name, buyer_id, seller_id, status):
                self.type = 'buy_confirmation'
                self.request_id = request_id
                self.product_name = product_name
                self.buyer_id = buyer_id
                self.seller_id = seller_id
                self.status = status

            def to_dict(self):
                return self.__dict__

            @staticmethod
            def from_dict(d):
                return BuyConfirmationMessage(d['request_id'], d['product_name'], d['buyer_id'], d['seller_id'],
                                              d['status'])

        ROOT_PEER_PORT = 6000  # The port where the root peer listens
        BUY_PROBABILITY = 1  # Probability (50%) that a buyer will continue to buy after a successful purchase

        class Peer:
            def __init__(self, peer_id, role, neighbors, port, ip_address='localhost', item=None, test=True,
                         cache_size=math.inf, hop_count=3, max_distance=3):
                self.peer_id = peer_id
                self.role = role  # buyer or seller
                self.neighbors = neighbors
                self.ip_address = ip_address
                self.port = port
                self.item = item
                self.stock = random.randint(1, 5) if role == 'seller' else 0
                self.lock = threading.Lock()  # For thread safety
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.socket.bind((self.ip_address, port))
                self.running = True
                self.test = test
                self.cache = {}
                self.looked_up_items = set()
                self.hop_count = hop_count
                self.max_distance = max_distance
                self.available_items = ["fish", "salt", "boar"]
                self.cache_size = cache_size

                # For buyer timeout handling
                self.pending_requests = {}
                self.timeout = 5  # seconds

            def start_peer(self):
                """Start listening for messages from other peers."""
                print(f"Peer {self.peer_id} ({self.role}) with item {self.item} listening on port {self.port}...")
                threading.Thread(target=self.listen_for_messages).start()

            def listen_for_messages(self):
                """Continuously listen for incoming messages."""
                while self.running:
                    try:
                        self.socket.settimeout(1.0)
                        data, addr = self.socket.recvfrom(1024)
                        message = pickle.loads(data)
                        print(f"[{self.peer_id}] Received Message: {message}")
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
                        pass  # Timeout occurred, can be used to check pending requests

                    # For buyers, check for timeouts on pending requests
                    if self.role == 'buyer':
                        self.check_pending_requests()

            def check_pending_requests(self):
                current_time = time.time()
                to_remove = []
                for request_id, (product_name, timestamp) in self.pending_requests.items():
                    if current_time - timestamp > self.timeout:
                        print(
                            f"[{self.peer_id}] No response received for {product_name}. Timing out and selecting another item.")
                        self.looked_up_items.add(product_name)
                        remaining_items = [item for item in self.available_items if item not in self.looked_up_items]
                        if not remaining_items:
                            print(f"[{self.peer_id}] No more items to look up. Shutting down.")
                            self.shutdown_peer()
                            return
                        new_product = random.choice(remaining_items)
                        print(f"[{self.peer_id}] Searching for a new product: {new_product}")
                        threading.Thread(target=self.lookup_item, args=(new_product, self.max_distance)).start()
                        to_remove.append(request_id)
                for request_id in to_remove:
                    del self.pending_requests[request_id]

            def send_message(self, addr, message):
                """Send a message to a specific address."""
                data = pickle.dumps(message)
                self.socket.sendto(data, addr)

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
                        print(
                            f"[{self.peer_id}] Sent reply for {req_id} to peer {next_peer_info[0]} for item {product_name}")

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
                                print(
                                    f"[{self.peer_id}] Forwarding lookup for {product_name} to Peer {neighbor.peer_id}")
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
                    print(
                        f"[{self.peer_id}] Sent reply to peer {next_peer_info[0]} for item {reply_message['product_name']} with id {reply_message['request_id']}")
                else:
                    if self.role == 'buyer':
                        # As a buyer, decide to buy the item
                        print(
                            f"[{self.peer_id}] Deciding to buy item {reply_message['product_name']} from seller {reply_message['seller_id']}")
                        buy_message = BuyMessage(
                            reply_message["request_id"],
                            self.peer_id,
                            reply_message['seller_id'],
                            reply_message['product_name']
                        ).to_dict()
                        seller_addr = reply_message['seller_addr']
                        self.send_message(seller_addr, buy_message)
                        # Remove from pending requests
                        if reply_message["request_id"] in self.pending_requests:
                            del self.pending_requests[reply_message["request_id"]]
                    else:
                        print(f"[{self.peer_id}] Received reply but not the buyer.")

            def handle_buy(self, message, addr):
                """Handle a buy request from a buyer."""
                with self.lock:
                    if self.stock > 0 and self.item == message["product_name"]:
                        self.stock -= 1
                        print(
                            f"[{self.peer_id}] Sold item to buyer {message['buyer_id']}. Remaining stock: {self.stock}")
                        buy_confirmation_reply = BuyConfirmationMessage(
                            message["request_id"],
                            message["product_name"],
                            message["buyer_id"],
                            message["seller_id"],
                            status=True
                        ).to_dict()
                        self.send_message(addr, buy_confirmation_reply)
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
                        print(
                            f"[{self.peer_id}] Purchase of {confirmation_message.product_name} from seller {confirmation_message.seller_id} was successful.")

                        # Decide whether to continue buying based on probability
                        if random.random() < BUY_PROBABILITY:
                            print(f"[{self.peer_id}] Buyer decided to continue looking for another item.")
                            threading.Thread(target=self.lookup_item, args=(None, self.max_distance)).start()
                        else:
                            print(f"[{self.peer_id}] Buyer is satisfied and stops buying.")
                            self.shutdown_peer()
                    else:
                        # Purchase failed
                        print(
                            f"[{self.peer_id}] Purchase of {confirmation_message.product_name} from seller {confirmation_message.seller_id} failed.")
                        print(
                            f"[{self.peer_id}] Buyer will search for another seller for {confirmation_message.product_name}.")
                        threading.Thread(target=self.lookup_item,
                                         args=(confirmation_message.product_name, self.max_distance)).start()
                else:
                    print(f"[{self.peer_id}] Received buy confirmation not intended for this peer.")

            def lookup_item(self, product_name=None, hopcount=3):
                """Buyers can send lookup messages to their neighbors."""
                if product_name is None:
                    remaining_items = [item for item in self.available_items if item not in self.looked_up_items]
                    if not remaining_items:
                        print(f"[{self.peer_id}] No more items to look up. Shutting down.")
                        self.shutdown_peer()
                        return
                    product_name = random.choice(remaining_items)
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
                    print(f"[{self.peer_id}] Initiating lookup for {product_name}")
                    for neighbor in self.neighbors:
                        print(f"[{self.peer_id}] Looking for {product_name} with neighbor {neighbor.peer_id}")
                        self.send_message((neighbor.ip_address, neighbor.port), lookup_message)
                    # Add to pending requests with a timestamp
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
                exit(0)  # Exits the program

            def handle_no_seller(self, message):
                """Handle the 'no_seller' message, which should not occur since we discard messages at hopcount zero."""
                print(f"[{self.peer_id}] Received unexpected 'no_seller' message.")

        def bfs_paths(start_peer, peers):
            distances = {peer.peer_id: float('inf') for peer in peers}
            distances[start_peer.peer_id] = 0
            queue = deque([start_peer])
            while queue:
                peer = queue.popleft()
                for neighbor in peer.neighbors:
                    if distances[neighbor.peer_id] == float('inf'):
                        distances[neighbor.peer_id] = distances[peer.peer_id] + 1
                        queue.append(neighbor)
            return distances

        def graph_diameter(peers):
            max_distance = 0
            for peer in peers:
                distances = bfs_paths(peer, peers)
                furthest_peer = max(distances.values())
                max_distance = max(max_distance, furthest_peer)
            return max_distance

        def main(N):
            num_peers = N  # Number of peers in the network
            peers = []
            ports = [5000 + i for i in range(num_peers)]  # Assign unique ports for all the peers
            roles = ["buyer", "seller"]
            items = ["fish", "salt", "boar"]

            buyers = []
            sellers = []

            # Create peers with random roles and items, ensuring at least one buyer
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

            # Connect peers randomly into a network with no more than 3 neighbors each
            for i in range(num_peers):
                next_peer = (i + 1) % num_peers  # Connect to the next peer in a circular fashion
                if peers[next_peer] not in peers[i].neighbors:
                    peers[i].neighbors.append(peers[next_peer])
                    peers[next_peer].neighbors.append(peers[i])

            # Add random neighbors to ensure no more than 3 neighbors per peer
            max_neighbors = min(3, num_peers - 1)
            for peer in peers:
                retries = 0
                while len(peer.neighbors) < max_neighbors and retries < 10:
                    neighbor = random.choice(peers)
                    if neighbor.peer_id != peer.peer_id and neighbor not in peer.neighbors and len(
                            neighbor.neighbors) < max_neighbors:
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
                # for buyer in buyers:
                buyer = random.choice(buyers)
                item = random.choice(items)
                print(f"Buyer {buyer.peer_id} is initiating a lookup for {item} with hopcount {hopcount}")
                threading.Thread(target=buyer.lookup_item, args=(item, hopcount)).start()

            print("The peer-to-peer network has been set up successfully!")

        if __name__ == '__main__':
            if len(sys.argv) != 2:
                print("Need to give the correct commands")
                sys.exit(1)
            N = int(sys.argv[1])
            # if N<2:
            #     print("Need atleast 2 peers")
            #     sys.exit(1)
            main(N)

        req_id = message['request_id']
        if req_id not in self.cache:
            if len(self.cache) > self.cache_size:
                '''evict the first item'''
                first_key, _ = next(iter(self.cache.items()))
                self.cache = self.cache[1:]

            self.cache[req_id] = message
            buyer_id = message['buyer_id']
            product_name = message['product_name']
            # hopcount = message['hop_count']
            print(f"Message hopcount {self.hop_count}")
            search_path = message['search_path']

            # If this peer is a seller and has the requested product, reply to the buyer
            if self.role == 'seller' and self.item == product_name and self.stock > 0:
                addr = self.get_neighbor_info(search_path[-1])
                reply_message = ReplyMessage(
                                            self.peer_id, 
                                            reply_path= search_path[:-1],
                                            seller_addr=(self.ip_address, self.port),
                                            product_name=product_name,
                                            request_id=req_id).to_dict()
                
                self.socket.sendto(pickle.dumps(reply_message), addr)
                print(f"[{self.peer_id}] Sent reply for {req_id} to peer {addr[1]} for item {product_name}")

                # If hopcount > 0, propagate the lookup to neighbors
            elif self.hop_count > 0:
                search_path.append(self.peer_id)
                # hopcount_new = hopcount - 1
                self.hop_count-=1
                for neighbor in self.neighbors:
                    # Avoid sending the message back to the peer it came from
                    if neighbor.peer_id not in search_path:
                        lookup_message = LookupMessage(
                                                        req_id, 
                                                        buyer_id, 
                                                        product_name, 
                                                        # hopcount_new,
                                                        self.hop_count,
                                                        search_path.copy()
                                                    ).to_dict()
                        print(f"[{self.peer_id}] Forwarding lookup for {product_name} to Peer {neighbor.peer_id}")
                        self.send_message(neighbor, lookup_message)
            elif self.hop_count==0:
                print(
                    f"[{self.peer_id}] Hopcount 0 reached. No sellers found for {product_name}. Informing buyer {buyer_id}.")
                #buyer_addr = self.get_neighbor_info(buyer_id)
                buyer_addr = self.ip_address, self.port
                no_seller_message = {
                    'type': 'no_seller',
                    'buyer_id': buyer_id,
                    'product_name': product_name,
                    'request_id': req_id,
                    'message': "No sellers found for the requested product. Please select another item."
                }
                self.socket.sendto(pickle.dumps(no_seller_message), buyer_addr)

    def get_neighbor_info(self, neighbor_id):
        """Return the IP address and port of the specified neighbor ID."""
        for neighbor in self.neighbors:
            if neighbor.peer_id == neighbor_id:
                return neighbor.ip_address, neighbor.port
        return None, None  # Return None if neighbor ID not found

    def handle_reply(self, message):
        """Handle a reply recusively."""
        reply_message = ReplyMessage.from_dict(message).to_dict()
        search_path = reply_message["reply_path"]
        
        if len(search_path) != 0 or search_path == 0:
            addr = self.get_neighbor_info(search_path[-1])
            reply_message["reply_path"] = search_path[:-1] 
            self.socket.sendto(pickle.dumps(reply_message), addr)
            print(f"[{self.peer_id}] Sent reply to peer {search_path[-1]} for item {reply_message['product_name']} with id {reply_message['request_id']}")
        else:
            if self.role == 'buyer':
                # As a buyer, decide to buy the item (you can add logic here to choose if you'd like)
                    print(f"[{self.peer_id}] Deciding to buy item {reply_message['product_name']} from seller {reply_message['seller_id']}")
                    buy_message = BuyMessage(reply_message["request_id"],self.peer_id, reply_message['seller_id'], reply_message['product_name']).to_dict()
                    self.socket.sendto(pickle.dumps(buy_message), reply_message['seller_addr'])

    def handle_buy(self, message, addr):
        """Handle a buy request from a buyer."""
        with self.lock:
            if self.stock > 0 and self.item == message["product_name"]:
                self.stock -= 1
                print(f"[{self.peer_id}] Sold item to buyer {message['buyer_id']}. Remaining stock: {self.stock}")
                # self.send_status_to_root()
                buy_confirmation_reply = BuyConfirmationMessage(message["request_id"], message["product_name"], message["buyer_id"], message["seller_id"], status = True).to_dict()
                self.socket.sendto(pickle.dumps(buy_confirmation_reply), addr)
            if self.stock == 0 and self.item != message["product_name"]:
                buy_confirmation_reply = BuyConfirmationMessage(message["request_id"], message["product_name"], message["buyer_id"], message["seller_id"], status = False).to_dict()
                self.socket.sendto(pickle.dumps(buy_confirmation_reply), addr)
                
    def handle_buy_confirmation(self, message):
        """Handle the buy confirmation from a seller.

        Args:
            message (dict): The confirmation message from the seller.
        """
        confirmation_message = BuyConfirmationMessage.from_dict(message)
        
        # Check if the confirmation is for the current buyer
        if confirmation_message.buyer_id == self.peer_id:
            if confirmation_message.status:
                # Purchase was successful
                print(f"[{self.peer_id}] Purchase of {confirmation_message.product_name} from seller {confirmation_message.seller_id} was successful.")
                
                # Decide whether to continue buying based on probability
                if random.random() < BUY_PROBABILITY:
                    print(f"[{self.peer_id}] Buyer decided to continue looking for another item.")
                    self.lookup_item()  # Buyer continues to search for a new product
                else:
                    print(f"[{self.peer_id}] Buyer is satisfied and stops buying.")
            else:
                # Purchase failed
                print(f"[{self.peer_id}] Purchase of {confirmation_message.product_name} from seller {confirmation_message.seller_id} failed.")
                print(f"[{self.peer_id}] Buyer will search for another seller for {confirmation_message.product_name}.")
                
                # Initiate a new lookup for the same product
                self.lookup_item()


    def lookup_item(self, product_name=None, hopcount=3):
        """Buyers can send lookup messages to their neighbors."""
        if product_name == None:
            product_name = random.choice(['fish', 'salt', 'boars'])

        id_string = str(self.peer_id) + product_name + str(time.time())
        if self.role == 'buyer':
            request_id = hashlib.sha256(id_string.encode('utf-8')).hexdigest()
            lookup_message = {
                'request_id': request_id,
                'type': 'lookup',
                'buyer_id': self.peer_id,
                'product_name': product_name,
                'hop_count': hopcount,
                'search_path': [self.peer_id]
            }
            print(lookup_message)
            for neighbor in self.neighbors:
                print(f"[{self.peer_id}] Looking for {product_name} with neighbor {neighbor.peer_id}")
                self.send_message(neighbor, lookup_message)


    def display_network(self):
        """Print network structure for this peer."""
        neighbor_ids = [neighbor.peer_id for neighbor in self.neighbors]
        print(f"Peer {self.peer_id} connected to peers {neighbor_ids}")

    def shutdown_peer(self):
        """Shutdown the peer."""
        print(f"[{self.peer_id}] Shutting down peer.")
        self.socket.close()

    def handle_no_seller(self, message):
        print(f"[{self.peer_id}] No seller found for {message['product_name']}. Selecting another item...")
        self.looked_up_items.add(message['product_name'])
        print(f"Length of gone through items {len(self.looked_up_items)}")
        # Check if all products have been looked up
        if len(self.looked_up_items) == len(self.available_items):
            print(f"[{self.peer_id}] All items have been looked up. No sellers found. Shutting down...")
            self.running =False
            self.shutdown_peer()  # Call the method to shutdown the peer
            return
        remaining_items = [item for item in self.available_items if item not in self.looked_up_items]
        new_product = random.choice(remaining_items)
        print(f"[{self.peer_id}] Searching for a new product: {new_product}")

        self.lookup_item(new_product,self.max_distance)


def bfs_paths(start_peer, peers):
    distances = {peer.peer_id: float('inf') for peer in peers}
    distances[start_peer.peer_id]=0
    queue = deque([start_peer])
    while queue:
        peer = queue.popleft()
        for neighbor in peer.neighbors:
            if distances[neighbor.peer_id] == float('inf'):
                distances[neighbor.peer_id] = distances[peer.peer_id]+1
                queue.append(neighbor)
    return distances


def graph_diameter(peers):
    max_distance = 0
    for peer in peers:
        distances = bfs_paths(peer,peers)
        furthest_peer = max(distances.values())
        max_distance = max(max_distance,furthest_peer)
    return max_distance


def main(N):
    num_peers = N
    peers = []
    # ports = [5000, 5001]  # Assign unique ports for the two peers
    ports = [5000+i for i in range(num_peers)] # Assign unique ports for all the peers
    roles = ["buyer","seller"]
    items = ["fish","salt","boar"]

    # Step 1: Create one buyer and one seller
    # buyer = Peer(peer_id=0, role='buyer', neighbors=[], port=ports[0]) # Each peer having a random role
    # seller = Peer(peer_id=1, role='seller', neighbors=[], port=ports[1], item='salt')
    # every peer should be either a buyer or a seller and create a network in which a peer has atmost 3 neighbours

    for i in range(num_peers):
        role = random.choice(roles)
        item = None
        if role =="seller":
            item = random.choice(items)
        peer = Peer(peer_id=i, role=role, neighbors=[], item= item, port=ports[i])
        peers.append(peer)


    # peers.append(buyer)
    # peers.append(seller)

    # Step 2: Connect buyer and seller as neighbors # Change this network
    # buyer.neighbors = [seller]
    # seller.neighbors = [buyer]

    for i in range(num_peers):
        next_peer = (i + 1) % num_peers  # Connect to the next peer in a circular fashion
        # neighbor_ids = [neighbor.peer_id for neighbor in peers[i].neighbors]
        # print(neighbor_ids)
        if peers[next_peer] not in peers[i].neighbors:
            peers[i].neighbors.append(peers[next_peer])
            peers[next_peer].neighbors.append(peers[i])

    # Step 3: Add random neighbors to ensure no more than 3 neighbors per peer
    max_neighbors = min(3, num_peers - 1)
    for peer in peers:
        retries=0
        while len(peer.neighbors) < max_neighbors and retries<10:
            neighbor = random.choice(peers)
            if neighbor.peer_id != peer.peer_id and neighbor not in peer.neighbors and len(neighbor.neighbors) < max_neighbors:
                peer.neighbors.append(neighbor)
                neighbor.neighbors.append(peer)
            else:
                retries+=1

    # Step 3: Display the network structure
    print("Network structure initialized:")
    for peer in peers:
        peer.display_network()

    # Step 4: Start the peers to listen for messages

    buyers = [peer for peer in peers if peer.role == 'buyer']
    diameter = graph_diameter(peers)
    for peer in peers:
        peer.max_distance = diameter
        peer.hop_count = diameter

    for peer in peers:
        peer.start_peer()

    if buyers:
        buyer = random.choice(buyers)
        item = random.choice(items)
        # print(f"Buyer {random_buyer.peer_id} is initiating a lookup for {item}")
        # max_search_distance = max(1,diameter)
        item = random.choice(items)
        print(f"Buyer {buyer.peer_id} is initiating a lookup for {item}")
        print(f"The max hopcount is {buyer.max_distance}")
        threading.Thread(target=buyer.lookup_item, args=(item, buyer.max_distance)).start()

        # random_buyer.lookup_item(product_name=item, hopcount=max_search_distance)

    # print("The peer-to-peer network with one buyer and one seller has been set up successfully!")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Need to give the correct commands")
        sys.exit(1)
    N = int(sys.argv[1])
    # if N<2:
    #     print("Need atleast 2 peers")
    #     sys.exit(1)
    main(N)


# def main():
#     num_peers = 6  # Number of peers in the network
#     peers = []
#     ports = [5000 + i for i in range(num_peers)]  # Assign each peer a unique port

#     # Step 1: Create peer objects with random roles and assign random neighbors
#     for i in range(num_peers):
#         role = random.choice(['buyer', 'seller'])
#         item = random.choice(['apple', 'book', 'laptop']) if role == 'seller' else None
#         peer = Peer(peer_id=i, role=role, neighbors=[], port=ports[i], item=item)
#         peers.append(peer)

#     # Step 2: Connect peers randomly into a network with no more than 3 neighbors each
#     for i, peer in enumerate(peers):
#         available_peers = [p for p in peers if p != peer and len(p.neighbors) < 3]
#         neighbors = random.sample(available_peers, min(3, len(available_peers)))
#         peer.neighbors = neighbors
#         for neighbor in neighbors:
#             if peer not in neighbor.neighbors:  # Ensure bidirectional connection
#                 neighbor.neighbors.append(peer)

#     # Step 3: Display the network structure
#     print("Network structure initialized:")
#     for peer in peers:
#         peer.display_network()

#     # Step 4: Start the peers to listen for messages
#     for peer in peers:
#         peer.start_peer()

#     # Start a buyer peer to initiate a lookup
#     buyer_peer = random.choice([p for p in peers if p.role == 'buyer'])
#     print(f"Buyer {buyer_peer.peer_id} is initiating a lookup for an apple")
#     buyer_peer.lookup_item(product_name='apple', hopcount=3)

#     print("The peer-to-peer network has been set up successfully!")

# if __name__ == '__main__':
   
#     main()