import random
import socket
import threading
import pickle
import time
import hashlib
import math
from utils.messages import *

ROOT_PEER_PORT = 6000  # The port where the root peer listens
BUY_PROBABILITY = 0.5  # Probability (50%) that a buyer will continue to buy after a successful purchase


class Neighbour:

    def __init__(self):
        pass

class Peer:
    def __init__(self, peer_id, role, neighbors,  port, ip_address = 'localhost', item=None, test = True, cache_size = math.inf):
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
        self.cache_size = cache_size


    def start_peer(self):
        """Start listening for messages from other peers."""
        print(f"Peer {self.peer_id} ({self.role}) listening on port {self.port}...")
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
        
        req_id = message['request_id']
        if req_id not in self.cache:
            if len(self.cache) > self.cache_size:
                '''evict the first item'''
                first_key, _ = next(iter(self.cache.items()))
                self.cache = self.cache[1:]

            self.cache[req_id] = message
            buyer_id = message['buyer_id']
            product_name = message['product_name']
            hopcount = message['hopcount']
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
            elif hopcount > 0:
                search_path.append(self.peer_id)
                hopcount_new = hopcount - 1
                for neighbor in self.neighbors:
                    # Avoid sending the message back to the peer it came from
                    if neighbor.peer_id not in search_path:
                        lookup_message = LookupMessage(
                                                        req_id, 
                                                        buyer_id, 
                                                        product_name, 
                                                        hopcount_new, 
                                                        search_path.copy()
                                                    ).to_dict()
                        print(f"[{self.peer_id}] Forwarding lookup for {product_name} to Peer {neighbor.peer_id}")
                        self.send_message(neighbor, lookup_message)

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
            print(f"[{self.peer_id}] Sent reply to peer {search_path[-1]} for item {reply_message["product_name"]} with id {reply_message['request_id']}")
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
                'hopcount': hopcount,
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

def main():
    num_peers = 2  # Exactly two peers: one buyer and one seller
    peers = []
    ports = [5000, 5001]  # Assign unique ports for the two peers

    # Step 1: Create one buyer and one seller
    buyer = Peer(peer_id=0, role='buyer', neighbors=[], port=ports[0])
    seller = Peer(peer_id=1, role='seller', neighbors=[], port=ports[1], item='salt')

    peers.append(buyer)
    peers.append(seller)

    # Step 2: Connect buyer and seller as neighbors
    buyer.neighbors = [seller]
    seller.neighbors = [buyer]

    # Step 3: Display the network structure
    print("Network structure initialized:")
    for peer in peers:
        peer.display_network()

    # Step 4: Start the peers to listen for messages
    for peer in peers:
        peer.start_peer()

    # Start the buyer peer to initiate a lookup for an apple
    print(f"Buyer {buyer.peer_id} is initiating a lookup for salt")
    buyer.lookup_item(product_name='salt', hopcount=1)

    print("The peer-to-peer network with one buyer and one seller has been set up successfully!")

if __name__ == '__main__':
    main()


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