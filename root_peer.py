import socket
import threading
import pickle

ROOT_PEER_PORT = 6000
ITEMS_TO_BE_SOLD = 5  # Define the threshold of total items sold before shutdown

class RootPeer:
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('localhost', ROOT_PEER_PORT))
        self.total_items_sold = 0
        self.peers = set()  # Track all peers to send shutdown messages

    def start_root_peer(self):
        print(f"Root Peer listening on port {ROOT_PEER_PORT}...")
        threading.Thread(target=self.listen_for_updates).start()

    def listen_for_updates(self):
        """Listen for status updates from peers."""
        while True:
            data, addr = self.socket.recvfrom(1024)
            message = pickle.loads(data)
            if message['status'] == 'update':
                self.peers.add(addr)  # Track peer address
                print(f"Root Peer received status update from Peer {message['peer_id']}")
                if message['role'] == 'seller' and message['stock'] == 0:
                    self.total_items_sold += 1
                    print(f"Root Peer: Item sold by Peer {message['peer_id']}. Total items sold: {self.total_items_sold}")
                    if self.total_items_sold >= ITEMS_TO_BE_SOLD:
                        self.shutdown_network()

    def shutdown_network(self):
        """Send shutdown message to all peers and close the network."""
        print("Root Peer: Threshold reached. Shutting down the network...")
        shutdown_message = pickle.dumps({'type': 'shutdown'})
        for peer in self.peers:
            self.socket.sendto(shutdown_message, peer)
        self.socket.close()

if __name__ == '__main__':
    root_peer = RootPeer()
    root_peer.start_root_peer()
