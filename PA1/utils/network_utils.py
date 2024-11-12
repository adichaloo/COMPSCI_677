# network_utils.py

from collections import deque

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
