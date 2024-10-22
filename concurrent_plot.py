# main.py

import random
import sys
import threading
import time
import csv
import matplotlib.pyplot as plt
from datetime import datetime

from peer import Peer
from utils.network_utils import graph_diameter


def main(N, min_neighbors, max_neighbors, num_trials, buyer_counts): # Slight modification from the main.py code

    # Initialize variables to store results
    results = []

    for buyer_count in buyer_counts:
        print(f"\n=== Testing with {buyer_count} Concurrent Buyers ===")
        trial_rtts = []

        for trial in range(1, num_trials + 1):
            print(f"\n--- Trial {trial}: {buyer_count} Buyers ---")

            peers = []
            ports = [5000 + i for i in range(N)]  # Assign unique ports for all the peers
            roles = ["buyer", "seller"]
            items = ["fish", "salt", "boar"]

            buyers = []
            sellers = []

            # Create peers with random roles and items, ensuring at least buyer_count buyers and at least one seller
            for i in range(N):
                role = 'seller'
                item = None
                if i < buyer_count:
                    role = 'buyer'
                elif i >= N - 1 and len(sellers) == 0:
                    # Ensure at least one seller exists
                    role = 'seller'

                if role == "seller":
                    item = random.choice(items)
                peer = Peer(peer_id=i, role=role, neighbors=[], item=item, port=ports[i])
                peers.append(peer)
                if role == 'buyer':
                    buyers.append(peer)
                else:
                    sellers.append(peer)

            # Connect peers randomly within min and max neighbors
            for peer in peers:
                possible_neighbors = [p for p in peers if p.peer_id != peer.peer_id and p not in peer.neighbors]
                num_connections = random.randint(min_neighbors, max_neighbors)
                num_connections = min(num_connections, len(possible_neighbors))
                neighbors = random.sample(possible_neighbors, num_connections)
                peer.neighbors.extend(neighbors)
                for neighbor in neighbors:
                    if peer not in neighbor.neighbors:
                        neighbor.neighbors.append(peer)

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

            # Initialize RTT list for this trial
            individual_rtts = []

            # Have every buyer initiate a lookup
            if buyers:
                threads = []  # List to hold thread references
                for buyer in buyers:
                    item = random.choice(items)
                    print(f"Buyer {buyer.peer_id} is initiating a lookup for {item} with hopcount {hopcount}")

                    # Start the thread and pass the buyer's ID to the target function to avoid overwriting
                    thread = threading.Thread(target=buyer.lookup_item, args=(item, hopcount))
                    threads.append(thread)  # Store the thread reference
                    thread.start()  # Start the thread

                # Join all threads to ensure they initiate their lookups
                for thread in threads:
                    thread.join()

                # Wait until all buyers have completed their transactions
                while True:
                    alive_buyers = [buyer for buyer in buyers if buyer.running]
                    if not alive_buyers:
                        print("All buyers have shut down. Shutting down sellers and collecting RTT data.")
                        # Shut down all seller peers
                        for seller in sellers:
                            seller.running = False
                            try:
                                seller.socket.close()
                            except Exception as e:
                                print(f"[{seller.peer_id}] Error closing socket: {e}")
                        break
                    time.sleep(1)  # Sleep before checking again

                # Collect the average RTTs for each buyer after all transactions are complete
                for buyer in buyers:
                    trial_rtts.append(buyer.average_rtt)
                    print(f"Buyer {buyer.peer_id} average RTT: {buyer.average_rtt:.4f} seconds")

                # Reset the average RTT for the next trial
                for buyer in buyers:
                    buyer.average_rtt = 0.0

            # Compute the overall average RTT for this trial
            average_rtt_trial = sum(trial_rtts) / len(trial_rtts) if trial_rtts else 0
            results.append({
                'buyer_count': buyer_count,
                'trial': trial,
                'average_rtt': average_rtt_trial
            })

            # Wait before starting the next trial to ensure clean setup
            time.sleep(2)

            # Wait for all peer threads to finish
            for peer in peers:
                if peer.thread.is_alive():
                    peer.thread.join()

        # After all trials for this buyer_count, continue to the next buyer_count

    # After all buyer_counts and trials, save results and plot
    # Save to CSV
    with open('rtt_results.csv', 'w', newline='') as csvfile:
        fieldnames = ['buyer_count', 'trial', 'average_rtt']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in results:
            writer.writerow(result)

    print("\nRTT Results Saved to rtt_results.csv")

    # Process results to compute average RTT per buyer_count
    average_rtt_per_buyer = {}
    for result in results:
        bc = result['buyer_count']
        ar = result['average_rtt']
        if bc not in average_rtt_per_buyer:
            average_rtt_per_buyer[bc] = []
        average_rtt_per_buyer[bc].append(ar)

    # Compute the mean RTT for each buyer_count
    buyer_counts_sorted = sorted(average_rtt_per_buyer.keys())
    mean_rtts = [sum(average_rtt_per_buyer[bc]) / len(average_rtt_per_buyer[bc]) for bc in buyer_counts_sorted]

    # Plotting Average RTT vs Number of Buyers
    plt.figure(figsize=(10, 6))
    plt.plot(buyer_counts_sorted, mean_rtts, marker='o', linestyle='-', color='b')
    plt.title('Average RTT per Request vs. Number of Concurrent Buyers')
    plt.xlabel('Number of Concurrent Buyers')
    plt.ylabel('Average RTT (seconds)')
    plt.xticks(buyer_counts_sorted)
    plt.grid(True)
    plt.savefig('average_rtt_vs_buyers.png')
    plt.show()

    print("Average RTT vs. Number of Buyers Plot Saved as average_rtt_vs_buyers.png")


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("Usage: python concurrent_plot.py <number_of_peers> <min_neighbors> <max_neighbors> <num_trials> <buyer_counts>")
        sys.exit(1)
    N = int(sys.argv[1])
    min_neighbors = int(sys.argv[2])
    max_neighbors = int(sys.argv[3])
    num_trials = int(sys.argv[4])
    buyer_counts = list(map(int, sys.argv[5].split(',')))
    main(N, min_neighbors, max_neighbors, num_trials, buyer_counts)

# Example command to run:
# python concurrent_plot.py 6 1 3 1 "1,2,3,4,5"
