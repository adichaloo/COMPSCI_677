import multiprocessing
import time
from database_server import DatabaseServer
from trader import Trader  # Ensure trader.py has symmetrical heartbeat logic
from seller import Seller
from buyer import Buyer
from multiprocessing import Manager
import math
import threading

N_B = 10
N_T = 2
N_S = 8
N_G = 10
T_G = 5

BUY_PROBABILITY = 1
MAX_TRANSACTIONS = math.inf

def run_database(db_host, db_port, shipped_goods):
    db_server = DatabaseServer(host=db_host, port=db_port, shipped_goods=shipped_goods)
    db_server.run()

def run_trader(db_host, db_port, trader_host, trader_port, trader_id, backup_host, backup_port, all_clients):
    trader = Trader(
        host=trader_host,
        port=trader_port,
        db_host=db_host,
        db_port=db_port,
        trader_id=trader_id,
        all_clients=all_clients
    )
    trader.run(backup_host, backup_port)

def run_seller(trader, seller_id, ng, tg, port):
    seller = Seller(traders=[trader], seller_id=seller_id, ng=ng, tg=tg, port=port)
    seller.run()

def run_buyer(trader, buyer_id, max_transactions, buy_probability, port):
    buyer = Buyer(traders=[trader], buyer_id=buyer_id, max_transactions=max_transactions, p=buy_probability, port = port)
    buyer.run()

class TradingPostNetwork:
    def __init__(self, num_buyers=6, num_sellers=6, num_traders=2, db_host='localhost', db_port=5000):
        self.num_buyers = num_buyers
        self.num_sellers = num_sellers
        self.num_traders = num_traders
        self.db_host = db_host
        self.db_port = db_port
        self.trader_start_port = db_port + 1
        self.seller_start_port = 6000
        self.buyer_start_port = 7000
        self.trading_posts = []

    def start_database_server(self):
        manager = Manager()
        shipped_goods = manager.Value("i", 0)
        db_process = multiprocessing.Process(
            target=run_database,
            args=(self.db_host, self.db_port, shipped_goods),
            daemon=True
        )
        db_process.start()
        print(f"Database server started on {self.db_host}:{self.db_port}")
        return db_process, shipped_goods

    def distribute_entities(self, total_entities, num_posts):
        base_count = total_entities // num_posts
        remainder = total_entities % num_posts
        return [base_count + (1 if i < remainder else 0) for i in range(num_posts)]

    def start_traders(self):
        """Start traders for each trading post."""
        trader_processes = []
        trader_ports = [5002, 5003]
        all_clients = []

        for i in range(self.num_sellers):
            port = self.seller_start_port + i + 1
            all_clients.append(('localhost', port))

        # Distribute buyers
        for i in range(self.num_buyers):
            port = self.buyer_start_port + i + 1
            all_clients.append(('localhost', port))

        # Trader 1 sends heartbeats to Trader 2
        trader1_port = trader_ports[0]
        trader2_port = trader_ports[1]

        self.trading_posts.append({"trader": (self.db_host, trader1_port), "buyers": [], "sellers": []})
        trader1_process = multiprocessing.Process(
            target=run_trader,
            args=(self.db_host, self.db_port, self.db_host, trader1_port, 1, "localhost", trader2_port, all_clients),
            daemon=True
        )
        trader1_process.start()
        trader_processes.append(trader1_process)
        print(f"Trader 1 started on {self.db_host}:{trader1_port} (heartbeat to port {trader2_port})")
        time.sleep(0.5)

        # Trader 2 also sends heartbeats to Trader 1
        self.trading_posts.append({"trader": (self.db_host, trader2_port), "buyers": [], "sellers": []})
        trader2_process = multiprocessing.Process(
            target=run_trader,
            args=(self.db_host, self.db_port, self.db_host, trader2_port, 2, "localhost", trader1_port, all_clients),
            daemon=True
        )
        trader2_process.start()
        trader_processes.append(trader2_process)
        print(f"Trader 2 started on {self.db_host}:{trader2_port} (heartbeat to port {trader1_port})")
        time.sleep(0.5)

        return trader_processes

    def start_sellers(self):
        seller_processes = []
        distribution = self.distribute_entities(self.num_sellers, self.num_traders)
        seller_id = 1

        for i, post in enumerate(self.trading_posts):
            for _ in range(distribution[i]):
                seller_port = self.seller_start_port + seller_id
                seller_process = multiprocessing.Process(
                    target=run_seller,
                    args=(post["trader"], seller_id, N_G, T_G, seller_port),
                    daemon=True
                )
                seller_process.start()
                seller_processes.append(seller_process)
                post["sellers"].append(seller_id)
                print(f"Seller {seller_id} started in Trading Post {i + 1} with listener on port {seller_port}.")
                seller_id += 1
                time.sleep(0.5)
        return seller_processes

    def start_buyers(self):
        buyer_processes = []
        distribution = self.distribute_entities(self.num_buyers, self.num_traders)
        buyer_id = 1

        for i, post in enumerate(self.trading_posts):
            for _ in range(distribution[i]):
                buyer_port = self.buyer_start_port + buyer_id
                buyer_process = multiprocessing.Process(
                    target=run_buyer,
                    args=(post["trader"], buyer_id, MAX_TRANSACTIONS, BUY_PROBABILITY, buyer_port),
                    daemon=True
                )
                buyer_process.start()
                buyer_processes.append(buyer_process)
                post["buyers"].append(buyer_id)
                print(f"Buyer {buyer_id} started in Trading Post {i + 1}.")
                buyer_id += 1
                time.sleep(0.5)
        return buyer_processes

    def setup_network(self):
        print("Starting trading post network setup...")
        db_process, shipped_goods = self.start_database_server()
        time.sleep(1)

        trader_processes = self.start_traders()
        time.sleep(1)
        seller_processes = self.start_sellers()
        time.sleep(1)
        buyer_processes = self.start_buyers()
        time.sleep(1)

        print("Trading post network setup complete. All components are running.")
        for i, post in enumerate(self.trading_posts):
            print(f"Trading Post {i + 1}: Trader={post['trader']}, Buyers={post['buyers']}, Sellers={post['sellers']}")

        return db_process, trader_processes, seller_processes, buyer_processes, shipped_goods


if __name__ == "__main__":
    import socket
    import subprocess
    import os
    import signal

    def clean_port(port):
        """Kill any process using the specified port (Unix systems)."""
        try:
            result = subprocess.run(["lsof", "-t", f"-i:{port}"], capture_output=True, text=True)
            if result.stdout.strip():
                pid = int(result.stdout.strip())
                os.kill(pid, signal.SIGKILL)
                print(f"Killed process {pid} using port {port}.")
        except Exception as e:
            print(f"Error cleaning port {port}: {e}")

    def disconnect_trader_after_delay(trader_process, delay):
        """Disconnect a trader process after a specified delay."""
        def shutdown():
            if trader_process.is_alive():
                trader_process.terminate()
                print(f"Trader process {trader_process.pid} has been terminated after {delay} seconds.")
        
        threading.Timer(delay, shutdown).start()

    # Example configuration for the trading post network
    multiprocessing.set_start_method("spawn", force=True)
    clean_port(5555)
    network = TradingPostNetwork(num_buyers=N_B, num_sellers=N_S, num_traders=N_T, db_host='localhost', db_port=5555)
    db_process, trader_processes, seller_processes, buyer_processes, shipped_goods = network.setup_network()

    # Disconnect a trader after 10 seconds
    disconnect_delay = 10  # Time in seconds
    disconnect_trader_after_delay(trader_processes[0], disconnect_delay)

    try:
        while True:
            time.sleep(1)  # Keep the main process alive
    except KeyboardInterrupt:
        print("Shutting down trading post network...")
        # Terminate all processes
        db_process.terminate()
        for process in trader_processes + seller_processes + buyer_processes:
            process.terminate()
        print("Trading post network shutdown complete.")
