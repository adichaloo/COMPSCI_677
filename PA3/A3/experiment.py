import multiprocessing
import threading
import time
import math
from multiprocessing import Manager
from database_server import DatabaseServer
from trader import Trader  # Ensure trader.py has symmetrical heartbeat logic
from seller import Seller
from buyer import Buyer


N_B = 10  # Number of buyers
N_T = 2   # Number of traders
N_S = 8   # Number of sellers
N_G = 10  # Number of goods
T_G = 5   # Time to produce goods

BUY_PROBABILITY = 1
MAX_TRANSACTIONS = math.inf

# Functions for running entities
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
    buyer = Buyer(traders=[trader], buyer_id=buyer_id, max_transactions=max_transactions, p=buy_probability, port=port)
    buyer.run()

# Trading post network class
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
        trader_processes = []
        trader_ports = [5002, 5003]
        all_clients = []

        for i in range(self.num_sellers):
            port = self.seller_start_port + i + 1
            all_clients.append(('localhost', port))

        for i in range(self.num_buyers):
            port = self.buyer_start_port + i + 1
            all_clients.append(('localhost', port))

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
        return db_process, trader_processes, seller_processes, buyer_processes, shipped_goods

def monitor_throughput_for_duration(shipped_goods, duration, interval=1):
    """
    Monitor throughput for a specific duration by measuring goods shipped per second.
    """
    start_time = time.time()
    end_time = start_time + duration
    previous_count = shipped_goods.value
    throughput_data = []

    while time.time() < end_time:
        time.sleep(interval)
        current_count = shipped_goods.value
        throughput = current_count - previous_count
        throughput_data.append(throughput)
        previous_count = current_count

    total_shipped = sum(throughput_data)
    average_throughput = total_shipped / duration
    return average_throughput, throughput_data

def run_experiment():
    """
    Run the experiment to assess throughput before and after a trader failure.
    """
    N_B = 5
    N_S = 20
    N_T = 2
    DURATION_BEFORE = 10  # Monitor for 10 seconds before failure
    DURATION_AFTER = 20  # Monitor for 10 seconds after failure

    network = TradingPostNetwork(num_buyers=N_B, num_sellers=N_S, num_traders=N_T, db_host='localhost', db_port=5555)
    db_process, trader_processes, seller_processes, buyer_processes, shipped_goods = network.setup_network()
    time.sleep(DURATION_BEFORE)
    shipped_goods_before = shipped_goods.value

    # print("Monitoring throughput before trader failure...")
    # throughput_before, data_before = monitor_throughput_for_duration(shipped_goods, DURATION_BEFORE)
    # print(f"Throughput before trader failure: {throughput_before:.2f} goods/second")

    print("Simulating trader failure...")
    trader_processes[0].terminate()
    trader_processes[0].join()
    print("Trader 1 has failed. Assessing fault tolerance...")

    print("Monitoring throughput after trader failure...")
    time.sleep(1)  # Allow fault tolerance mechanisms to stabilize
    time.sleep(DURATION_AFTER)
    shipped_goods_after = shipped_goods.value - shipped_goods_before
    # throughput_after, data_after = monitor_throughput_for_duration(shipped_goods, DURATION_AFTER)
    # print(f"Throughput after trader failure: {throughput_after:.2f} goods/second")

    # Clean up processes
    print("Terminating all processes...")
    db_process.terminate()
    for process in trader_processes[1:] + seller_processes + buyer_processes:
        process.terminate()

    print("Experiment complete.")
    return shipped_goods_before/DURATION_BEFORE, shipped_goods_after/DURATION_AFTER

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    throughput_before, throughput_after = run_experiment()
    print(f"Average Throughput Before Failure: {throughput_before:.2f} goods/second")
    print(f"Average Throughput After Failure: {throughput_after:.2f} goods/second")


##RESULTS
'''
(10,10,2): befor=20, after = 11.45
(15, 10, 2): before = 27.2 , after = 14.8 
(10, 15, 2): before = 17.7, after = 11.6
(20, 5, 2): before = 45.8, after =23.75
(5, 20, 2): before = 8.2, after = 6.5
(50, 50, 2): before =145.8 , after =56.1
'''