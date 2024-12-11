import multiprocessing
import time
from database_server import DatabaseServer
from trader import Trader
from seller import Seller
from buyer import Buyer

import math

N_B = 10
N_T = 2
N_S = 8
N_G = 10
T_G = 5

BUY_PROBABILITY = 1
MAX_TRANSACTIONS = math.inf

# Top-level functions for multiprocessing

def run_database(db_host, db_port, shipped_goods):
    """Top-level function to run the database server."""
    db_server = DatabaseServer(host=db_host, port=db_port, shipped_goods=shipped_goods)
    db_server.run()


def run_trader(db_host, db_port, trader_host, trader_port, trader_id):
    """Top-level function to run a trader."""
    trader = Trader(host=trader_host, port=trader_port, db_host=db_host, db_port=db_port, trader_id=trader_id)
    trader.run()


def run_seller(trader, seller_id, ng, tg, port):
    """Top-level function to run a seller."""
    seller = Seller(traders=[trader], seller_id=seller_id, ng=ng, tg=tg, port=port)
    seller.run()


def run_buyer(trader, buyer_id, max_transactions, buy_probability):
    """Top-level function to run a buyer."""
    buyer = Buyer(traders=[trader], buyer_id=buyer_id, max_transactions=max_transactions, p = buy_probability)
    buyer.run()


class TradingPostNetwork:
    def __init__(self, num_buyers=6, num_sellers=6, num_traders=3, db_host='localhost', db_port=5000):
        """
        Initialize the trading post network.
        """
        self.num_buyers = num_buyers
        self.num_sellers = num_sellers
        self.num_traders = num_traders
        self.db_host = db_host
        self.db_port = db_port
        self.trader_start_port = db_port + 1
        self.seller_start_port = 6000
        self.trading_posts = []

    def start_database_server(self):
        """Start the central warehouse database server."""
        from multiprocessing import Manager
        manager = Manager()
        shipped_goods = manager.Value("i", 0)

        # manager = Manager()
        # shipped_goods = manager.Value("i", 0)
        db_process = multiprocessing.Process(
            target=run_database,
            args=(self.db_host, self.db_port, shipped_goods),
            daemon=True
        )
        db_process.start()
        print(f"Database server started on {self.db_host}:{self.db_port}")
        return db_process, shipped_goods

    def distribute_entities(self, total_entities, num_posts):
        """Distribute entities (buyers or sellers) across trading posts."""
        base_count = total_entities // num_posts
        remainder = total_entities % num_posts
        distribution = [base_count + (1 if i < remainder else 0) for i in range(num_posts)]
        return distribution

    def start_traders(self):
        """Start traders for each trading post."""
        trader_processes = []
        for i in range(self.num_traders):
            trader_port = self.trader_start_port + i
            self.trading_posts.append({"trader": (self.db_host, trader_port), "buyers": [], "sellers": []})

            trader_process = multiprocessing.Process(
                target=run_trader,
                args=(self.db_host, self.db_port, self.db_host, trader_port, i + 1),
                daemon=True
            )
            trader_process.start()
            trader_processes.append(trader_process)
            print(f"Trader {i + 1} (Post {i + 1}) started on {self.db_host}:{trader_port}")
            time.sleep(0.5)
        return trader_processes

    def start_sellers(self):
        """Start sellers, distributed across trading posts."""
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
        """Start buyers, distributed across trading posts."""
        buyer_processes = []
        distribution = self.distribute_entities(self.num_buyers, self.num_traders)
        buyer_id = 1

        for i, post in enumerate(self.trading_posts):
            for _ in range(distribution[i]):
                buyer_process = multiprocessing.Process(
                    target=run_buyer,
                    args=(post["trader"], buyer_id, MAX_TRANSACTIONS, BUY_PROBABILITY),
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
        """Set up the entire trading post network."""
        print("Starting trading post network setup...")
        db_process,  shipped_goods = self.start_database_server()
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
    # Example configuration for the trading post network
    multiprocessing.set_start_method("spawn", force=True)

    network = TradingPostNetwork(num_buyers=N_B, num_sellers=N_S, num_traders=N_T, db_host='localhost', db_port=5555)
    db_process, trader_processes, seller_processes, buyer_processes = network.setup_network()

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
