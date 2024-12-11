import multiprocessing
import time
from database_server import DatabaseServer
from trader import Trader
from seller import Seller
from buyer import Buyer
from multiprocessing import Manager
import math
import threading


# Configuration
N_B = 10  # Number of buyers
N_T = 2   # Number of traders
N_S = 8   # Number of sellers
N_G = 10  # Goods accrued per interval
T_G = 5   # Interval for accruing goods

BUY_PROBABILITY = 1
MAX_TRANSACTIONS = math.inf


def run_database(db_host, db_port, shipped_goods):
    db_server = DatabaseServer(host=db_host, port=db_port, shipped_goods=shipped_goods)
    db_server.run()


def run_trader(db_host, db_port, trader_host, trader_port, trader_id, backup_host, backup_port, buyers, sellers):
    trader = Trader(
        host=trader_host,
        port=trader_port,
        db_host=db_host,
        db_port=db_port,
        trader_id=trader_id,
        buyers=buyers,
        sellers=sellers
    )
    trader.run(backup_host, backup_port)


def run_seller(trader_address, seller_id, ng, tg, port):
    seller = Seller(traders=[trader_address], seller_id=seller_id, ng=ng, tg=tg, port=port)
    seller.run()


def run_buyer(trader_address, buyer_id, max_transactions, buy_probability):
    buyer = Buyer(traders=[trader_address], buyer_id=buyer_id, max_transactions=max_transactions, p=buy_probability)
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
        self.buyers = []  # Global list of all buyers as (host, port)
        self.sellers = []  # Global list of all sellers as (host, port)

    def distribute_buyers_and_sellers(self):
        """Populate global buyers and sellers lists."""
        for i in range(self.num_sellers):
            port = self.seller_start_port + i + 1
            self.sellers.append(('localhost', port))

        for i in range(self.num_buyers):
            port = self.buyer_start_port + i + 1
            self.buyers.append(('localhost', port))

        print(f"Global Buyers: {self.buyers}")
        print(f"Global Sellers: {self.sellers}")
        
    def start_database_server(self):
        """Start the database server."""
        manager = Manager()
        shipped_goods = manager.Value("i", 0)  # Shared value for tracking shipped goods
        db_process = multiprocessing.Process(
            target=run_database,
            args=(self.db_host, self.db_port, shipped_goods),
            daemon=True
        )
        db_process.start()
        print(f"Database server started on {self.db_host}:{self.db_port}")
        return db_process, shipped_goods


    def start_traders(self):
        """Start traders and assign buyers and sellers."""
        trader_processes = []
        trader_ports = [self.trader_start_port + i for i in range(self.num_traders)]

        for i, port in enumerate(trader_ports):
            backup_port = trader_ports[(i + 1) % len(trader_ports)]  # Assign the next trader as backup
            assigned_buyers = self.buyers[i::self.num_traders]  # Divide buyers among traders
            assigned_sellers = self.sellers[i::self.num_traders]  # Divide sellers among traders

            self.trading_posts.append({"trader": (self.db_host, port), "buyers": assigned_buyers, "sellers": assigned_sellers})

            trader_process = multiprocessing.Process(
                target=run_trader,
                args=(self.db_host, self.db_port, self.db_host, port, i + 1, "localhost", backup_port, assigned_buyers, assigned_sellers),
                daemon=True
            )
            trader_process.start()
            trader_processes.append(trader_process)
            print(f"Trader {i + 1} started on {self.db_host}:{port} (heartbeat to port {backup_port}).")
            print(f"Trader {i + 1} Buyers: {assigned_buyers}, Sellers: {assigned_sellers}")
            time.sleep(0.5)

        return trader_processes

    def start_sellers(self):
        seller_processes = []
        for post in self.trading_posts:
            for seller in post["sellers"]:
                seller_port = seller[1]
                seller_process = multiprocessing.Process(
                    target=run_seller,
                    args=(post["trader"], seller, N_G, T_G, seller_port),
                    daemon=True
                )
                seller_process.start()
                seller_processes.append(seller_process)
                print(f"Seller {seller} started at {seller_port}.")
        return seller_processes

    def start_buyers(self):
        buyer_processes = []
        for post in self.trading_posts:
            for buyer in post["buyers"]:
                buyer_port = buyer[1]
                buyer_process = multiprocessing.Process(
                    target=run_buyer,
                    args=(post["trader"], buyer, MAX_TRANSACTIONS, BUY_PROBABILITY),
                    daemon=True
                )
                buyer_process.start()
                buyer_processes.append(buyer_process)
                print(f"Buyer {buyer} started at {buyer_port}.")
        return buyer_processes

    def setup_network(self):
        print("Starting trading post network setup...")
        db_process, shipped_goods = self.start_database_server()
        time.sleep(1)

        self.distribute_buyers_and_sellers()
        trader_processes = self.start_traders()
        seller_processes = self.start_sellers()
        buyer_processes = self.start_buyers()

        print("Trading post network setup complete.")
        for i, post in enumerate(self.trading_posts):
            print(f"Trading Post {i + 1}: Trader={post['trader']}, Buyers={post['buyers']}, Sellers={post['sellers']}")

        return db_process, trader_processes, seller_processes, buyer_processes, shipped_goods


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    network = TradingPostNetwork(num_buyers=N_B, num_sellers=N_S, num_traders=N_T, db_host='localhost', db_port=5555)
    db_process, trader_processes, seller_processes, buyer_processes, shipped_goods = network.setup_network()

    try:
        while True:
            time.sleep(1)  # Keep the main process alive
    except KeyboardInterrupt:
        print("Shutting down trading post network...")
        db_process.terminate()
        for process in trader_processes + seller_processes + buyer_processes:
            process.terminate()
        print("Trading post network shutdown complete.")
