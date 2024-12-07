import multiprocessing
import time
from database_server import DatabaseServer
from seller import Seller
from trader import Trader
from buyer import Buyer


def run_database(host, port):
    """Top-level function to start the database server."""
    db_server = DatabaseServer(host=host, port=port)
    db_server.run()


def run_trader(host, port, db_host, db_port, trader_id):
    """Top-level function to start a trader."""
    trader = Trader(host=host, port=port, db_host=db_host, db_port=db_port, trader_id=trader_id)
    trader.run()


def run_seller(traders, seller_id, ng, tg, port):
    """Top-level function to start a seller with a specified listener port."""
    seller = Seller(traders=traders, seller_id=seller_id, ng=ng, tg=tg, port=port)
    seller.run()



def run_buyer(traders, buyer_id):
    """Top-level function to start a buyer."""
    buyer = Buyer(traders=traders, buyer_id=buyer_id, p = 1)
    buyer.run()


class NetworkSetup:
    def __init__(self, num_buyers=2, num_sellers=2, num_traders=1, db_host='localhost', db_port=5000):
        """
        Initialize the network setup.
        :param num_buyers: Number of buyer nodes.
        :param num_sellers: Number of seller nodes.
        :param num_traders: Number of trader nodes.
        :param db_host: Host of the database server.
        :param db_port: Port of the database server.
        """
        self.num_buyers = num_buyers
        self.num_sellers = num_sellers
        self.num_traders = num_traders
        self.db_host = db_host
        self.db_port = db_port
        self.trader_start_port = db_port + 1  # Traders start on ports after the database server port

    def start_database_server(self):
        """Start the database server in a separate process."""
        db_process = multiprocessing.Process(
            target=run_database, args=(self.db_host, self.db_port), daemon=True
        )
        db_process.start()
        print(f"Database server started on {self.db_host}:{self.db_port}")
        return db_process

    def start_traders(self):
        """Start trader nodes in separate processes."""
        trader_processes = []
        for i in range(self.num_traders):
            trader_port = self.trader_start_port + i
            trader_process = multiprocessing.Process(
                target=run_trader,
                args=(self.db_host, trader_port, self.db_host, self.db_port, i + 1),
                daemon=True
            )
            trader_process.start()
            trader_processes.append(trader_process)
            print(f"Trader {i + 1} started on {self.db_host}:{trader_port}")
            time.sleep(0.5)  # Allow time for startup
        return trader_processes

    def start_sellers(self, traders):
        """Start seller nodes in separate processes with unique listener ports."""
        seller_processes = []
        base_port = 6000  # Base port for sellers
        for i in range(self.num_sellers):
            seller_port = base_port + i  # Unique port for each seller
            seller_process = multiprocessing.Process(
                target=run_seller,
                args=(traders, i + 1, 10, 50, seller_port),  # Pass the unique port
                daemon=True
            )
            seller_process.start()
            seller_processes.append(seller_process)
            print(f"Seller {i + 1} started with listener on port {seller_port}.")
            time.sleep(0.5)  # Allow time for startup
        return seller_processes


    def start_buyers(self, traders):
        """Start buyer nodes in separate processes."""
        buyer_processes = []
        for i in range(self.num_buyers):
            buyer_process = multiprocessing.Process(
                target=run_buyer,
                args=(traders, i + 1),
                daemon=True
            )
            buyer_process.start()
            buyer_processes.append(buyer_process)
            print(f"Buyer {i + 1} started.")
            time.sleep(0.5)  # Allow time for startup
        return buyer_processes

    def setup_network(self):
        """Setup the entire network."""
        print("Starting network setup...")
        db_process = self.start_database_server()

        # Prepare trader information
        traders = [(self.db_host, self.trader_start_port + i) for i in range(self.num_traders)]

        trader_processes = self.start_traders()
        seller_processes = self.start_sellers(traders)
        buyer_processes = self.start_buyers(traders)

        print("Network setup complete. All components are running.")
        return db_process, trader_processes, seller_processes, buyer_processes


if __name__ == "__main__":
    # Example configuration
    network = NetworkSetup(num_buyers=2, num_sellers=1, num_traders=3, db_host='localhost', db_port=5555)
    db_process, trader_processes, seller_processes, buyer_processes = network.setup_network()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down network...")
        # Terminate all processes
        db_process.terminate()
        for process in trader_processes + seller_processes + buyer_processes:
            process.terminate()
        print("Network shutdown complete.")
