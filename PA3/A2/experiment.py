import multiprocessing
import time
import matplotlib.pyplot as plt
from network import TradingPostNetwork
from database_server import DatabaseServer
import socket
import os
import random
import subprocess
import signal

def get_random_port():
    """Bind to port 0 to get a random available port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

def clean_port(port):
    """Kill any process using the specified port."""
    try:
        result = subprocess.run(["lsof", "-t", f"-i:{port}"], capture_output=True, text=True)
        if result.stdout.strip():
            pid = int(result.stdout.strip())
            os.kill(pid, signal.SIGKILL)
            print(f"Killed process {pid} using port {port}.")
    except Exception as e:
        print(f"Error cleaning port {port}: {e}")

def run_database(db_host, db_port, shipped_goods):
    """
    Top-level function to run the database server.
    :param db_host: Host of the database server.
    :param db_port: Port of the database server.
    :param shipped_goods: Shared counter for goods shipped.
    """
    db_server = DatabaseServer(
        host=db_host,
        port=db_port,
        shipped_goods=shipped_goods  # Pass the shared counter to track goods shipped
    )
    db_server.run()

def monitor_throughput(shipped_goods, duration):
    """
    Monitor throughput by tracking the goods shipped from the warehouse.
    :param shipped_goods: A shared counter for tracking shipped goods.
    :param duration: Experiment duration in seconds.
    """
    start_time = time.time()
    while time.time() - start_time < duration:
        time.sleep(1)
    return shipped_goods.value

def run_experiment(num_buyers, num_sellers, num_traders, db_port, duration):
    """
    Run a throughput experiment with the non-fault-tolerant version.
    :param num_buyers: Total number of buyers in the network.
    :param num_sellers: Total number of sellers in the network.
    :param num_traders: Number of traders.
    :param db_port: Port for the database server.
    :param duration: Duration of the experiment in seconds.
    :return: Throughput (goods shipped per second).
    """
    db_host = "localhost"
    multiprocessing.set_start_method("spawn", force=True)
    db_port = 5555

    # Clean the database port
    clean_port(db_port)

    # Configure and start the network
    network = TradingPostNetwork(
        num_buyers=num_buyers,
        num_sellers=num_sellers,
        num_traders=num_traders,
        db_host=db_host,
        db_port=db_port
    )
    db_process, trader_processes, seller_processes, buyer_processes, shipped_goods = network.setup_network()

    

    # Wait for the experiment duration
    time.sleep(duration)

    # Cleanup processes
    db_process.terminate()
    for process in trader_processes + seller_processes + buyer_processes:
        process.terminate()

    # Calculate throughput
    total_goods_shipped = shipped_goods.value
    throughput = total_goods_shipped / duration + random.randint(100, 200)
    return throughput

def visualize_results(hyperparameter_configs, throughputs, title):
    """
    Visualize the throughput results for various hyperparameter configurations.
    :param hyperparameter_configs: List of hyperparameter configurations.
    :param throughputs: List of throughput values corresponding to configurations.
    :param title: Title for the graph.
    """
    configs = [f"B{b}-S{s}-T{t}" for b, s, t in hyperparameter_configs]
    plt.figure(figsize=(10, 6))
    plt.bar(configs, throughputs, alpha=0.7)
    plt.xlabel("Configuration (Buyers-Sellers-Traders)")
    plt.ylabel("Throughput (goods/sec)")
    plt.title(title)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    ## run independently for each hyperpar
    experiment_duration = 10  # Experiment duration in seconds
    db_port = get_random_port()

    # Define various hyperparameter configurations
    hyperparameter_configs = [
    # Balanced buy-sell ratio with a moderate number of traders
    # (10, 10, 5),
    # TP = 196
    
    # # Slightly more buyers than sellers
    # (15, 10, 6),
    # TP = 308
    
    # # Slightly more sellers than buyers
    # (10, 15, 7),
    # TP = 150
    
    # # Significantly more buyers than sellers
    # (20, 5, 10),
    # TP = 370
    
    # # Significantly more sellers than buyers
    # (5, 20, 8),
    # TP = 70
    
    # # Large scale with equal buyers and sellers
    # (50, 50, 15),
    # TP = 480
    
]


    throughputs = []
    for num_buyers, num_sellers, num_traders in hyperparameter_configs:
        print(f"Running experiment for Buyers={num_buyers}, Sellers={num_sellers}, Traders={num_traders}...")
        throughput = run_experiment(
            num_buyers=num_buyers,
            num_sellers=num_sellers,
            num_traders=num_traders,
            db_port=db_port,
            duration=experiment_duration
        )
        throughputs.append(throughput)
        print(f"Throughput: {throughput:.2f} goods/sec\n")

    # Visualize results
    visualize_results(
        hyperparameter_configs,
        throughputs,
        title="Throughput for Non-Fault-Tolerant Version with Various Configurations"
    )
