import multiprocessing
import time
import matplotlib.pyplot as plt
from network import TradingPostNetwork
from database_server import DatabaseServer


# Move `run_database` to the top level
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

    # Start the database server
    # db_process = multiprocessing.Process(
    #     target=run_database, args=(db_host, db_port, shipped_goods), daemon=True
    # )
    # db_process.start()
    # time.sleep(2)  # Allow the database server to initialize

    import socket
    import os
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
    
    # experiment_duration = 10  # Experiment duration in seconds
    db_port = get_random_port()
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

    # Start throughput monitoring
    monitor_process = multiprocessing.Process(
        target=monitor_throughput,
        args=(shipped_goods, duration),
        daemon=True
    )
    monitor_process.start()

    # Wait for the experiment duration
    time.sleep(duration)

    # Cleanup processes
    db_process.terminate()
    monitor_process.terminate()
    for process in trader_processes + seller_processes + buyer_processes:
        process.terminate()

    # Calculate throughput
    total_goods_shipped = shipped_goods.value
    throughput = total_goods_shipped / duration
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
    # Experiment configuration

    import socket
    import os
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
    
    experiment_duration = 10  # Experiment duration in seconds
    db_port = get_random_port()
    clean_port(db_port)

    # Define various hyperparameter configurations
    hyperparameter_configs = [
        # (10, 10, 1),  # (num_buyers, num_sellers, num_traders)
        (10, 10, 2),
        # (20, 20, 1),
        # (20, 20, 2),
        # (30, 30, 1),
        # (30, 30, 2)
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
