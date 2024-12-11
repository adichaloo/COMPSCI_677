import socket
import threading

class OversellRateCollector:
    def __init__(self, host="localhost", port=8888):
        self.host = host
        self.port = port
        self.reports = []
        self.lock = threading.Lock()

    def handle_report(self, client_socket):
        try:
            data = client_socket.recv(1024).decode()
            with self.lock:
                self.reports.append(data)
                print(f"Received: {data}")
        finally:
            client_socket.close()

    def start(self):
        """Start the collector server."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.host, self.port))
            server_socket.listen(5)
            print(f"Oversell Rate Collector running on {self.host}:{self.port}")
            while True:
                client_socket, _ = server_socket.accept()
                threading.Thread(target=self.handle_report, args=(client_socket,), daemon=True).start()

    def get_report(self):
        """Concatenate and return all reports."""
        with self.lock:
            return "\n".join(self.reports)
        
if __name__ == "__main__":
    collector = OversellRateCollector(host="localhost", port=8888)
    collector.start()

