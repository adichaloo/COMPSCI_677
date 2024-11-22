# plot_response_times.py

import matplotlib.pyplot as plt

def plot_response_times(file_path):
    with open(file_path, "r") as f:
        response_times = [float(line.strip()) for line in f.readlines()]

    plt.figure(figsize=(10, 6))
    plt.plot(response_times, marker='o', linestyle='-', color='b')
    plt.title('Response Times per Request')
    plt.xlabel('Request Number')
    plt.ylabel('Response Time (seconds)')
    plt.grid(True)
    plt.savefig(file_path+".png")
    plt.show()

if __name__ == "__main__":
    plot_response_times("buyer_2_response_times.txt")
    plot_response_times("buyer_1_response_times.txt")
