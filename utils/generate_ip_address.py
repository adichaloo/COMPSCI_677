import random

def generate_ip():
    """Generate a single valid IPv4 address."""
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"

def generate_unique_ips(n):
    """Generate a set of 'n' unique valid IPv4 addresses."""
    ip_set = set()  # Use a set to store unique IPs
    
    while len(ip_set) < n:
        new_ip = generate_ip()
        ip_set.add(new_ip)  # Set ensures uniqueness automatically
    
    return list(ip_set)

def main():
    num_ips = int(input("Enter the number of IPs to generate: "))
    
    # Generate 'num_ips' unique IP addresses
    ip_addresses = generate_unique_ips(num_ips)
    
    print("\nGenerated unique IP addresses:")
    for ip in ip_addresses:
        print(ip)

if __name__ == "__main__":
    main()

