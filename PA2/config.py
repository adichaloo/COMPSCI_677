# import math
# # config.py
# BUY_PROBABILITY = 1  # Probability that a buyer will continue buying after a successful purchase
# SELLER_STOCK = 5     # Each seller starts with 5 items
# MAX_TRANSACTIONS = math.inf   # NUMBER OF TRANSACTIONS A BUYER CAN DO BEFORE IT SHUTSDOWN
# TIMEOUT = 0.1  #S
# PRICE = 1
# COMMISSION = 0.1
#
# LEADER_FAILURE_PROBABILITY = 1 # Probability that the leader dies (20%)
# TIME_QUANTUM = 5  # Time in seconds
#
# OK_TIMEOUT = 1

# config.py

BUY_PROBABILITY = 0.5
SELLER_STOCK = 10
MAX_TRANSACTIONS = 5
TIMEOUT = 5  # seconds
PRICE = 10
COMMISSION = 0.1
TIME_QUANTUM = 10  # seconds between leader checks
LEADER_FAILURE_PROBABILITY = 0.8
OK_TIMEOUT = 1  # seconds to wait for OK messages during election