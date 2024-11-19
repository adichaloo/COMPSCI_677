# utils.py

import threading
import pickle
import os

# Event to indicate if an election is in progress
is_election_in_progress = threading.Event()

def persist_leader_state(inventory):
    """Persist the inventory state to disk."""
    with open('inventory_state.pkl', 'wb') as f:
        pickle.dump(inventory, f)
    print("[Persist] Leader's inventory state has been saved.")

def load_leader_state():
    """Load the inventory state from disk."""
    if os.path.exists('inventory_state.pkl'):
        with open('inventory_state.pkl', 'rb') as f:
            inventory = pickle.load(f)
        print("[Load] Leader's inventory state has been loaded.")
        return inventory
    else:
        print("[Load] No inventory state found. Starting with empty inventory.")
        return {}
