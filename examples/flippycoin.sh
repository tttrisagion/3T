#!/bin/bash

# ==============================================================================
# Flippycoin Runner Script
# ==============================================================================
#
# This script acts as a safety harness for the main flippycoin trading script.
# It ensures that the trading process runs continuously in a loop.
#
# How it works:
# 1. It first checks if the required `example_feed.py` process is running.
# 2. If the feed is running, it displays a welcome message and waits for user
#    confirmation to start.
# 3. It starts the main Python script (`example_flippycoin.py`).
# 4. The Python script is designed to run for one "block" and then exit.
# 5. When the Python script exits, this shell script catches it, waits, and
#    restarts it, ensuring a clean state for the next block.
#

# --- Dependency Check ---
# Before doing anything else, ensure the required price feed script is running.
if ! pgrep -f "example_feed.py" > /dev/null; then
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "!! ERROR: The 'example_feed.py' price feed is NOT running."
    echo "!! This script is required to provide price data."
    echo "!! Please start it in another terminal before running this script."
    echo "!! Example: python3 examples/example_feed.py"
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    exit 1 # Exit the script with an error code
fi

# --- Welcome Message and Confirmation ---
echo '''
   ▄▖▄▖  ▄▖▖ ▄▖
   ▄▌▐   ▙▖▌ ▌ 
   ▄▌▐   ▌ ▙▖▙▖
            
'
echo "Welcome to the Flippycoin 'Hello World' Trading Demonstration!"
echo ""
echo "This script will launch and manage the trading bot."
echo "The bot will run continuously, restarting with a fresh state for each new 'block'."
echo "Press CTRL+C in this terminal to stop the bot completely."
echo ""
echo "✓ Price feed (example_feed.py) is running."


# --- Coin Flip Animation ---
echo -n "Initializing... "
frames=("  o  " "  O  " "  0  " "  O  " "  o  " "  -  " "  o  " "  O  " "  0  " "  O  ")
for i in {1..15}; do
    echo -ne "   ${frames[i % ${#frames[@]}]}"
    sleep 0.1
done
echo "     O   All systems ready!"
echo ""


read -n 1 -s -r -p "Press any key to begin..."
echo ""
echo "Starting..."


# --- Main Application Loop ---
while true; do
    # Execute the main Python application.
    # We use '/usr/bin/env python3' to ensure we use the correct Python interpreter
    # from the user's environment.
    /usr/bin/env python3 /opt/3T/examples/example_flippycoin.py

    # The script has finished its block. We add a small delay before restarting.
    # This prevents rapid-fire restarts in case of an immediate crash.
    echo "----------------------------------------------------------------"
echo "Block finished. The script will restart with a fresh state in 5 seconds."
echo "Press CTRL+C now to exit."
echo "----------------------------------------------------------------"
sleep 5
done
