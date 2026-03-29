import asyncio
import datetime
import json
import os
import sys
from contextlib import asynccontextmanager

import ccxt.async_support as ccxt
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

# Add shared to sys.path to allow importing config
sys.path.append("/app")
try:
    from shared.config import config
except ImportError:
    config = None

# Configuration
OBSERVER_ID = os.getenv("OBSERVER_ID", "default-observer")
wallet_addresses_str = os.getenv(
    "WALLET_ADDRESSES", "0x6F87795cF1B94f1572c161E0633751C9e226f955"
)
WALLET_ADDRESSES = [address.strip() for address in wallet_addresses_str.split(",") if address.strip()]
POLL_INTERVAL_SECONDS = 30
OUTPUT_FILE = "/tmp/3T-observer.json"
HIP3_DEXES = ["xyz"]

# Initialize cache with empty data to avoid 404s during startup
positions_cache = {
    "observer_id": OBSERVER_ID,
    "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
    "positions": {}
}

async def poll_positions_periodically():
    """Periodically polls for position data and updates the JSON file."""
    global positions_cache
    print(f"Observer: Starting background polling task for {len(WALLET_ADDRESSES)} addresses...")
    
    # Try to load existing data from disk on startup
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE) as f:
                data = json.load(f)
                positions_cache = data
                print(f"Observer: Loaded data for {len(data.get('positions', {}))} addresses from disk.")
        except Exception as e:
            print(f"Observer: Could not load disk cache: {e}")

    while True:
        try:
            # Simple setup to ensure stability; we avoid proxy rotation here for now
            async with ccxt.hyperliquid({"enableRateLimit": True, "timeout": 15000}) as exchange:
                new_positions = {}
                for address in WALLET_ADDRESSES:
                    try:
                        print(f"Observer: Polling {address}...")
                        payload = {"type": "clearinghouseState", "user": address}
                        state = await exchange.public_post_info(payload)
                        
                        if state:
                            # Fetch HIP-3 dex positions and merge into assetPositions
                            for dex in HIP3_DEXES:
                                try:
                                    hip3_state = await exchange.public_post_info({
                                        "type": "clearinghouseState",
                                        "user": address,
                                        "dex": dex,
                                    })
                                    if hip3_state and hip3_state.get("assetPositions"):
                                        state.setdefault("assetPositions", []).extend(
                                            hip3_state["assetPositions"]
                                        )
                                except Exception as e:
                                    print(f"Observer: HIP-3 error for {address} on {dex}: {e}")
                            
                            new_positions[address] = state
                    except Exception as e:
                        print(f"Observer: Error polling {address}: {e}")
                        if "429" in str(e):
                            print("Observer: Rate limited (429). Skipping remaining addresses this cycle.")
                            break
                
                if new_positions:
                    timestamp = datetime.datetime.now(datetime.UTC).isoformat()
                    positions_cache = {
                        "observer_id": OBSERVER_ID,
                        "timestamp": timestamp,
                        "positions": new_positions
                    }
                    
                    # Ensure data directory exists and save to disk
                    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
                    try:
                        with open(OUTPUT_FILE, "w") as f:
                            json.dump(positions_cache, f, indent=4)
                    except Exception as e:
                        print(f"Observer: Error writing to disk: {e}")
                        
                    print(f"Observer: Cache updated with {len(new_positions)} addresses at {timestamp}")

        except Exception as e:
            print(f"Observer: Unexpected error in polling loop: {e}")
            
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the background polling task
    task = asyncio.create_task(poll_positions_periodically())
    yield
    # Clean up the task on shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
    }


@app.get("/3T-observer.json")
async def get_observer_data():
    """Returns the latest polled position data."""
    return JSONResponse(content=positions_cache)

# Serve static files from /app/www (mounted from docs/www)
if os.path.exists("/app/www"):
    app.mount("/", StaticFiles(directory="/app/www", html=True), name="static")
