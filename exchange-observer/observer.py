import asyncio
import datetime
import json
import os
from contextlib import asynccontextmanager

import ccxt.async_support as ccxt
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# Configuration
OBSERVER_ID = os.getenv("OBSERVER_ID", "default-observer")
# Get comma-separated addresses and split into a list
wallet_addresses_str = os.getenv(
    "WALLET_ADDRESSES", "0x6F87795cF1B94f1572c161E0633751C9e226f955"
)
WALLET_ADDRESSES = [address.strip() for address in wallet_addresses_str.split(",")]
POLL_INTERVAL_SECONDS = 5
OUTPUT_FILE = "/app/data/3T-observer.json"

# In-memory cache for the positions data
positions_cache = {}


async def poll_positions_periodically():
    """Periodically polls for position data and updates the JSON file."""
    exchange = ccxt.hyperliquid()
    try:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

        while True:
            try:
                all_positions = {}

                async def fetch_and_store(address, positions):
                    try:
                        payload = {"type": "clearinghouseState", "user": address}
                        state = await exchange.public_post_info(payload)
                        if state:
                            positions[address] = state
                    except Exception as e:
                        print(f"Error fetching state for {address}: {e}")

                tasks = [
                    fetch_and_store(address, all_positions)
                    for address in WALLET_ADDRESSES
                ]
                await asyncio.gather(*tasks)

                timestamp = datetime.datetime.now(datetime.UTC).isoformat()

                output_data = {
                    "observer_id": OBSERVER_ID,
                    "timestamp": timestamp,
                    "positions": all_positions,
                }

                global positions_cache
                positions_cache = output_data

                try:
                    with open(OUTPUT_FILE, "w") as f:
                        json.dump(output_data, f, indent=4)
                except OSError as e:
                    print(f"Error writing to {OUTPUT_FILE}: {e}")

            except Exception as e:
                print(f"An error occurred in the polling loop: {e}")

            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        await exchange.close()
        print("Exchange connection closed.")


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
        print("Polling task cancelled.")


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
    if not positions_cache:
        if os.path.exists(OUTPUT_FILE):
            try:
                with open(OUTPUT_FILE) as f:
                    return JSONResponse(content=json.load(f))
            except (OSError, json.JSONDecodeError) as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Could not read or parse positions file: {e}",
                ) from e
        else:
            raise HTTPException(
                status_code=404,
                detail="Position data not available yet. Please try again later.",
            )
    return JSONResponse(content=positions_cache)
