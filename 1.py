import asyncio
import aiohttp
import json
import time
import uuid
from datetime import datetime

# URL of the target Node.js server endpoint
url = "http://localhost:3000/bids"

# Helper function to generate UUIDs
def generate_uuid():
    return str(uuid.uuid4())

# Helper function to get the current timestamp
def current_timestamp():
    return datetime.utcnow().isoformat()

# Data to be sent with each request
data_list = [
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 25, "price_per_unit": 150, "trade_type": False, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 35, "price_per_unit": 250, "trade_type": True, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 45, "price_per_unit": 350, "trade_type": False, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 55, "price_per_unit": 450, "trade_type": True, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 65, "price_per_unit": 550, "trade_type": False, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 75, "price_per_unit": 650, "trade_type": True, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 85, "price_per_unit": 750, "trade_type": False, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 95, "price_per_unit": 850, "trade_type": True, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 105, "price_per_unit": 950, "trade_type": False, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 115, "price_per_unit": 1050, "trade_type": True, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 125, "price_per_unit": 1150, "trade_type": False, "time": current_timestamp(), "status": False},
    {"id": generate_uuid(), "user_id": generate_uuid(), "channel_id": generate_uuid(), "units": 135, "price_per_unit": 1250, "trade_type": True, "time": current_timestamp(), "status": False}
]

# Function to send a POST request from a specified local port to port 3000
async def send_post_request(session, data, local_port):
    headers = {'Content-Type': 'application/json'}
    url_with_port = f"http://localhost:3000/bids"  # Always target port 3000
    while True:
        try:
            async with session.post(url_with_port, headers=headers, data=json.dumps(data)) as response:
                response_text = await response.text()
                print(f"Sent data: {data} from port {local_port}, Response: {response.status}, Response Body: {response_text}")
            break  # Exit the retry loop if request succeeds
        except aiohttp.ClientOSError as e:
            print(f"Encountered ClientOSError: {e}, retrying after 1 second...")
            time.sleep(1)  # Wait for 1 second before retrying

async def main():
    # Starting local port
    starting_port = 5000
    
    # Create a TCPConnector with limit=None, i.e., no limit on the number of connections
    connector = aiohttp.TCPConnector(limit=None)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Create tasks for each request
        tasks = []
        for i, data in enumerate(data_list):
            local_port = starting_port + i
            task = send_post_request(session, data, local_port)
            tasks.append(task)
        
        # Execute tasks concurrently
        await asyncio.gather(*tasks)

# Run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())
