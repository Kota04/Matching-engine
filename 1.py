import asyncio
import aiohttp
import json
import time

# URL of the target Node.js server endpoint
url = "http://localhost:3000/buy"

# Data to be sent with each request
data_list = [
    {"id": 1, "quantity": 40, "type": 0, "price": 300},
    {"id": 2, "quantity": 60, "type": 1, "price": 400},
    {"id": 3, "quantity": 50, "type": 0, "price": 500},
    {"id": 4, "quantity": 70, "type": 1, "price": 600},
    {"id": 5, "quantity": 80, "type": 0, "price": 700},
    {"id": 6, "quantity": 50, "type": 1, "price": 500},
    {"id": 7, "quantity": 40, "type": 0, "price": 700},
    {"id": 8, "quantity": 80, "type": 1, "price": 400},
    {"id": 9, "quantity": 90, "type": 0, "price": 300},
    {"id": 10, "quantity": 85, "type": 1, "price": 400},
    {"id": 11, "quantity": 80, "type": 0, "price": 500},
    {"id": 12, "quantity": 90, "type": 1, "price": 300}
]

# Function to send a POST request from a specified local port to port 3000
async def send_post_request(session, data, local_port):
    headers = {'Content-Type': 'application/json'}
    url_with_port = f"http://localhost:3000/buy"  # Always target port 3000
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
