import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('ETHERLINK_API_KEY')
BASE_URL = f"https://the-tie-mainnet-evm.octez.io?apikey={API_KEY}"

payload = {
    "jsonrpc": "2.0",
    "method": "eth_blockNumber",
    "params": [],
    "id": 1
}

headers = {"Content-Type": "application/json"}

response = requests.post(BASE_URL, json=payload, headers=headers)

print(f"Status: {response.status_code}")
print(f"Response: {response.json()}")
