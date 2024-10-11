from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request
from web3 import Web3
from typing import Dict, List
import psycopg2
import time
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

INFURA_PROJECT_ID = os.getenv("INFURA_API_KEY")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_ID = os.getenv("DATABASE_ID")

templates = Jinja2Templates(directory="templates")

CHAIN_RPC_URLS = {
    'ethereum': f'https://mainnet.infura.io/v3/{INFURA_PROJECT_ID}',
    'polygon': f'https://polygon-mainnet.infura.io/v3/{INFURA_PROJECT_ID}',
    'base': f'https://base-mainnet.infura.io/v3/{INFURA_PROJECT_ID}',
    'arbitrum': f'https://arbitrum-mainnet.infura.io/v3/{INFURA_PROJECT_ID}'
}

app = FastAPI()

# Mount the /static route for serving static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://token-pulse-4c9e7163d991.herokuapp.com/"],  # Or limit this to the frontend URL, e.g., ["http://localhost:8001"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def get_frontend(request: Request):
    return templates.TemplateResponse("frontend.html", {"request": request})

# Simple cache to store data per chain
cache = {}
CACHE_EXPIRY_TIME = 6 * 60 * 60  # 6 hours

# Function to check if the cache is valid
def is_cache_valid(chain):
    if chain in cache:
        cached_data = cache[chain]
        if time.time() - cached_data["timestamp"] < CACHE_EXPIRY_TIME:
            return True
    return False

# Chain URLs mapping
chain_urls = {
    "ethereum": "https://etherscan.io",
    "base": "https://basescan.org",
    "polygon": "https://polygonscan.com",
    "arbitrum": "https://arbiscan.io/"
}

# SQL connection (example configuration)
def get_connection():
    return psycopg2.connect(
        database=os.getenv("DATABASE_ID"),
        user=os.getenv("DATABASE_USER"),
        password="aaa", 
        host="pg.eu-west-1.agnostic.engineering", 
        port="5432",
        sslmode="require"
    )

# ERC-20 ABI snippet
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function"
    }
]

# ERC-721 ABI snippet
ERC721_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    }
]

# Function to determine if the contract is ERC-20 or ERC-721
def is_erc721(web3, token_address):
    try:
        # Try calling the `supports_interface` function for ERC-721 (0x80ac58cd)
        erc721_interface_id = Web3.to_bytes(hexstr="0x80ac58cd")
        contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC721_ABI)
        return contract.functions.supports_interface(erc721_interface_id).call()
    except Exception:
        return False


# Function to get token name, symbol, and decimals using Web3 and Infura
def get_token_info(chain, token_address):
    try:
        # Get the appropriate RPC URL for the chain
        rpc_url = CHAIN_RPC_URLS.get(chain, CHAIN_RPC_URLS['ethereum'])
        
        # Initialize Web3 connection
        web3 = Web3(Web3.HTTPProvider(rpc_url))
        
        # Check if connected by querying the latest block number (alternative to is_connected())
        try:
            web3.eth.block_number
        except Exception as e:
            raise Exception(f"Failed to connect to {chain} via Infura: {e}")

        # Determine if the contract is ERC-20 or ERC-721
        if is_erc721(web3, token_address):
            # ERC-721 Token
            token_contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC721_ABI)
            token_name = token_contract.functions.name().call()
            token_symbol = token_contract.functions.symbol().call()
            token_decimals = None  # ERC-721 tokens do not have decimals
        else:
            # ERC-20 Token
            token_contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
            token_name = token_contract.functions.name().call()
            token_symbol = token_contract.functions.symbol().call()
            token_decimals = token_contract.functions.decimals().call()

        return token_name, token_symbol, token_decimals
    except Exception as e:
        print(f"Error fetching token info for {token_address} on {chain}: {e}")
        return 'Unknown', 'Unknown', None  # Default to 'Unknown' and None decimals if something goes wrong

# Function to fetch token activity data based on the chain
def fetch_token_activity_data(chain: str) -> List[Dict]:
    conn = get_connection()
    cur = conn.cursor()

    # Define the table name based on the chain parameter
    table_name = f"evm_events_{chain}_mainnet_v1"

    # SQL query with the dynamic table name
    query = f"""
        with token_activity as (
            select
                address as token_address,
                date_trunc('day', timestamp) as stringDay,
                count(distinct input_0_value_address) as unique_addresses,
                sum(input_2_value_uint256) as total_volume,
                count(*) as total_transactions
            from {table_name}
            where
                signature = 'Transfer(address,address,uint256)'
                and timestamp >= now() - interval '14 days'  -- Only keep data from the last 14 days
            group by
                token_address, date_trunc('day', timestamp)
            having
                count(distinct input_0_value_address) > 100  -- Filter out tokens with fewer transactions
        ),

        rolling_7_day as (
            select
                token_address,
                stringDay,
                sum(unique_addresses) over (partition by token_address order by stringDay rows between 13 preceding and current row) as rolling_unique_addresses,
                sum(total_volume) over (partition by token_address order by stringDay rows between 13 preceding and current row) as rolling_total_volume,
                sum(total_transactions) over (partition by token_address order by stringDay rows between 13 preceding and current row) as rolling_total_transactions
            from
                token_activity
        ),
        comparison as (
            select
                token_address,
                max(case when stringDay between date_trunc('day', now() - interval '7 days') and date_trunc('day', now()) then rolling_unique_addresses end) as current_week_unique_addresses,
                max(case when stringDay between date_trunc('day', now() - interval '14 days') and date_trunc('day', now() - interval '7 days') then rolling_unique_addresses end) as previous_week_unique_addresses,
                max(case when stringDay between date_trunc('day', now() - interval '7 days') and date_trunc('day', now()) then rolling_total_volume end) as current_week_total_volume,
                max(case when stringDay between date_trunc('day', now() - interval '14 days') and date_trunc('day', now() - interval '7 days') then rolling_total_volume end) as previous_week_total_volume,
                max(case when stringDay between date_trunc('day', now() - interval '7 days') and date_trunc('day', now()) then rolling_total_transactions end) as current_week_total_transactions,
                max(case when stringDay between date_trunc('day', now() - interval '14 days') and date_trunc('day', now() - interval '7 days') then rolling_total_transactions end) as previous_week_total_transactions
            from
                rolling_7_day
            group by
                token_address
        )
        select
            token_address,
            coalesce(current_week_unique_addresses, 0) - coalesce(previous_week_unique_addresses, 0) as unique_addresses_growth,
            coalesce(current_week_total_transactions, 0) - coalesce(previous_week_total_transactions, 0) as total_transaction_growth,
            current_week_unique_addresses,
            previous_week_unique_addresses,
            current_week_total_volume,
            previous_week_total_volume,
            current_week_total_transactions,
            previous_week_total_transactions
        from
            comparison
        where
            previous_week_unique_addresses > 100
        order by
            unique_addresses_growth desc
        limit 50;
    """

    print(f"Executing query: {query}")

    try:
        # Execute the query
        cur.execute(query)
        rows = cur.fetchall()
    except Exception as e:
        print(f"Error executing query: {e}")
        cur.close()
        conn.close()
        return []

    # Fetch the column names from the cursor
    columns = [desc[0] for desc in cur.description]

    # Convert to a list of dictionaries
    results = [dict(zip(columns, row)) for row in rows]

    # Enrich each token with name, symbol, token type (ERC-20/721), and decimals
    for token in results:
        address = token['token_address']
        
        # Use the get_token_info function to get name, symbol, and decimals
        name, symbol, decimals = get_token_info(chain, address)
        
        # Determine token type based on decimals (ERC-20 usually has decimals, ERC-721 does not)
        if decimals is None:
            token_type = 'ERC-721'
        else:
            token_type = 'ERC-20'
        
        token['label'] = f"{name} ({symbol})"
        token['token_type'] = token_type
        token['decimals'] = decimals if decimals is not None else 'N/A'  # No decimals for ERC-721 tokens


    cur.close()
    conn.close()
    return results

@app.get("/data/{chain}")
def get_data(chain: str):
    try:
        # Check if data is cached and valid
        if is_cache_valid(chain):
            print(f"Serving cached data for {chain}")
            return cache[chain]["data"]

        # If no valid cache, fetch token activity data
        token_data = fetch_token_activity_data(chain)

        # Store the result in the cache with a timestamp
        cache[chain] = {
            "data": token_data,
            "timestamp": time.time()  # Store the time the data was cached
        }

        print(f"Fetching and caching data for {chain}")
        return token_data
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
