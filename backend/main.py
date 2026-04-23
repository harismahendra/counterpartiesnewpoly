"""
Dome API WebSocket Backend
Handles WebSocket connection to Dome API and broadcasts to frontend
"""
import asyncio
import json
import os
import re
import socket
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor
import socketio
import websockets
import aiohttp
import math
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Load environment variables
# Try to load from backend/.env first, then fallback to root .env
env_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    load_dotenv()  # Fallback to default location

# Configuration
DOME_API_KEY = os.getenv('DOME_API_KEY')
PLATFORM = os.getenv('PLATFORM', 'polymarket')
WS_VERSION = int(os.getenv('WS_VERSION', '1'))
# Use PORT from Render in production, fallback to BACKEND_PORT or 8000
BACKEND_PORT = int(os.getenv('PORT') or os.getenv('BACKEND_PORT', '8000'))

# Load wallet addresses
def load_wallet_addresses() -> List[str]:
    """Load wallet addresses from environment variables"""
    addresses = []
    
    # Try numbered format first
    index = 1
    while True:
        addr = os.getenv(f'WALLET_{index}_ADDRESS')
        if not addr:
            break
        addresses.append(addr.strip())
        index += 1
    
    # Fall back to comma-separated format
    if not addresses:
        wallet_addresses = os.getenv('WALLET_ADDRESSES', '')
        if wallet_addresses:
            addresses = [addr.strip() for addr in wallet_addresses.split(',') if addr.strip()]
    
    # Validate addresses
    eth_address_regex = re.compile(r'^0x[a-fA-F0-9]{40}$')
    valid_addresses = [addr for addr in addresses if eth_address_regex.match(addr)]
    
    return valid_addresses

WALLET_ADDRESSES = load_wallet_addresses()

# WebSocket connection state
dome_ws = None
is_connected = False
subscriptions = {}
dome_task = None
pending_updates = {}  # Track orders waiting for delayed updates

# Order history storage (in-memory, 24 hours retention)
order_history = []  # List of order dicts
MAX_ORDER_AGE_HOURS = 24
MAX_ORDER_HISTORY = 4000

# Batching and worker pool for high-frequency updates
order_queue = None  # Will be initialized in lifespan
BATCH_SIZE = 10  # Process orders in batches
BATCH_TIMEOUT = 0.5  # Wait max 0.5s before processing batch
MAX_WORKERS = 5  # Max concurrent database queries
db_semaphore = None  # Will be initialized in lifespan

# Polymarket CLOB settings
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
CLOB_REQUEST_TIMEOUT_SECONDS = 5
AFTER_MIN_AGE_SECONDS = 10
AFTER_RETRY_SECONDS = 30
AFTER_MAX_AGE_SECONDS = 120
AFTER_COLLECTOR_POLL_SECONDS = 5

# Cache for Polymarket API data (address -> data, expires after 1 hour)
polymarket_cache = {}  # {address: {'data': {...}, 'expires_at': timestamp}}
CACHE_DURATION = 3600  # 1 hour
TOKEN_METADATA_CACHE_DURATION = 3 * 3600  # 3 hours
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
polymarket_token_metadata_cache = {}  # {token_id: {'data': {...}, 'expires_at': timestamp}}
polymarket_token_metadata_inflight = {}  # {token_id: asyncio.Task}

# Team mappings for Pinnacle deltas matching (from analyzer3.py approach)
TEAM_MAPPINGS = {
    'MLB': {
        'mariners': 'SEA', 'rockies': 'COL', 'giants': 'SF', 'royals': 'KC',
        'astros': 'HOU', 'orioles': 'BAL', 'mets': 'NYM', 'cardinals': 'STL',
        'yankees': 'NYY', 'twins': 'MIN', 'tigers': 'DET', 'blue jays': 'TOR',
        'pirates': 'PIT', 'guardians': 'CLE', 'white sox': 'CWS', 'red sox': 'BOS',
        'brewers': 'MIL', 'braves': 'ATL', 'marlins': 'MIA', 'nationals': 'WSH',
        'phillies': 'PHI', 'rangers': 'TEX', 'rays': 'TB', 'cubs': 'CHC',
        'diamondbacks': 'ARI', 'dodgers': 'LAD', 'padres': 'SD', 'angels': 'LAA',
        'athletics': 'OAK', 'reds': 'CIN',
    },
    'NFL': {
        'eagles': 'PHI', 'cardinals': 'ARI', 'giants': 'NYG', 'falcons': 'ATL',
        'panthers': 'CAR', 'bears': 'CHI', 'cowboys': 'DAL', 'lions': 'DET',
        'packers': 'GB', 'rams': 'LAR', 'vikings': 'MIN', 'saints': 'NO',
        '49ers': 'SF', 'seahawks': 'SEA', 'buccaneers': 'TB', 'commanders': 'WSH',
        'ravens': 'BAL', 'bills': 'BUF', 'bengals': 'CIN', 'browns': 'CLE',
        'broncos': 'DEN', 'texans': 'HOU', 'colts': 'IND', 'jaguars': 'JAX',
        'chiefs': 'KC', 'chargers': 'LAC', 'dolphins': 'MIA', 'raiders': 'LV',
        'patriots': 'NE', 'jets': 'NYJ', 'steelers': 'PIT', 'titans': 'TEN',
    },
    'NHL': {
        'bruins': 'BOS', 'sabres': 'BUF', 'hurricanes': 'CAR', 'blue jackets': 'CBJ',
        'flames': 'CGY', 'blackhawks': 'CHI', 'avalanche': 'COL', 'stars': 'DAL',
        'red wings': 'DET', 'oilers': 'EDM', 'panthers': 'FLA', 'ducks': 'ANA',
        'golden knights': 'VGK', 'kraken': 'SEA', 'blues': 'STL', 'lightning': 'TBL',
        'mammoth': 'UTA', 'canucks': 'VAN', 'jets': 'WPG', 'capitals': 'WSH',
        'wild': 'MIN', 'canadiens': 'MTL', 'devils': 'NJD', 'predators': 'NSH',
        'islanders': 'NYI', 'rangers': 'NYR', 'senators': 'OTT', 'flyers': 'PHI',
        'penguins': 'PIT', 'sharks': 'SJS', 'maple leafs': 'TOR', 'utah': 'UTA',
        'kings': 'LAK',
    },
    'NBA': {
        'hawks': 'ATL', 'celtics': 'BOS', 'nets': 'BKN', 'hornets': 'CHA',
        'bulls': 'CHI', 'cavaliers': 'CLE', 'mavericks': 'DAL', 'nuggets': 'DEN',
        'pistons': 'DET', 'rockets': 'HOU', 'pacers': 'IND', 'clippers': 'LAC',
        'lakers': 'LAL', 'grizzlies': 'MEM', 'heat': 'MIA', 'bucks': 'MIL',
        'timberwolves': 'MIN', 'pelicans': 'NOP', 'knicks': 'NYK', 'thunder': 'OKC',
        'magic': 'ORL', '76ers': 'PHI', 'suns': 'PHX', 'trail blazers': 'POR',
        'kings': 'SAC', 'spurs': 'SAS', 'raptors': 'TOR', 'jazz': 'UTA',
        'warriors': 'GSW', 'wizards': 'WAS', 'pho': 'PHX',
    },
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown"""
    # Startup
    global dome_task
    
    print(f"🚀 Starting Dome API Backend...")
    print(f"📋 Configuration:")
    print(f"   Platform: {PLATFORM}")
    print(f"   Version: {WS_VERSION}")
    
    # Test database connection
    print(f"\n🔌 Testing Polymarket database connection...")
    database_url = os.getenv('DATABASE_URL_POLY')
    
    # Debug: Check all DATABASE related env vars
    all_db_vars = {k: v for k, v in os.environ.items() if 'DATABASE' in k}
    if all_db_vars:
        print(f"🔍 Found DATABASE env vars: {list(all_db_vars.keys())}")
    
    # Also check if it's in the .env file directly
    env_file_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file_path):
        try:
            with open(env_file_path, 'r') as f:
                env_content = f.read()
                if 'DATABASE_URL_POLY' in env_content:
                    print(f"✅ DATABASE_URL_POLY found in .env file")
                    # Check if it's commented out
                    lines = env_content.split('\n')
                    for i, line in enumerate(lines, 1):
                        if 'DATABASE_URL_POLY' in line:
                            if line.strip().startswith('#'):
                                print(f"⚠️  DATABASE_URL_POLY is commented out at line {i}")
                            else:
                                print(f"   Found at line {i}: {line.strip()[:80]}...")
                else:
                    print(f"❌ DATABASE_URL_POLY not found in .env file content")
        except Exception as e:
            print(f"⚠️  Could not read .env file: {e}")
    
    if not database_url:
        print(f"❌ DATABASE_URL_POLY not found in environment")
        print(f"⚠️  Matching will be disabled. Check DATABASE_URL_POLY in your .env file")
        print(f"💡 Current working directory: {os.getcwd()}")
        print(f"💡 .env file path: {env_file_path}")
    else:
        # Mask password in URL for display
        masked_url = database_url
        if '@' in database_url:
            parts = database_url.split('@')
            if ':' in parts[0]:
                user_pass = parts[0].split('://')
                if len(user_pass) > 1:
                    protocol = user_pass[0]
                    credentials = user_pass[1]
                    if ':' in credentials:
                        user = credentials.split(':')[0]
                        masked_url = f"{protocol}://{user}:***@{parts[1]}"
        
        print(f"   URL: {masked_url}")
        test_conn = get_poly_db_connection()
        if test_conn:
            try:
                cursor = test_conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                test_conn.close()
                print(f"✅ Polymarket database connection successful")
            except Exception as e:
                print(f"❌ Polymarket database connection test failed: {e}")
                print(f"⚠️  Matching will be disabled. Check DATABASE_URL_POLY in .env")
        else:
            print(f"❌ Could not connect to Polymarket database")
            print(f"⚠️  Matching will be disabled. Check DATABASE_URL_POLY in .env")
    
    if DOME_API_KEY and WALLET_ADDRESSES:
        print(f"\n📋 Wallet Configuration:")
        print(f"   Wallets: {len(WALLET_ADDRESSES)}")
        print(f"   Addresses: {', '.join(WALLET_ADDRESSES)}\n")
        
        # Start Dome WebSocket connection
        dome_task = asyncio.create_task(connect_to_dome())
    elif not DOME_API_KEY:
        print("\n❌ DOME_API_KEY is required in .env file")
    elif not WALLET_ADDRESSES:
        print("\n❌ No wallet addresses configured")
    
    # Start background task to clean up old orders every hour
    async def periodic_cleanup():
        while True:
            await asyncio.sleep(3600)  # Every hour
            cleanup_old_orders()
    
    cleanup_task = asyncio.create_task(periodic_cleanup())
    
    # Initialize batching system
    global order_queue, db_semaphore
    order_queue = asyncio.Queue()
    db_semaphore = asyncio.Semaphore(MAX_WORKERS)
    
    # Start order batch processor workers
    batch_workers = []
    num_workers = 3  # Multiple workers to handle high frequency
    for i in range(num_workers):
        worker = asyncio.create_task(order_batch_processor())
        batch_workers.append(worker)
        print(f"🚀 Started batch processor worker {i+1}/{num_workers}")

    after_collector_task = asyncio.create_task(polymarket_after_collector())
    print("🚀 Started Polymarket after collector")
    
    try:
        yield
    finally:
        # Shutdown batch workers
        for worker in batch_workers:
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        
        # Shutdown cleanup task
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass

        after_collector_task.cancel()
        try:
            await after_collector_task
        except asyncio.CancelledError:
            pass
        
        if dome_task:
            dome_task.cancel()
            try:
                await dome_task
            except asyncio.CancelledError:
                pass
        if dome_ws:
            await dome_ws.close()

# FastAPI app with lifespan
app = FastAPI(title="Dome API Backend", lifespan=lifespan)

# Socket.IO server for real-time updates
sio = socketio.AsyncServer(
    cors_allowed_origins=[
        f"http://localhost:{os.getenv('FRONTEND_PORT', '3000')}",
        os.getenv('FRONTEND_URL', ''),  # Production frontend URL
    ] if os.getenv('FRONTEND_URL') else [f"http://localhost:{os.getenv('FRONTEND_PORT', '3000')}"],
    async_mode='asgi'
)
socketio_app = socketio.ASGIApp(sio, app)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        f"http://localhost:{os.getenv('FRONTEND_PORT', '3000')}",
        os.getenv('FRONTEND_URL', ''),  # Production frontend URL
    ] if os.getenv('FRONTEND_URL') else [f"http://localhost:{os.getenv('FRONTEND_PORT', '3000')}"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "running",
        "connected": is_connected,
        "wallets": len(WALLET_ADDRESSES),
        "subscriptions": len(subscriptions)
    }

@app.get("/health")
async def health():
    """Health check"""
    return {"status": "healthy", "connected": is_connected}

@app.get("/api/orders/history")
async def get_order_history():
    """Get order history from last 48 hours"""
    try:
        # Clean up old orders first
        cleanup_old_orders()
        
        # Return recent orders (sorted by timestamp, newest first)
        return {
            "orders": order_history,
            "count": len(order_history),
            "max_age_hours": MAX_ORDER_AGE_HOURS
        }
    except Exception as e:
        print(f"❌ Error getting order history: {e}")
        return {"orders": [], "count": 0, "error": str(e)}

@app.get("/api/orders/opposite-parties")
async def get_opposite_parties_summary(user_filter: Optional[str] = None, time_filter: Optional[str] = None):
    """Get summary of opposite parties (taker/maker addresses not in our wallet list)"""
    try:
        # Clean up old orders first
        cleanup_old_orders()
        
        # Normalize wallet addresses for comparison (lowercase)
        our_wallets = {addr.lower() for addr in WALLET_ADDRESSES}
        
        # Optional filter by user column (case-insensitive contains)
        user_filter_norm = (user_filter or "").strip().lower()
        # Optional time filter aligned with frontend time selector
        time_filter_norm = (time_filter or "all").strip().lower()
        filter_hours_map = {
            "3h": 3,
            "8h": 8,
            "12h": 12,
            "24h": 24,
        }
        time_cutoff: Optional[float] = None
        if time_filter_norm in filter_hours_map:
            time_cutoff = datetime.now().timestamp() - (filter_hours_map[time_filter_norm] * 60 * 60)

        # Group orders by opposite party
        party_stats = {}  # {address: {'volume': 0, 'profit': 0, 'orders': 0, 'address': addr}}
        
        for order in order_history:
            # Apply time filter if requested
            if time_cutoff is not None and _get_order_time_for_retention(order) < time_cutoff:
                continue

            # Get opposite party address (taker or maker that's not us)
            user_addr = order.get('user', '').lower() if order.get('user') else ''
            taker_addr = order.get('taker', '').lower() if order.get('taker') else ''

            # Apply user filter first (only keep orders matching selected user)
            if user_filter_norm and user_filter_norm not in user_addr:
                continue
            
            # Determine opposite party
            opposite_party = None
            if user_addr and user_addr not in our_wallets:
                opposite_party = user_addr
            elif taker_addr and taker_addr not in our_wallets:
                opposite_party = taker_addr
            
            if not opposite_party:
                continue  # Skip if we can't identify opposite party
            
            # Initialize party stats if needed
            if opposite_party not in party_stats:
                party_stats[opposite_party] = {
                    'address': opposite_party,
                    'volume': 0.0,
                    'volume_with_pnl': 0.0,  # Volume for orders that have PNL data
                    'profit': 0.0,
                    'orders': 0,
                    'profitable_volume': 0.0,  # Volume for profitable orders (negative PNL)
                    'unprofitable_volume': 0.0  # Volume for unprofitable orders (positive PNL)
                }
            
            # Calculate order value
            price = order.get('price', 0) or 0
            shares = order.get('shares_normalized', 0) or 0
            value = price * shares
            
            # Calculate profitability from PM After data
            pm_after = order.get('polymarket_after')
            profit = 0.0
            has_pnl = False
            if pm_after and pm_after.get('bbo') is not None:
                fill_price = price
                bbo_after = pm_after.get('bbo', 0)
                if fill_price > 0 and bbo_after > 0:
                    # Profit = (Fill Price - PM After BBO) * Shares
                    # Negative means we paid less than market (profitable)
                    # Positive means we paid more than market (unprofitable)
                    # This matches the percentage calculation: (fillPrice - bbo) / fillPrice
                    price_diff = fill_price - bbo_after
                    profit = price_diff * shares
                    has_pnl = True
            
            # Update party stats
            party_stats[opposite_party]['volume'] += value
            party_stats[opposite_party]['profit'] += profit
            party_stats[opposite_party]['orders'] += 1
            
            # Track volume that has PNL data
            if has_pnl:
                party_stats[opposite_party]['volume_with_pnl'] += value
                
                # Track volume by profitability
                if profit < 0:  # Negative profit = profitable (we paid less)
                    party_stats[opposite_party]['profitable_volume'] += value
                elif profit > 0:  # Positive profit = unprofitable (we paid more)
                    party_stats[opposite_party]['unprofitable_volume'] += value
        
        # Calculate PNL percentage for each party
        for party in party_stats.values():
            if party['volume_with_pnl'] > 0:
                # PNL % = (PNL / Volume with PNL) * 100
                party['pnl_percentage'] = (party['profit'] / party['volume_with_pnl']) * 100
            else:
                party['pnl_percentage'] = None
        
        # Convert to list and sort by volume (descending)
        parties_list = list(party_stats.values())
        parties_list.sort(key=lambda x: x['volume'], reverse=True)
        
        # Enrich with Polymarket account info (in parallel, but limit concurrent requests)
        async def enrich_party(party):
            address = party['address']
            pm_info = await get_polymarket_account_info(address)
            if pm_info:
                party['polymarket'] = pm_info
            else:
                party['polymarket'] = None
        
        # Enrich all parties in parallel (but limit to 5 concurrent requests)
        semaphore = asyncio.Semaphore(5)
        async def enrich_with_limit(party):
            async with semaphore:
                await enrich_party(party)
        
        if parties_list:
            await asyncio.gather(*[enrich_with_limit(p) for p in parties_list], return_exceptions=True)
        
        # Calculate score for each party after enrichment
        def logistic(roi, steepness=80):
            """Neutral ROI = 0 → score = 0"""
            return 100 * (1 / (1 + math.exp(-steepness * roi)) - 0.5)
        
        def compute_score(roi, volume, markets_traded):
            """Compute score based on ROI, volume, and markets traded"""
            # Low-sample automatic categories
            if volume < 50_000 and markets_traded < 100:
                if volume < 10_000:
                    return -50
                return -40
            
            # ROI-based scoring
            base_score = logistic(roi)
            
            vol_mult = 1 + 0.00002 * math.log(volume) if volume > 0 else 1
            mkt_mult = 1 + 0.015 * math.log(1 + markets_traded) if markets_traded > 0 else 1
            
            final_score = base_score * vol_mult * mkt_mult
            final_score = max(-50, min(50, final_score))
            
            return round(final_score, 2)
        
        for party in parties_list:
            pm = party.get('polymarket', {})
            pm_volume = pm.get('global_volume', 0) or 0
            pm_trades = pm.get('total_trades', 0) or 0
            pm_pnl = pm.get('global_pnl', 0) or 0
            
            # Calculate ROI: PNL / Volume
            roi = (pm_pnl / pm_volume) if pm_volume > 0 else 0
            
            # Compute score
            party['score'] = compute_score(roi, pm_volume, pm_trades)
        
        return {
            "parties": parties_list,
            "count": len(parties_list),
            "total_volume": sum(p['volume'] for p in parties_list),
            "total_profit": sum(p['profit'] for p in parties_list),
            "time_filter": time_filter_norm if time_filter_norm in filter_hours_map or time_filter_norm == "all" else "all"
        }
    except Exception as e:
        print(f"❌ Error getting opposite parties summary: {e}")
        import traceback
        traceback.print_exc()
        return {"parties": [], "count": 0, "error": str(e)}

@app.get("/api/takers/enrich")
async def enrich_takers(addresses: str):
    """Enrich taker addresses with Polymarket profile info"""
    try:
        address_list = addresses.split(',') if addresses else []
        if not address_list:
            return {"takers": []}
        
        # Enrich all addresses in parallel
        semaphore = asyncio.Semaphore(5)
        async def enrich_with_limit(address):
            async with semaphore:
                pm_info = await get_polymarket_account_info(address)
                return {
                    "address": address,
                    "polymarket": pm_info if pm_info else None
                }
        
        enriched = await asyncio.gather(*[enrich_with_limit(addr.strip()) for addr in address_list], return_exceptions=True)
        
        # Filter out exceptions
        takers = [t for t in enriched if not isinstance(t, Exception)]
        
        return {"takers": takers}
    except Exception as e:
        print(f"❌ Error enriching takers: {e}")
        import traceback
        traceback.print_exc()
        return {"takers": [], "error": str(e)}

def _get_order_time_for_retention(order: dict) -> float:
    """Return epoch seconds for retention checks, preferring received_at."""
    received_at = order.get('received_at')
    if received_at:
        try:
            parsed = datetime.fromisoformat(str(received_at).replace('Z', '+00:00'))
            return parsed.timestamp()
        except Exception:
            pass
    
    raw_timestamp = order.get('timestamp', 0)
    try:
        ts = float(raw_timestamp)
        # Normalize milliseconds to seconds if needed
        if ts > 1e12:
            ts = ts / 1000.0
        return ts
    except Exception:
        return 0.0

def cleanup_old_orders():
    """Remove orders older than the configured retention window."""
    global order_history
    if not order_history:
        return
    
    cutoff_time = datetime.now().timestamp() - (MAX_ORDER_AGE_HOURS * 60 * 60)
    initial_count = len(order_history)
    
    order_history = [
        order for order in order_history
        if _get_order_time_for_retention(order) > cutoff_time
    ]
    
    removed = initial_count - len(order_history)
    if removed > 0:
        print(f"🗑️  Cleaned up {removed} orders older than {MAX_ORDER_AGE_HOURS}h")

async def get_polymarket_account_info(address: str):
    """Get Polymarket account information from multiple APIs"""
    global polymarket_cache
    
    # Check cache first
    address_lower = address.lower()
    if address_lower in polymarket_cache:
        cached = polymarket_cache[address_lower]
        if cached['expires_at'] > datetime.now().timestamp():
            return cached['data']
    
    try:
        async with aiohttp.ClientSession() as session:
            results = {
                'address': address,
                'total_trades': None,
                'global_volume': None,
                'global_pnl': None,
                'global_rank': None,
                'verified_badge': False,
                'name': None,
                'pseudonym': None,
                'created_at': None
            }
            
            # 1. Total Trades API
            try:
                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'origin': 'https://polymarket.com',
                    'referer': 'https://polymarket.com/',
                }
                async with session.get(
                    f'https://data-api.polymarket.com/traded?user={address}',
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        results['total_trades'] = data.get('traded', 0)
            except Exception as e:
                print(f"⚠️  Error fetching total trades for {address}: {e}")
            
            # 2. Leaderboard API
            try:
                async with session.get(
                    'https://data-api.polymarket.com/v1/leaderboard',
                    params={
                        'timePeriod': 'all',
                        'orderBy': 'VOL',
                        'limit': 1,
                        'offset': 0,
                        'category': 'overall',
                        'user': address
                    },
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data and len(data) > 0:
                            user_data = data[0]
                            results['global_volume'] = user_data.get('vol', 0)  # Use 'vol' not 'volume'
                            results['global_pnl'] = user_data.get('pnl', 0)
                            results['global_rank'] = user_data.get('rank', None)
                            results['verified_badge'] = user_data.get('verifiedBadge', False)
            except Exception as e:
                print(f"⚠️  Error fetching leaderboard for {address}: {e}")
            
            # 3. Profile API
            try:
                async with session.get(f'https://polymarket.com/api/profile/userData?address={address}', timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        results['name'] = data.get('name', None)
                        results['pseudonym'] = data.get('pseudonym', None)
                        results['created_at'] = data.get('createdAt', None)
                        if data.get('verifiedBadge', False):
                            results['verified_badge'] = True
            except Exception as e:
                print(f"⚠️  Error fetching profile for {address}: {e}")
            
            # Cache the results
            polymarket_cache[address_lower] = {
                'data': results,
                'expires_at': datetime.now().timestamp() + CACHE_DURATION
            }
            
            return results
    except Exception as e:
        print(f"❌ Error getting Polymarket account info for {address}: {e}")
        return None

def _parse_json_list_field(raw_value):
    if isinstance(raw_value, list):
        return raw_value
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []

async def get_polymarket_token_metadata(token_id: str):
    """Resolve token metadata (label/slug/title/condition) from Gamma markets API."""
    global polymarket_token_metadata_cache, polymarket_token_metadata_inflight

    token_id_normalized = str(token_id or '').strip()
    if not token_id_normalized:
        return None

    now_ts = datetime.now().timestamp()
    cached = polymarket_token_metadata_cache.get(token_id_normalized)
    if cached and cached.get('expires_at', 0) > now_ts:
        return cached.get('data')

    existing_task = polymarket_token_metadata_inflight.get(token_id_normalized)
    if existing_task:
        return await existing_task

    async def _fetch_metadata():
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.get(
                    GAMMA_MARKETS_URL,
                    params={'clob_token_ids': token_id_normalized}
                ) as resp:
                    if resp.status != 200:
                        return None

                    payload = await resp.json(content_type=None)
                    if not isinstance(payload, list) or not payload:
                        return None

                    market = payload[0] if isinstance(payload[0], dict) else None
                    if not market:
                        return None

                    clob_token_ids = _parse_json_list_field(market.get('clobTokenIds'))
                    outcomes = _parse_json_list_field(market.get('outcomes'))

                    token_label = None
                    for idx, candidate_token_id in enumerate(clob_token_ids):
                        if str(candidate_token_id).strip() == token_id_normalized:
                            if idx < len(outcomes):
                                token_label = outcomes[idx]
                            break

                    metadata = {
                        'token_id': token_id_normalized,
                        'token_label': token_label,
                        'market_slug': market.get('slug'),
                        'title': market.get('question') or market.get('title'),
                        'condition_id': market.get('conditionId')
                    }

                    polymarket_token_metadata_cache[token_id_normalized] = {
                        'data': metadata,
                        'expires_at': datetime.now().timestamp() + TOKEN_METADATA_CACHE_DURATION
                    }
                    return metadata
        except Exception as e:
            print(f"⚠️  Error fetching token metadata for token_id={token_id_normalized}: {e}")
            return None

    task = asyncio.create_task(_fetch_metadata())
    polymarket_token_metadata_inflight[token_id_normalized] = task
    try:
        return await task
    finally:
        polymarket_token_metadata_inflight.pop(token_id_normalized, None)

async def backfill_order_metadata_from_token_id(order_data: Dict):
    """Fill missing order fields from token_id via Polymarket Gamma API."""
    if not isinstance(order_data, dict):
        return

    token_id = str(order_data.get('token_id') or '').strip()
    if not token_id:
        return

    needs_backfill = (
        not str(order_data.get('token_label') or '').strip() or
        not str(order_data.get('market_slug') or '').strip() or
        not str(order_data.get('title') or '').strip() or
        not str(order_data.get('condition_id') or '').strip()
    )
    if not needs_backfill:
        return

    metadata = await get_polymarket_token_metadata(token_id)
    if not metadata:
        return

    if not str(order_data.get('token_label') or '').strip() and metadata.get('token_label'):
        order_data['token_label'] = metadata['token_label']
    if not str(order_data.get('market_slug') or '').strip() and metadata.get('market_slug'):
        order_data['market_slug'] = metadata['market_slug']
    if not str(order_data.get('title') or '').strip() and metadata.get('title'):
        order_data['title'] = metadata['title']
    if not str(order_data.get('condition_id') or '').strip() and metadata.get('condition_id'):
        order_data['condition_id'] = metadata['condition_id']

@sio.event
async def connect(sid, environ):
    """Handle frontend connection"""
    print(f"📱 Frontend connected: {sid}")
    await sio.emit('status', {'connected': is_connected}, room=sid)

@sio.event
async def disconnect(sid):
    """Handle frontend disconnection"""
    print(f"📱 Frontend disconnected: {sid}")

def get_poly_db_connection():
    """Create PostgreSQL database connection for Polymarket BBO"""
    database_url = os.getenv('DATABASE_URL_POLY')
    if not database_url:
        print(f"⚠️  DATABASE_URL_POLY not found in environment variables")
        return None
    
    try:
        result = urlparse(database_url)
        # Determine if SSL is needed (Render, AWS, etc.)
        ssl_mode = None
        if result.hostname and ('render.com' in result.hostname or 'amazonaws.com' in result.hostname):
            ssl_mode = 'require'
        
        connection = psycopg2.connect(
            database=result.path[1:] if result.path else None,
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port,
            connect_timeout=10,
            cursor_factory=RealDictCursor,
            sslmode=ssl_mode
        )
        return connection
    except psycopg2.OperationalError as e:
        print(f"⚠️  Database connection error: {e}")
        return None
    except Exception as e:
        print(f"⚠️  Error connecting to Polymarket DB: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None

def normalize_league_name(league: str) -> Optional[str]:
    if not league:
        return None
    league_upper = league.strip().upper()
    league_map = {
        'NBA': 'NBA',
        'NFL': 'NFL',
        'MLB': 'MLB',
        'NHL': 'NHL',
        'ATP': 'ATP',
        'WTA': 'WTA',
    }
    return league_map.get(league_upper)

def get_league_from_market_slug(market_slug: str) -> Optional[str]:
    if not market_slug:
        return None
    parts = market_slug.split('-')
    if not parts:
        return None
    return parts[0].upper()

def should_run_pinnacle_matching(market_slug: str) -> bool:
    """Only run Pinnacle matching for moneyline-style slugs."""
    slug = (market_slug or '').strip().lower()
    if not slug:
        return False
    # Spread/total market slugs do not map reliably to deltas team rows.
    if '-spread-' in slug or '-total-' in slug:
        return False
    return True

def get_pinnacle_db_connection():
    """Create PostgreSQL database connection for Pinnacle deltas (DATABASE_URL)."""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        return None
    try:
        result = urlparse(database_url)
        ssl_mode = None
        if result.hostname and ('render.com' in result.hostname or 'amazonaws.com' in result.hostname):
            ssl_mode = 'require'
        return psycopg2.connect(
            database=result.path[1:] if result.path else None,
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port,
            connect_timeout=10,
            cursor_factory=RealDictCursor,
            sslmode=ssl_mode
        )
    except Exception as e:
        print(f"⚠️  Error connecting to Pinnacle DB: {type(e).__name__}: {e}")
        return None

def get_deltas_team_name(fill_side: str, league: str) -> Optional[str]:
    """Convert fill side to Deltas team abbreviation/name using analyzer3-style mappings."""
    if not fill_side or not league:
        return None
    normalized_sport = normalize_league_name(league)
    if not normalized_sport:
        return None

    fill_lower = str(fill_side).strip().lower()
    league_teams = TEAM_MAPPINGS.get(normalized_sport, {})
    mapped = league_teams.get(fill_lower)
    if mapped:
        return mapped

    # Tennis fallback: preserve analyzer3 behavior (best-effort name matching).
    if normalized_sport in ['ATP', 'WTA']:
        words = fill_lower.split()
        return ' '.join(word.capitalize() for word in words)
    return None

def _normalize_deltas_record(record: Dict, fill_timestamp_ms: int, relation: str) -> Dict:
    bbo_price = float(record.get('bbo_price')) if record.get('bbo_price') is not None else None
    ts_ms = float(record.get('timestamp_ms')) if record.get('timestamp_ms') is not None else None
    seconds_from_fill = None
    if ts_ms is not None:
        if relation == 'before':
            seconds_from_fill = round((fill_timestamp_ms - ts_ms) / 1000.0, 3)
        else:
            seconds_from_fill = round((ts_ms - fill_timestamp_ms) / 1000.0, 3)

    ts_raw = record.get('timestamp')
    timestamp_str = None
    if isinstance(ts_raw, datetime):
        timestamp_str = ts_raw.isoformat()
    elif ts_raw is not None:
        timestamp_str = str(ts_raw)

    return {
        'bbo': bbo_price,
        'percentage': record.get('percentage'),
        'total_spread': record.get('total_spread'),
        'opponent_percentage': record.get('opponent_percentage'),
        'team': record.get('team'),
        'opposing_team': record.get('opposing_team'),
        'seconds_from_fill': seconds_from_fill,
        'timestamp': timestamp_str,
        'source': str(record.get('sportsbook') or 'pinnacle').strip().lower()
    }

def _get_pinnacle_bbo_sync(market_slug: str, token_label: str, fill_timestamp_ms: int, relation: str):
    """Get Pinnacle before/after deltas using analyzer3-style matching."""
    conn = None
    cursor = None
    try:
        league = normalize_league_name(get_league_from_market_slug(market_slug))
        if not league:
            return None

        team_name = get_deltas_team_name(token_label, league) or token_label
        if not team_name:
            return None

        conn = get_pinnacle_db_connection()
        if not conn:
            return None
        cursor = conn.cursor()

        window_before_ms = 5 * 60 * 1000
        window_after_ms = 5 * 60 * 1000
        start_ms = fill_timestamp_ms - window_before_ms
        end_ms = fill_timestamp_ms + window_after_ms
        after_min_ms = fill_timestamp_ms + 10 * 1000

        base_query = """
            SELECT
                timestamp_ms,
                timestamp,
                sport,
                sportsbook,
                team,
                opposing_team,
                percentage,
                bbo_price,
                total_spread
            FROM deltas
            WHERE UPPER(COALESCE(sportsbook, '')) IN ('PINNACLE', 'CONSENSUS')
              AND timestamp_ms >= %s
              AND timestamp_ms <= %s
              AND UPPER(sport) = %s
        """
        params = [start_ms, end_ms, league]

        if league in ['ATP', 'WTA']:
            # Tennis names vary; use contains match similar to analyzer fallback behavior.
            base_query += " AND (UPPER(team) LIKE UPPER(%s) OR UPPER(opposing_team) LIKE UPPER(%s))"
            like_team = f"%{team_name}%"
            params.extend([like_team, like_team])
        else:
            # Team sports use mapped abbreviations.
            base_query += " AND (team = %s OR opposing_team = %s)"
            params.extend([team_name, team_name])

        base_query += " ORDER BY timestamp_ms ASC LIMIT 500"
        cursor.execute(base_query, tuple(params))
        records = cursor.fetchall()
        if not records:
            return None

        # Remove LOCKED/invalid rows as analyzer3 does
        valid_records = []
        for r in records:
            percentage_str = str(r.get('percentage')).upper() if r.get('percentage') is not None else ''
            total_spread_str = str(r.get('total_spread')).upper() if r.get('total_spread') is not None else ''
            bbo_price = r.get('bbo_price')
            if 'LOCKED' in percentage_str or 'LOCKED' in total_spread_str:
                continue
            if bbo_price is None:
                continue
            try:
                if float(bbo_price) == 0:
                    continue
            except Exception:
                continue
            valid_records.append(r)

        if not valid_records:
            return None

        # Use team column as primary side matching (same as analyzer get_deltas_data)
        matching = []
        for r in valid_records:
            team_val = (r.get('team') or '')
            if league in ['ATP', 'WTA']:
                # For tennis allow partial both ways.
                if team_name.lower() in team_val.lower() or team_val.lower() in team_name.lower():
                    matching.append(r)
            else:
                if team_val.upper() == str(team_name).upper():
                    matching.append(r)

        if not matching:
            return None

        if relation == 'before':
            candidates = [r for r in matching if float(r.get('timestamp_ms') or 0) < fill_timestamp_ms]
            if not candidates:
                return None
            chosen = max(candidates, key=lambda x: float(x.get('timestamp_ms') or 0))
        else:
            candidates = [r for r in matching if float(r.get('timestamp_ms') or 0) >= after_min_ms]
            if not candidates:
                return None
            chosen = min(candidates, key=lambda x: float(x.get('timestamp_ms') or float('inf')))

        # Add opponent percentage at closest timestamp, like analyzer3
        opposing_team = chosen.get('opposing_team')
        if opposing_team:
            opp_candidates = [
                r for r in valid_records
                if (r.get('team') or '').upper() == str(opposing_team).upper()
            ]
            if opp_candidates:
                target_ts = float(chosen.get('timestamp_ms') or 0)
                closest_opp = min(opp_candidates, key=lambda r: abs(float(r.get('timestamp_ms') or 0) - target_ts))
                chosen['opponent_percentage'] = closest_opp.get('percentage')

        return _normalize_deltas_record(chosen, fill_timestamp_ms, relation)
    except Exception as e:
        print(f"⚠️  Error getting Pinnacle {relation}: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def get_pinnacle_before(market_slug: str, token_label: str, fill_timestamp_ms: int):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_pinnacle_bbo_sync, market_slug, token_label, fill_timestamp_ms, 'before')

async def get_pinnacle_after(market_slug: str, token_label: str, fill_timestamp_ms: int):
    # Keep bounded by semaphore similarly to DB-heavy after lookups
    async with db_semaphore:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _get_pinnacle_bbo_sync, market_slug, token_label, fill_timestamp_ms, 'after')

def _current_timestamp_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def _format_timestamp_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).strftime('%m/%d/%Y, %H:%M:%S')

def _safe_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

def _build_polymarket_clob_snapshot(token_id: str, book_data: Dict, timestamp_ms: int) -> Optional[Dict]:
    bids = book_data.get('bids') or []
    asks = book_data.get('asks') or []

    bid_prices = [_safe_float(bid.get('price')) for bid in bids]
    ask_prices = [_safe_float(ask.get('price')) for ask in asks]

    best_bid = max((price for price in bid_prices if price > 0), default=0.0)
    best_ask = min((price for price in ask_prices if price > 0), default=0.0)

    if best_bid <= 0 and best_ask <= 0:
        return None

    bbo = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else (best_bid or best_ask)
    spread = (best_ask - best_bid) if best_bid > 0 and best_ask > 0 else None

    return {
        'token_id': token_id,
        'bbo': bbo,
        'best_bid': best_bid,
        'best_ask': best_ask,
        'spread': round(spread, 4) if spread is not None else None,
        'timestamp': _format_timestamp_ms(timestamp_ms),
        'timestamp_ms': timestamp_ms,
        'source': 'clob_api'
    }

def _format_polymarket_snapshot_for_order(snapshot: Dict, fill_timestamp_ms: int, seconds_from_fill: Optional[float] = None) -> Dict:
    if seconds_from_fill is None:
        seconds_from_fill = (snapshot['timestamp_ms'] - fill_timestamp_ms) / 1000.0

    return {
        'bbo': snapshot['bbo'],
        'best_bid': snapshot['best_bid'],
        'best_ask': snapshot['best_ask'],
        'spread': snapshot['spread'],
        'seconds_from_fill': round(seconds_from_fill, 3),
        'timestamp': snapshot['timestamp'],
        'source': snapshot.get('source', 'clob_api')
    }

async def _fetch_polymarket_clob_price(session: aiohttp.ClientSession, token_id: str):
    try:
        async with session.get(CLOB_BOOK_URL, params={'token_id': token_id}) as response:
            if response.status != 200:
                print(f"⚠️  CLOB /book returned {response.status} for token_id={token_id}")
                return token_id, None

            book_data = await response.json(content_type=None)
            snapshot = _build_polymarket_clob_snapshot(token_id, book_data, _current_timestamp_ms())
            return token_id, snapshot
    except Exception as e:
        print(f"⚠️  Error fetching CLOB /book for token_id={token_id}: {e}")
        return token_id, None

async def fetch_polymarket_clob_prices(token_ids: List[str]) -> Dict[str, Dict]:
    unique_token_ids = []
    seen_token_ids = set()

    for token_id in token_ids:
        normalized_token_id = str(token_id or '').strip()
        if normalized_token_id and normalized_token_id not in seen_token_ids:
            seen_token_ids.add(normalized_token_id)
            unique_token_ids.append(normalized_token_id)

    if not unique_token_ids:
        return {}

    timeout = aiohttp.ClientTimeout(total=CLOB_REQUEST_TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=min(len(unique_token_ids), 20))

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        results = await asyncio.gather(
            *[_fetch_polymarket_clob_price(session, token_id) for token_id in unique_token_ids],
            return_exceptions=True
        )

    clob_prices = {}
    for result in results:
        if isinstance(result, Exception):
            print(f"⚠️  Unexpected error in CLOB batch fetch: {result}")
            continue

        token_id, snapshot = result
        if snapshot:
            clob_prices[token_id] = snapshot

    return clob_prices

async def get_polymarket_bbo_before(market_slug: str, token_label: str, fill_timestamp: int):
    """Get Polymarket Before BBO data - match by market_slug and token_label"""
    # Run database query in thread pool to avoid blocking
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_polymarket_bbo_before_sync, market_slug, token_label, fill_timestamp)

def _get_polymarket_bbo_before_sync(market_slug: str, token_label: str, fill_timestamp: int):
    """Synchronous version of get_polymarket_bbo_before"""
    conn = None
    cursor = None
    
    try:
        print(f"🔌 Connecting to database for Before query...")
        conn = get_poly_db_connection()
        if not conn:
            print(f"❌ Failed to connect to database")
            return None
        print(f"✅ Database connected")
        
        cursor = conn.cursor()
        
        fill_dt = datetime.fromtimestamp(fill_timestamp / 1000.0, tz=timezone.utc)
        start_dt = fill_dt - timedelta(minutes=5)
        fill_dt_utc = fill_dt.replace(tzinfo=timezone.utc)
        
        query = """
            SELECT 
                bbo,
                best_bid,
                best_ask,
                polymarket_timestamp,
                EXTRACT(EPOCH FROM polymarket_timestamp) * 1000 as timestamp_ms
            FROM market_data
            WHERE game_slug = %s
                AND LOWER(TRIM(outcome)) = %s
                AND polymarket_timestamp >= %s
                AND polymarket_timestamp <= %s
                AND bbo IS NOT NULL
                AND bbo > 0
            ORDER BY polymarket_timestamp DESC
            LIMIT 100
        """
        
        # Match by token_label (normalized to lowercase)
        token_label_normalized = token_label.strip().lower()
        print(f"🔍 Querying: game_slug='{market_slug}', outcome='{token_label_normalized}'")
        cursor.execute(query, (market_slug, token_label_normalized, start_dt, fill_dt_utc))
        records = cursor.fetchall()
        
        print(f"📊 Found {len(records)} records in time window")
        if not records:
            return None
        
        # Find closest before - convert Decimal to float for comparison
        before_records = [r for r in records if float(r['timestamp_ms']) < fill_timestamp]
        if not before_records:
            return None
        
        closest_before = max(before_records, key=lambda x: float(x['timestamp_ms']))
        timestamp_ms_float = float(closest_before['timestamp_ms'])
        best_bid = float(closest_before['best_bid']) if closest_before['best_bid'] else None
        best_ask = float(closest_before['best_ask']) if closest_before['best_ask'] else None
        bbo = float(closest_before['bbo'])
        
        # Calculate spread (absolute difference in dollars: best_ask - best_bid)
        spread = None
        if best_bid is not None and best_ask is not None:
            spread = best_ask - best_bid
            print(f"📊 Before Spread: best_ask={best_ask}, best_bid={best_bid}, spread={spread}")
        
        # Get timestamp string
        timestamp_str = None
        if closest_before.get('polymarket_timestamp'):
            if isinstance(closest_before['polymarket_timestamp'], datetime):
                timestamp_str = closest_before['polymarket_timestamp'].strftime('%m/%d/%Y, %H:%M:%S')
            else:
                try:
                    ts = datetime.fromisoformat(str(closest_before['polymarket_timestamp']).replace('Z', '+00:00'))
                    timestamp_str = ts.strftime('%m/%d/%Y, %H:%M:%S')
                except:
                    timestamp_str = str(closest_before['polymarket_timestamp'])
        
        return {
            'bbo': bbo,
            'best_bid': best_bid,
            'best_ask': best_ask,
            'spread': round(spread, 4) if spread is not None else None,
            'seconds_from_fill': round((fill_timestamp - timestamp_ms_float) / 1000.0, 3),
            'timestamp': timestamp_str,
            'source': 'database'
        }
        
    except Exception as e:
        print(f"⚠️  Error getting Polymarket Before: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def store_order_in_history(order_data: dict):
    """Store order in history, updating if exists"""
    global order_history
    
    # Keep memory healthy by pruning stale orders before each write
    cleanup_old_orders()
    
    order_id = f"{order_data.get('tx_hash')}_{order_data.get('log_index')}"
    
    # Check if order already exists
    existing_index = None
    for i, existing_order in enumerate(order_history):
        existing_id = f"{existing_order.get('tx_hash')}_{existing_order.get('log_index')}"
        if existing_id == order_id:
            existing_index = i
            break
    
    if existing_index is not None:
        # Update existing order - preserve all fields including PM data
        order_history[existing_index].update(order_data)
        # Debug: Log if PM data is present
        has_before = order_data.get('polymarket_before') is not None
        has_after = order_data.get('polymarket_after') is not None
        has_sportbook = order_data.get('sportbook') is not None
        if has_before or has_after or has_sportbook:
            print(f"💾 Updated order {order_id} in history - PM Before: {has_before}, PM After: {has_after}, Sportbook: {has_sportbook}")
    else:
        # Add new order
        order_history.append(order_data.copy())
        has_before = order_data.get('polymarket_before') is not None
        has_after = order_data.get('polymarket_after') is not None
        has_sportbook = order_data.get('sportbook') is not None
        if has_before or has_after or has_sportbook:
            print(f"💾 Added order {order_id} to history - PM Before: {has_before}, PM After: {has_after}, Sportbook: {has_sportbook}")
    
    # Sort by received/order time (newest first)
    order_history.sort(key=_get_order_time_for_retention, reverse=True)
    
    # Limit history size (keep last 4000 orders max)
    if len(order_history) > MAX_ORDER_HISTORY:
        order_history = order_history[:MAX_ORDER_HISTORY]

async def get_polymarket_bbo_after(market_slug: str, token_label: str, fill_timestamp: int):
    """Get Polymarket After BBO data - match by market_slug and token_label"""
    # Use semaphore to limit concurrent database connections
    async with db_semaphore:
        # Run database query in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _get_polymarket_bbo_after_sync, market_slug, token_label, fill_timestamp)

def _get_polymarket_bbo_after_sync(market_slug: str, token_label: str, fill_timestamp: int):
    """Synchronous version of get_polymarket_bbo_after"""
    conn = None
    cursor = None
    
    try:
        print(f"🔌 Connecting to database for After query...")
        conn = get_poly_db_connection()
        if not conn:
            print(f"❌ Failed to connect to database")
            return None
        print(f"✅ Database connected")
        
        cursor = conn.cursor()
        
        fill_dt = datetime.fromtimestamp(fill_timestamp / 1000.0, tz=timezone.utc)
        after_dt = fill_dt + timedelta(seconds=10)  # At least 10s after
        end_dt = fill_dt + timedelta(minutes=5)
        
        query = """
            SELECT 
                bbo,
                best_bid,
                best_ask,
                polymarket_timestamp,
                EXTRACT(EPOCH FROM polymarket_timestamp) * 1000 as timestamp_ms
            FROM market_data
            WHERE game_slug = %s
                AND LOWER(TRIM(outcome)) = %s
                AND polymarket_timestamp >= %s
                AND polymarket_timestamp <= %s
                AND bbo IS NOT NULL
                AND bbo > 0
            ORDER BY polymarket_timestamp ASC
            LIMIT 100
        """
        
        # Match by token_label (normalized to lowercase)
        token_label_normalized = token_label.strip().lower()
        print(f"🔍 Querying: game_slug='{market_slug}', outcome='{token_label_normalized}' (after 10s)")
        cursor.execute(query, (market_slug, token_label_normalized, after_dt, end_dt))
        records = cursor.fetchall()
        
        print(f"📊 Found {len(records)} records after 10s")
        if not records:
            return None
        
        # Get closest after (first record >= 10s) - convert Decimal to float
        closest_after = min(records, key=lambda x: float(x['timestamp_ms']))
        timestamp_ms_float = float(closest_after['timestamp_ms'])
        best_bid = float(closest_after['best_bid']) if closest_after['best_bid'] else None
        best_ask = float(closest_after['best_ask']) if closest_after['best_ask'] else None
        bbo = float(closest_after['bbo'])
        
        # Calculate spread (absolute difference in dollars: best_ask - best_bid)
        spread = None
        if best_bid is not None and best_ask is not None:
            spread = best_ask - best_bid
            print(f"📊 After Spread: best_ask={best_ask}, best_bid={best_bid}, spread={spread}")
        
        # Get timestamp string
        timestamp_str = None
        if closest_after.get('polymarket_timestamp'):
            if isinstance(closest_after['polymarket_timestamp'], datetime):
                timestamp_str = closest_after['polymarket_timestamp'].strftime('%m/%d/%Y, %H:%M:%S')
            else:
                try:
                    ts = datetime.fromisoformat(str(closest_after['polymarket_timestamp']).replace('Z', '+00:00'))
                    timestamp_str = ts.strftime('%m/%d/%Y, %H:%M:%S')
                except:
                    timestamp_str = str(closest_after['polymarket_timestamp'])
        
        return {
            'bbo': bbo,
            'best_bid': best_bid,
            'best_ask': best_ask,
            'spread': round(spread, 4) if spread is not None else None,
            'seconds_from_fill': round((timestamp_ms_float - fill_timestamp) / 1000.0, 3),
            'timestamp': timestamp_str,
            'source': 'database'
        }
        
    except Exception as e:
        print(f"⚠️  Error getting Polymarket After: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def process_order_batch(orders: List[Dict]):
    """Process a batch of orders in parallel"""
    tasks = []
    order_contexts = []
    clob_token_ids = []
    seen_token_ids = set()

    for order_data in orders:
        market_slug = order_data.get('market_slug')
        token_label = order_data.get('token_label', '').strip()
        token_id = str(order_data.get('token_id') or '').strip()
        timestamp = order_data.get('timestamp')

        if not timestamp:
            continue

        if not token_id and not (market_slug and token_label):
            continue

        timestamp_ms = int(timestamp * 1000)
        order_id = f"{order_data.get('tx_hash')}_{order_data.get('log_index')}"

        order_contexts.append({
            'order_id': order_id,
            'order_data': order_data,
            'market_slug': market_slug,
            'token_label': token_label,
            'token_id': token_id,
            'timestamp_ms': timestamp_ms
        })

        pending_updates[order_id] = {
            'order_data': order_data,
            'market_slug': market_slug,
            'token_label': token_label,
            'token_id': token_id,
            'timestamp_ms': timestamp_ms,
            'last_after_attempt_ms': None
        }

        if token_id and token_id not in seen_token_ids:
            seen_token_ids.add(token_id)
            clob_token_ids.append(token_id)

    clob_before = await fetch_polymarket_clob_prices(clob_token_ids)

    async def fetch_before_for_order(context: Dict):
        order_data = context['order_data']
        market_slug = context['market_slug']
        token_label = context['token_label']
        token_id = context['token_id']
        timestamp_ms = context['timestamp_ms']

        try:
            polymarket_before = None
            clob_snapshot = clob_before.get(token_id) if token_id else None

            if clob_snapshot:
                polymarket_before = _format_polymarket_snapshot_for_order(clob_snapshot, timestamp_ms)
            elif market_slug and token_label:
                polymarket_before = await get_polymarket_bbo_before(market_slug, token_label, timestamp_ms)

            order_data['polymarket_before'] = polymarket_before

            pinnacle_before = None
            if market_slug and token_label and should_run_pinnacle_matching(market_slug):
                pinnacle_before = await get_pinnacle_before(market_slug, token_label, timestamp_ms)

            order_data['pinnacle_before'] = pinnacle_before

            if polymarket_before or pinnacle_before:
                await sio.emit('order-update', order_data)
        except Exception as e:
            print(f"⚠️  Error fetching Before data for {order_data.get('tx_hash')}: {e}")

    for context in order_contexts:
        tasks.append(fetch_before_for_order(context))
    
    # Process all Before queries in parallel
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    
    # Broadcast all orders (with or without PM data)
    for order_data in orders:
        store_order_in_history(order_data)
        await sio.emit('order-update', order_data)

async def order_batch_processor():
    """Worker that processes orders in batches"""
    while True:
        batch = []
        try:
            # Collect orders for batch (wait up to BATCH_TIMEOUT seconds)
            try:
                first_order = await asyncio.wait_for(order_queue.get(), timeout=BATCH_TIMEOUT)
                batch.append(first_order)
            except asyncio.TimeoutError:
                continue
            
            # Collect more orders up to BATCH_SIZE
            while len(batch) < BATCH_SIZE:
                try:
                    order = await asyncio.wait_for(order_queue.get(), timeout=0.1)
                    batch.append(order)
                except asyncio.TimeoutError:
                    break
            
            if batch:
                print(f"📦 Processing batch of {len(batch)} orders")
                await process_order_batch(batch)
        except Exception as e:
            print(f"❌ Error in batch processor: {e}")
            import traceback
            traceback.print_exc()

def _update_after_derived_fields(order_data: Dict, polymarket_after: Optional[Dict], pinnacle_after: Optional[Dict]):
    fill_price = _safe_float(order_data.get('price'))
    sportbook_bid = polymarket_after.get('best_bid') if polymarket_after else None

    if sportbook_bid:
        price_diff = fill_price - sportbook_bid
        order_data['sportbook'] = {
            'best_bid': sportbook_bid,
            'fill_price': fill_price,
            'price_diff': price_diff,
            'price_diff_pct': (price_diff * 100) if sportbook_bid > 0 else 0
        }
    else:
        order_data['sportbook'] = None

    pinnacle_bid = pinnacle_after.get('bbo') if pinnacle_after else None
    if pinnacle_bid:
        pn_price_diff = fill_price - pinnacle_bid
        order_data['pinnacle_sportbook'] = {
            'best_bid': pinnacle_bid,
            'fill_price': fill_price,
            'price_diff': pn_price_diff,
            'price_diff_pct': (pn_price_diff * 100) if pinnacle_bid > 0 else 0
        }
    else:
        order_data['pinnacle_sportbook'] = None

async def _process_single_pending_after_update(order_id: str, update_info: Dict, age_seconds: float, clob_snapshot: Optional[Dict]):
    order_data = update_info['order_data']
    market_slug = update_info['market_slug']
    token_label = update_info['token_label']
    timestamp_ms = update_info['timestamp_ms']

    polymarket_after = order_data.get('polymarket_after')
    if polymarket_after is None:
        if clob_snapshot:
            polymarket_after = _format_polymarket_snapshot_for_order(
                clob_snapshot,
                timestamp_ms,
                seconds_from_fill=age_seconds
            )
        elif market_slug and token_label:
            polymarket_after = await get_polymarket_bbo_after(market_slug, token_label, timestamp_ms)

    needs_pinnacle = should_run_pinnacle_matching(market_slug)
    pinnacle_after = order_data.get('pinnacle_after')
    if pinnacle_after is None and needs_pinnacle and market_slug and token_label:
        pinnacle_after = await get_pinnacle_after(market_slug, token_label, timestamp_ms)

    pm_changed = polymarket_after is not None and order_data.get('polymarket_after') is None
    pn_changed = pinnacle_after is not None and order_data.get('pinnacle_after') is None

    if polymarket_after is not None:
        order_data['polymarket_after'] = polymarket_after
    if pinnacle_after is not None:
        order_data['pinnacle_after'] = pinnacle_after

    if pm_changed or pn_changed:
        _update_after_derived_fields(order_data, order_data.get('polymarket_after'), order_data.get('pinnacle_after'))
        store_order_in_history(order_data)
        await sio.emit('order-update', order_data)
        print(f"🔄 Updated After/Sportbook/Pinnacle for order {order_id} (age={round(age_seconds, 1)}s)")
    elif polymarket_after is None and pinnacle_after is None:
        print(f"⚠️  No After data found (PM/Pinnacle) for order {order_id} at age={round(age_seconds, 1)}s")

    has_pm_after = order_data.get('polymarket_after') is not None
    has_pn_after = order_data.get('pinnacle_after') is not None
    should_finalize = age_seconds >= AFTER_MAX_AGE_SECONDS or (has_pm_after and (not needs_pinnacle or has_pn_after))

    if should_finalize and order_id in pending_updates:
        del pending_updates[order_id]
        reason = "timeout reached" if age_seconds >= AFTER_MAX_AGE_SECONDS else "after data collected"
        print(f"✅ Completed updates for order {order_id} ({reason}, removed from pending)")

async def process_pending_polymarket_after_updates():
    if not pending_updates:
        return

    now_ms = _current_timestamp_ms()
    eligible_updates = []
    token_ids = []
    seen_token_ids = set()

    for order_id, update_info in list(pending_updates.items()):
        age_seconds = (now_ms - update_info['timestamp_ms']) / 1000.0
        if age_seconds < AFTER_MIN_AGE_SECONDS:
            continue

        last_after_attempt_ms = update_info.get('last_after_attempt_ms')
        if last_after_attempt_ms is not None and (now_ms - last_after_attempt_ms) < (AFTER_RETRY_SECONDS * 1000):
            continue

        update_info['last_after_attempt_ms'] = now_ms
        eligible_updates.append((order_id, update_info, age_seconds))

        token_id = update_info.get('token_id')
        if token_id and token_id not in seen_token_ids:
            seen_token_ids.add(token_id)
            token_ids.append(token_id)

    if not eligible_updates:
        return

    clob_after = await fetch_polymarket_clob_prices(token_ids)
    tasks = []

    for order_id, update_info, age_seconds in eligible_updates:
        token_id = update_info.get('token_id')
        clob_snapshot = clob_after.get(token_id) if token_id else None
        tasks.append(_process_single_pending_after_update(order_id, update_info, age_seconds, clob_snapshot))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def polymarket_after_collector():
    """Periodically fetch Polymarket After snapshots using CLOB first, then DB fallback."""
    while True:
        try:
            await process_pending_polymarket_after_updates()
        except Exception as e:
            print(f"❌ Error in Polymarket after collector: {e}")
        await asyncio.sleep(AFTER_COLLECTOR_POLL_SECONDS)

async def unsubscribe_all_dome_subscriptions(websocket):
    """Best-effort unsubscribe for all known Dome subscriptions."""
    if not websocket:
        return

    active_ids = list(subscriptions.keys())
    if not active_ids:
        return

    for subscription_id in active_ids:
        try:
            await websocket.send(json.dumps({
                "action": "unsubscribe",
                "subscription_id": subscription_id
            }))
        except Exception as e:
            print(f"⚠️  Unsubscribe failed for {subscription_id}: {e}")
            break

    subscriptions.clear()

async def mark_dome_disconnected():
    """Mark Dome connection as disconnected and attempt graceful unsubscribe/close."""
    global dome_ws, is_connected

    ws = dome_ws
    dome_ws = None
    if is_connected:
        is_connected = False
        await sio.emit('status', {'connected': False})

    if not ws:
        subscriptions.clear()
        return

    try:
        if not ws.closed:
            await unsubscribe_all_dome_subscriptions(ws)
            await ws.close()
    except Exception as e:
        print(f"⚠️  Error closing Dome websocket: {e}")
        subscriptions.clear()

async def connect_to_dome():
    """Connect to Dome API WebSocket"""
    global dome_ws, is_connected
    
    ws_url = f"wss://ws.domeapi.io/{DOME_API_KEY}"
    print(f"🔌 Connecting to Dome API: {ws_url.replace(DOME_API_KEY, '***')}")
    reconnect_delay = 1

    while True:
        try:
            async with websockets.connect(ws_url) as websocket:
                dome_ws = websocket
                is_connected = True
                reconnect_delay = 1
                subscriptions.clear()
                print("✅ Connected to Dome API WebSocket")
                
                # Notify frontend
                await sio.emit('status', {'connected': True})
                
                # Subscribe to wallets
                await subscribe_to_wallets(websocket)
                
                # Listen for messages
                async for message in websocket:
                    await handle_dome_message(message)
                    
        except websockets.exceptions.ConnectionClosed as e:
            print(f"🔌 Dome API connection closed ({e.code}: {e.reason}), reconnecting...")
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            await mark_dome_disconnected()
        
        # Exponential backoff for retries
        print(f"🔁 Reconnecting to Dome in {reconnect_delay}s...")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 30)

async def subscribe_to_wallets(websocket):
    """Subscribe to wallet addresses"""
    subscribe_message = {
        "action": "subscribe",
        "platform": PLATFORM,
        "version": WS_VERSION,
        "type": "orders",
        "filters": {
            "users": WALLET_ADDRESSES
        }
    }
    
    print(f"📡 Subscribing to {len(WALLET_ADDRESSES)} wallet(s)")
    await websocket.send(json.dumps(subscribe_message))

async def handle_dome_message(message: str):
    """Handle incoming messages from Dome API"""
    try:
        data = json.loads(message)
        
        # Handle subscription acknowledgment
        if data.get('type') == 'ack' and data.get('subscription_id'):
            subscription_id = data['subscription_id']
            subscriptions[subscription_id] = {
                'created_at': datetime.now(timezone.utc).isoformat(),
                'platform': PLATFORM
            }
            print(f"✅ Subscription acknowledged: {subscription_id}")
        
        # Handle order updates
        if data.get('type') == 'event' and data.get('subscription_id') and data.get('data'):
            order_data = data['data']
            order_data['subscription_id'] = data['subscription_id']
            order_data['received_at'] = datetime.now(timezone.utc).isoformat()

            await backfill_order_metadata_from_token_id(order_data)
            
            print(f"📊 Order update: {order_data.get('side')} {order_data.get('token_label')} @ ${order_data.get('price')}")
            
            # Initialize Polymarket fields to None (will be populated if matching succeeds)
            order_data['polymarket_before'] = None
            order_data['polymarket_after'] = None
            order_data['sportbook'] = None
            order_data['pinnacle_before'] = None
            order_data['pinnacle_after'] = None
            order_data['pinnacle_sportbook'] = None
            
            # Add to queue for batch processing
            if order_queue:
                await order_queue.put(order_data)
            else:
                # Fallback if queue not initialized (shouldn't happen)
                store_order_in_history(order_data)
                await sio.emit('order-update', order_data)
            
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing message: {e}")
    except Exception as e:
        print(f"❌ Error handling message: {e}")

if __name__ == "__main__":
    import uvicorn
    
    # Check if port is available
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', BACKEND_PORT))
    sock.close()
    
    if result == 0:
        print(f"❌ Port {BACKEND_PORT} is already in use!")
        print(f"💡 Try one of these:")
        print(f"   1. Kill the process: kill -9 $(lsof -ti:{BACKEND_PORT})")
        print(f"   2. Use a different port: BACKEND_PORT=8001 python main.py")
        sys.exit(1)
    
    try:
        uvicorn.run(
            socketio_app,
            host="0.0.0.0",
            port=BACKEND_PORT,
            log_level="info"
        )
    except OSError as e:
        if e.errno == 48:  # Address already in use
            print(f"❌ Port {BACKEND_PORT} is already in use!")
            print(f"💡 Try: kill -9 $(lsof -ti:{BACKEND_PORT})")
        else:
            raise
