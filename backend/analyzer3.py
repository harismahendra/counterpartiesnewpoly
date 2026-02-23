import os
import json
import requests
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from flask import Flask, jsonify, request, Response, stream_with_context
from flask_cors import CORS
from dotenv import load_dotenv
from urllib.parse import urlparse
import psycopg2
from psycopg2.extras import RealDictCursor
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()  # Load environment variables from .env file

app = Flask(__name__)
CORS(app)

def get_db_connection():
    """Create PostgreSQL database connection for cache (uses DATABASE_URL)"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        # Fallback to DATABASE_URL_POLY if DATABASE_URL not available
        database_url = os.getenv('DATABASE_URL_POLY')
        if not database_url:
            raise ValueError("DATABASE_URL or DATABASE_URL_POLY not found in environment variables")
    
    # Parse the DATABASE_URL
    result = urlparse(database_url)
    
    connection = psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port,
        connect_timeout=10,  # Connection timeout in seconds
        cursor_factory=RealDictCursor,  # Returns results as dictionaries
        keepalives=1,  # Enable TCP keepalives
        keepalives_idle=30,  # Seconds before sending keepalive
        keepalives_interval=10,  # Seconds between keepalives
        keepalives_count=5  # Number of keepalives before considering connection dead
    )
    return connection

def get_poly_db_connection():
    """Create PostgreSQL database connection for Polymarket BBO (uses DATABASE_URL_POLY)"""
    database_url = os.getenv('DATABASE_URL_POLY')
    if not database_url:
        raise ValueError("DATABASE_URL_POLY not found in environment variables")
    
    # Parse the DATABASE_URL_POLY
    result = urlparse(database_url)
    
    # Handle Render's SSL requirement
    connection = psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port,
        connect_timeout=10,  # Connection timeout in seconds
        cursor_factory=RealDictCursor,  # Returns results as dictionaries
        keepalives=1,  # Enable TCP keepalives
        keepalives_idle=30,  # Seconds before sending keepalive
        keepalives_interval=10,  # Seconds between keepalives
        keepalives_count=5  # Number of keepalives before considering connection dead
    )
    return connection

class TradeFeedDataLoader:
    """Loader for FILL data from Trade Feed API"""
    def __init__(self):
        """Initialize Trade Feed API connection"""
        self.base_url = "https://t-api-production.fly.dev/api"
        self.email = os.getenv("EMAIL")
        self.password = os.getenv("PASSWORD")
        self.token = os.getenv("BEARER_TOKEN")
        
        if not self.email and not self.password and not self.token:
            raise ValueError("Either EMAIL+PASSWORD or BEARER_TOKEN must be set in .env file")
    
    def login(self):
        """Login to get authentication token"""
        if not self.email or not self.password:
            print("No EMAIL/PASSWORD in .env, using existing token")
            return self.token is not None
        
        endpoint = "/auth/sign_in"
        login_data = {
            "email": self.email,
            "password": self.password
        }
        
        auth_base_url = "https://t-api-production.fly.dev"
        
        try:
            response = requests.post(
                f"{auth_base_url}{endpoint}",
                json=login_data,
                headers={
                    "Content-Type": "application/json",
                    "accept": "application/json, text/plain, */*",
                    "origin": "https://d2cniso3hkia3k.cloudfront.net",
                    "referer": "https://d2cniso3hkia3k.cloudfront.net/"
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                self.token = (data.get("token") or 
                            data.get("access_token") or 
                            data.get("jwt") or
                            data.get("auth_token") or
                            data.get("data", {}).get("token"))
                
                if self.token:
                    print("Login successful, token obtained")
                    return True
                else:
                    print("Login successful but couldn't find token in response")
                    return False
            else:
                print(f"Login failed with status {response.status_code}")
                return False
                
        except Exception as e:
            print(f"Login error: {e}")
            return False
    
    def get_headers(self):
        return {
            "accept": "application/json, text/plain, */*",
            "authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def fetch_trades(self, page_size=500, total_trades=3000, start_time=None, end_time=None, last_internal_id=None):
        """Fetch trades with pagination - fetches 500 trades per page until we have total_trades
        Args:
            page_size: Number of trades per page
            total_trades: Maximum number of trades to fetch
            start_time: Optional start time filter
            end_time: Optional end time filter
            last_internal_id: If provided, only fetch trades with id > last_internal_id (for incremental fetching)
        """
        if not self.token:
            if not self.login():
                return []
        
        all_trades = []
        page = 1
        max_pages = (total_trades + page_size - 1) // page_size  # Calculate number of pages needed
        
        params = {
            "page_size": page_size,
            "sort_by": "id:desc"
        }
        
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        
        if last_internal_id:
            print(f"📊 Fetching new trades after internal_id={last_internal_id} (max {total_trades} trades)...")
        else:
            print(f"📊 Fetching {total_trades} trades (500 per page, {max_pages} pages)...")
        
        while len(all_trades) < total_trades and page <= max_pages:
            params["page"] = page
            
            try:
                print(f"  📄 Fetching page {page}/{max_pages}...", flush=True)
                response = requests.get(
                    f"{self.base_url}/trade_feed/order_fills",
                    headers=self.get_headers(),
                    params=params
                )
                
                response.raise_for_status()
                data = response.json()
                
                # Debug: print response structure
                if page == 1:
                    print(f"  🔍 API Response keys: {list(data.keys())}", flush=True)
                
                trades = data.get("entries", [])
                
                if not trades:
                    print(f"  ⚠️  No more trades available (page {page} returned empty)")
                    break
                
                # If last_internal_id is provided, filter to only include new trades
                if last_internal_id:
                    new_trades = []
                    for trade in trades:
                        trade_id = trade.get("id") or trade.get("internal_id")
                        if trade_id and trade_id > last_internal_id:
                            new_trades.append(trade)
                        elif trade_id == last_internal_id:
                            # Same timestamp but different internal_id - include it
                            new_trades.append(trade)
                        else:
                            # We've reached trades older than last_internal_id, stop fetching
                            print(f"  ✅ Reached last_internal_id={last_internal_id}, stopping incremental fetch")
                            all_trades.extend(new_trades)
                            break
                    
                    if new_trades:
                        all_trades.extend(new_trades)
                        print(f"  ✅ Page {page}: Got {len(new_trades)} new trades (total: {len(all_trades)})", flush=True)
                    else:
                        print(f"  ✅ No new trades on page {page}, stopping")
                        break
                else:
                    all_trades.extend(trades)
                    print(f"  ✅ Page {page}: Got {len(trades)} trades (total: {len(all_trades)})", flush=True)
                
                if len(all_trades) >= total_trades:
                    all_trades = all_trades[:total_trades]
                    print(f"  ✅ Reached target of {total_trades} trades")
                    break
                
                # If we got fewer trades than page_size, we've reached the end
                if len(trades) < page_size:
                    print(f"  ✅ Reached end of available trades (got {len(trades)} < {page_size})")
                    break
                
                page += 1
                
            except requests.exceptions.HTTPError as e:
                print(f"  ❌ HTTP Error on page {page}: {e}")
                break
            except Exception as e:
                print(f"  ❌ Error on page {page}: {e}")
                break
        
        print(f"✅ Fetched {len(all_trades)} trades from Trade Feed API")
        return all_trades
    
    def extract_fill_data(self, trade):
        """Extract fill data from trade - returns both formatted and full trade data"""
        try:
            # Safely get trading_pair - handle None values
            trading_pair = trade.get("trading_pair") or {}
            primary_instrument = trading_pair.get("primary_instrument") or {}
            boltodds_game = primary_instrument.get("boltodds_game") or {}
            opticodds_game = primary_instrument.get("opticodds_game") or {}
            
            # Extract game slug from polymarket_exchange.market_slug
            game_slug = ""
            polymarket_exchange = trading_pair.get("polymarket_exchange") or {}
            if not polymarket_exchange:
                # Try inside primary_instrument
                polymarket_exchange = primary_instrument.get("polymarket_exchange") or {}
            
            market_slug = polymarket_exchange.get("market_slug", "")
            if market_slug:
                game_slug = market_slug
            
            # Parse timestamp
            fill_tx_ts = trade.get("fill_tx_ts", "")
            
            date_str = ""
            time_str = ""
            order_submission_str = ""
            
            if fill_tx_ts:
                try:
                    # Parse ISO timestamp - assume it's in UTC+0
                    if 'Z' in fill_tx_ts:
                        dt_utc = datetime.fromisoformat(fill_tx_ts.replace('Z', '+00:00'))
                    elif '+' in fill_tx_ts or fill_tx_ts.count('-') > 2:
                        # Already has timezone info
                        dt_utc = datetime.fromisoformat(fill_tx_ts)
                    else:
                        # No timezone info, assume UTC
                        dt_naive = datetime.fromisoformat(fill_tx_ts)
                        dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                    
                    # Convert from UTC to UTC+8
                    utc8 = timezone(timedelta(hours=8))
                    dt_utc8 = dt_utc.astimezone(utc8)
                    
                    # Format as M/D/YYYY
                    date_str = f"{dt_utc8.month}/{dt_utc8.day}/{dt_utc8.year}"
                    
                    # Format as H:M:S.mmm (with zero padding)
                    microseconds = dt_utc8.microsecond
                    milliseconds = microseconds // 1000
                    time_str = f"{dt_utc8.hour:02d}:{dt_utc8.minute:02d}:{dt_utc8.second:02d}.{milliseconds:03d}"
                    
                except Exception as e:
                    print(f"Error parsing timestamp {fill_tx_ts}: {e}")
            
            # Parse rest_start_bot_ts for Order submission
            rest_start_bot_ts = trade.get("rest_start_bot_ts")
            if rest_start_bot_ts:
                try:
                    # Ensure it's a string
                    rest_start_bot_ts = str(rest_start_bot_ts).strip()
                    
                    if rest_start_bot_ts:
                        # Parse ISO timestamp - assume it's in UTC+0
                        if 'Z' in rest_start_bot_ts:
                            dt_utc = datetime.fromisoformat(rest_start_bot_ts.replace('Z', '+00:00'))
                        elif '+' in rest_start_bot_ts or (rest_start_bot_ts.count('-') > 2 and 'T' in rest_start_bot_ts):
                            # Already has timezone info or is ISO format with timezone
                            try:
                                dt_utc = datetime.fromisoformat(rest_start_bot_ts)
                                if dt_utc.tzinfo is None:
                                    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                            except ValueError:
                                # Try parsing without timezone and assume UTC
                                dt_naive = datetime.fromisoformat(rest_start_bot_ts)
                                dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                        else:
                            # No timezone info, assume UTC
                            dt_naive = datetime.fromisoformat(rest_start_bot_ts)
                            dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                        
                        # Convert from UTC to UTC+8
                        utc8 = timezone(timedelta(hours=8))
                        dt_utc8 = dt_utc.astimezone(utc8)
                        
                        # Format as H:M:S.mmm (same format as Fill Time)
                        microseconds = dt_utc8.microsecond
                        milliseconds = microseconds // 1000
                        order_submission_str = f"{dt_utc8.hour:02d}:{dt_utc8.minute:02d}:{dt_utc8.second:02d}.{milliseconds:03d}"
                    
                except Exception as e:
                    print(f"Error parsing rest_start_bot_ts '{rest_start_bot_ts}': {e}")
            
            # Extract side (team name)
            side = trade.get("side", "")
            
            # Extract league/sport from trading_pair.polymarket_exchange.market_slug
            # Extract the word before the first dash (e.g., "nba-was-lac-2026-01-14" -> "nba")
            league = ""
            
            # First try: Get from polymarket_exchange.market_slug (primary method)
            polymarket_exchange = trading_pair.get("polymarket_exchange") or {}
            if not polymarket_exchange:
                # Try inside primary_instrument
                polymarket_exchange = primary_instrument.get("polymarket_exchange") or {}
            
            market_slug = polymarket_exchange.get("market_slug", "")
            if market_slug:
                # Extract word before first dash
                parts = market_slug.split("-")
                if parts:
                    league = parts[0].upper()  # Convert to uppercase (e.g., "nba" -> "NBA")
            
            # Fallback to boltodds_game if market_slug extraction fails
            if not league and boltodds_game:
                league = boltodds_game.get("sport", "") or boltodds_game.get("league", "")
            
            # Additional fallback to opticodds_game
            if not league and opticodds_game:
                league = opticodds_game.get("league", "") or opticodds_game.get("sport", "")
            
            # Extract fill price - handle None values
            last_fill_price = trade.get("last_fill_price") or {}
            fill_price = last_fill_price.get("amount", "") if isinstance(last_fill_price, dict) else ""
            
            # Extract fill value - handle None values
            last_fill_value = trade.get("last_fill_value") or {}
            fill_value = last_fill_value.get("amount", "") if isinstance(last_fill_value, dict) else ""
            
            # Extract implied_price and price_before_max_individual from placement_logic
            # Priority: placement_logic > top level of trade
            implied_price = None
            price_before_max_individual = None
            
            placement_logic = trade.get("placement_logic") or {}
            if placement_logic:
                if "implied_price" in placement_logic:
                    implied_price = placement_logic["implied_price"]
                if "price_before_max_individual" in placement_logic:
                    price_before_max_individual = placement_logic["price_before_max_individual"]
            
            # Fallback to top level if not in placement_logic
            if implied_price is None:
                implied_price = trade.get("implied_price")
            if price_before_max_individual is None:
                price_before_max_individual = trade.get("price_before_max_individual")
            
            # Calculate timestamp_ms for PolymarketBBO matching
            timestamp_ms = None
            if fill_tx_ts:
                try:
                    if 'Z' in fill_tx_ts:
                        dt_utc = datetime.fromisoformat(fill_tx_ts.replace('Z', '+00:00'))
                    elif '+' in fill_tx_ts or fill_tx_ts.count('-') > 2:
                        dt_utc = datetime.fromisoformat(fill_tx_ts)
                    else:
                        dt_naive = datetime.fromisoformat(fill_tx_ts)
                        dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                    timestamp_ms = int(dt_utc.timestamp() * 1000)
                except Exception as e:
                    print(f"Error calculating timestamp_ms: {e}")
            
            return {
                "Date": date_str,
                "Order submission": order_submission_str,
                "Fill Time": time_str,
                "Side": side,
                "League": league,
                "Game Slug": game_slug,
                "Fill Price": fill_price,
                "Implied Price": implied_price,
                "Price Before Max Individual": price_before_max_individual,
                "Fill Value": fill_value,
                "fill_tx_ts": fill_tx_ts,
                "timestamp_ms": timestamp_ms,  # For PolymarketBBO matching
                "full_trade_data": trade  # Include full trade data for expandable rows
            }
        except Exception as e:
            print(f"Error extracting fill data: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def read_fill_data(self, limit=3000, hours_back=None, progress_callback=None, last_internal_id=None) -> List[Dict]:
        """Read fill data from Trade Feed API and return with full trade data
        Args:
            limit: Maximum number of trades to fetch (default 3000)
            hours_back: Optional hours to filter by. If None, no time filtering is applied.
            progress_callback: Optional callback function to report progress (receives dict with progress info)
            last_internal_id: If provided, only fetch trades with id > last_internal_id (for incremental fetching)
        """
        read_start = time.time()
        try:
            # Calculate 3-minute cutoff to avoid processing fresh data without BBO records
            analyze_start_time = datetime.now(timezone.utc)
            three_minutes_ago = analyze_start_time - timedelta(minutes=3)
            three_minutes_ago_ts = int(three_minutes_ago.timestamp() * 1000)
            print(f"⏰ Analysis started at: {analyze_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print(f"⏰ Filtering out data newer than 3 minutes: {three_minutes_ago.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            
            fetch_start = time.time()
            # Fetch trades - use incremental fetching if last_internal_id is provided
            trades = self.fetch_trades(page_size=500, total_trades=limit, last_internal_id=last_internal_id)
            fetch_time = time.time() - fetch_start
            print(f"⏱️  fetch_trades(): {fetch_time:.2f}s ({len(trades)} trades)")
            
            fill_data = []
            filtered_out_count = 0
            filtered_fresh_count = 0
            
            # Only apply time filtering if hours_back is specified
            cutoff_time = None
            if hours_back:
                now_utc = datetime.now(timezone.utc)
                cutoff_time = now_utc - timedelta(hours=hours_back)
                print(f"📅 Filtering trades to last {hours_back} hours. Cutoff time (UTC): {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"📅 No time filtering - returning all {len(trades)} fetched trades")
            
            for idx, trade in enumerate(trades):
                extracted = self.extract_fill_data(trade)
                if not extracted:
                    if idx < 5:  # Only log first 5 failures to avoid spam
                        print(f"  ⚠️  Trade {idx}: extract_fill_data returned None")
                    continue
                
                # Filter out data newer than 3 minutes (to avoid processing data without BBO records)
                timestamp_ms = extracted.get("timestamp_ms")
                if timestamp_ms and timestamp_ms > three_minutes_ago_ts:
                    filtered_fresh_count += 1
                    if filtered_fresh_count <= 5:  # Only log first 5
                        fresh_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                        age_seconds = (analyze_start_time - fresh_time).total_seconds()
                        print(f"  ⏰ Filtered fresh data: {fresh_time.strftime('%Y-%m-%d %H:%M:%S')} UTC ({age_seconds:.1f}s old, too fresh for BBO matching)")
                    continue
                
                # Include trade even if Date/Fill Time are missing (they might be empty strings)
                # Apply time filtering only if cutoff_time is set AND we have valid date/time
                if cutoff_time and extracted.get("Date") and extracted.get("Fill Time"):
                        try:
                            date_str = extracted.get("Date")  # M/D/YYYY
                            time_str = extracted.get("Fill Time")  # H:M:S.mmm
                            
                            # Parse date
                            month, day, year = map(int, date_str.split('/'))
                            
                            # Parse time (handle decimal seconds)
                            if '.' in time_str:
                                time_main, ms_str = time_str.split('.')
                                time_parts = time_main.split(':')
                                hours = int(time_parts[0])
                                minutes = int(time_parts[1])
                                seconds = int(time_parts[2])
                                milliseconds = int(ms_str.ljust(3, '0')[:3])
                            else:
                                time_parts = time_str.split(':')
                                hours = int(time_parts[0])
                                minutes = int(time_parts[1])
                                seconds = int(time_parts[2])
                                milliseconds = 0
                            
                            # Create datetime in UTC+8 (as per existing logic)
                            utc8 = timezone(timedelta(hours=8))
                            dt_utc8 = datetime(year, month, day, hours, minutes, seconds, 
                                             microsecond=milliseconds*1000, tzinfo=utc8)
                            dt_utc = dt_utc8.astimezone(timezone.utc)
                            
                            if dt_utc >= cutoff_time:
                                fill_data.append(extracted)
                            else:
                                filtered_out_count += 1
                                age_hours = (now_utc - dt_utc).total_seconds() / 3600
                                print(f"  ✗ Filtered: {extracted.get('Side')} - {extracted.get('Date')} {extracted.get('Fill Time')} ({age_hours:.1f}h old)")
                        except Exception as parse_error:
                            print(f"  ⚠ Warning: Could not parse date/time for filtering: {parse_error}")
                            fill_data.append(extracted)
                else:
                    # No time filtering OR missing date/time - include all trades
                    fill_data.append(extracted)
            
            # Match PolymarketBBO data if available - load by market_slug to reduce memory usage
            try:
                if fill_data:
                    # Group fills by market_slug (Game Slug) for more specific grouping
                    fills_by_market_slug = {}
                    for fill in fill_data:
                        market_slug = fill.get("Game Slug", "").strip()
                        if market_slug:
                            if market_slug not in fills_by_market_slug:
                                fills_by_market_slug[market_slug] = []
                            fills_by_market_slug[market_slug].append(fill)
                    
                    if fills_by_market_slug:
                        total_market_slugs = len(fills_by_market_slug)
                        print(f"📊 Matching {len(fill_data)} fills with Polymarket BBO data (grouped by {total_market_slugs} market slugs)...")
                        max_workers = min(os.cpu_count() or 4, total_market_slugs, 8)  # Limit to 8 workers max
                        print(f"🚀 Using parallel processing with {max_workers} workers")
                        total_matched_count = 0
                        
                        # Prepare tasks for parallel processing
                        tasks = [(idx, slug, fills, total_market_slugs) for idx, (slug, fills) in enumerate(fills_by_market_slug.items(), 1)]
                        
                        # Process market slugs in parallel using ThreadPoolExecutor
                        completed_count = 0
                        results = []
                        
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            # Submit all tasks
                            future_to_task = {executor.submit(process_single_market_slug_parallel, task): task for task in tasks}
                            
                            # Process completed tasks as they finish
                            for future in as_completed(future_to_task):
                                completed_count += 1
                                try:
                                    result = future.result()
                                    market_slug_index, market_slug, slug_fills, slug_matched_count, loaded_records, message = result
                                    
                                    # Update total matched count
                                    total_matched_count += slug_matched_count
                                    
                                    # Store result for later (to maintain order if needed)
                                    results.append(result)
                                    
                                    # Print result
                                    print(f"  [{completed_count}/{total_market_slugs}] {message}")
                                    
                                    # Send progress update via callback if provided
                                    if progress_callback:
                                        try:
                                            display_slug = market_slug[:50] + "..." if len(market_slug) > 50 else market_slug
                                            progress_callback({
                                                'type': 'progress',
                                                'current': completed_count,
                                                'total': total_market_slugs,
                                                'market_slug': display_slug,
                                                'fills_count': len(slug_fills),
                                                'message': f'Processing {completed_count}/{total_market_slugs}: {display_slug} (parallel)'
                                            })
                                        except Exception as e:
                                            print(f"  ⚠️  Error sending progress update: {e}")
                                    
                                except Exception as e:
                                    task = future_to_task[future]
                                    print(f"  ❌ Error processing market_slug {task[1]}: {e}")
                                    import traceback
                                    traceback.print_exc()
                        
                        print(f"✅ Polymarket BBO matching complete: {total_matched_count}/{len(fill_data)} fills matched across all market slugs")
                    else:
                        print(f"⚠️  No market slugs found in fill data, skipping Polymarket BBO matching")
            except Exception as e:
                print(f"⚠️  Error matching Polymarket BBO data: {e}")
                import traceback
                traceback.print_exc()
            
            # Match Deltas (Pinnacle) data if available - parallel processing by slug
            try:
                if fill_data:
                    # Group fills by market_slug (same grouping as Polymarket BBO)
                    fills_by_market_slug = {}
                    for fill in fill_data:
                        market_slug = fill.get("Game Slug", "").strip()
                        if market_slug:
                            if market_slug not in fills_by_market_slug:
                                fills_by_market_slug[market_slug] = []
                            fills_by_market_slug[market_slug].append(fill)
                    
                    if fills_by_market_slug:
                        total_market_slugs = len(fills_by_market_slug)
                        print(f"📊 Matching {len(fill_data)} fills with Deltas data (grouped by {total_market_slugs} market slugs)...")
                        max_workers = min(os.cpu_count() or 4, total_market_slugs, 8)
                        print(f"🚀 Using parallel processing with {max_workers} workers for Deltas")
                        total_deltas_matched = 0
                        
                        # Prepare tasks for parallel processing
                        tasks = [(idx, slug, fills, total_market_slugs) 
                                for idx, (slug, fills) in enumerate(fills_by_market_slug.items(), 1)]
                        
                        # Process market slugs in parallel
                        completed_count = 0
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            future_to_task = {
                                executor.submit(process_single_slug_deltas_parallel, task): task 
                                for task in tasks
                            }
                            
                            for future in as_completed(future_to_task):
                                completed_count += 1
                                try:
                                    result = future.result()
                                    market_slug_index, market_slug, slug_fills, slug_matched_count, loaded_records, message = result
                                    total_deltas_matched += slug_matched_count
                                    print(f"  [{completed_count}/{total_market_slugs}] {message}")
                                    
                                    if progress_callback:
                                        try:
                                            display_slug = market_slug[:50] + "..." if len(market_slug) > 50 else market_slug
                                            progress_callback({
                                                'type': 'progress',
                                                'current': completed_count,
                                                'total': total_market_slugs,
                                                'market_slug': display_slug,
                                                'fills_count': len(slug_fills),
                                                'message': f'Processing Deltas {completed_count}/{total_market_slugs}: {display_slug}'
                                            })
                                        except Exception as e:
                                            print(f"  ⚠️  Error sending progress update: {e}")
                                except Exception as e:
                                    task = future_to_task[future]
                                    print(f"  ❌ Error processing Deltas for market_slug {task[1]}: {e}")
                                    import traceback
                                    traceback.print_exc()
                        
                        print(f"✅ Deltas matching complete: {total_deltas_matched}/{len(fill_data)} fills matched")
                    else:
                        print(f"⚠️  No market slugs found, skipping Deltas matching")
            except Exception as e:
                print(f"⚠️  Error matching Deltas data: {e}")
                import traceback
                traceback.print_exc()
            
            total_time = time.time() - read_start
            failed_extractions = len(trades) - len(fill_data) - filtered_out_count - filtered_fresh_count
            print(f"✓ Successfully converted {len(fill_data)} trades to fill data format")
            print(f"📊 Summary: {len(trades)} total trades fetched, {len(fill_data)} extracted successfully")
            if failed_extractions > 0:
                print(f"⚠️  {failed_extractions} trades failed extraction (returned None)")
            if filtered_fresh_count > 0:
                print(f"⏰ Filtered out {filtered_fresh_count} trades newer than 3 minutes (too fresh for BBO matching)")
            if cutoff_time:
                print(f"✗ Filtered out {filtered_out_count} trades older than {hours_back} hours")
            print(f"⏱️  TOTAL read_fill_data(): {total_time:.2f}s")
            return fill_data
            
        except Exception as e:
            print(f"Error reading fill data from Trade Feed API: {e}")
            return []

def process_single_market_slug_parallel(args):
    """Helper function to process a single market_slug in parallel (for ThreadPoolExecutor)"""
    market_slug_index, market_slug, slug_fills, total_market_slugs = args
    
    # Extract sport from market_slug (first part before dash)
    sport = ""
    if market_slug:
        parts = market_slug.split("-")
        if parts:
            sport = parts[0].upper()
    
    if not sport:
        # Fallback to League if market_slug doesn't have sport
        sport = slug_fills[0].get("League", "").strip().upper() if slug_fills else ""
    
    if not sport:
        return (market_slug_index, market_slug, slug_fills, 0, 0, f"⚠️  Skipped {market_slug} (no sport)")
    
    # Calculate time range for this market_slug only
    timestamps = [f.get("timestamp_ms") for f in slug_fills if f.get("timestamp_ms")]
    if not timestamps:
        return (market_slug_index, market_slug, slug_fills, 0, 0, f"⚠️  Skipped {market_slug} (no timestamps)")
    
    min_timestamp = min(timestamps)
    max_timestamp = max(timestamps)
    # Add buffer: 5 minutes before and after
    buffer_ms = 5 * 60 * 1000
    start_timestamp = min_timestamp - buffer_ms
    end_timestamp = max_timestamp + buffer_ms
    
    # Truncate market_slug for display if too long
    display_slug = market_slug[:50] + "..." if len(market_slug) > 50 else market_slug
    
    # Load data for this market_slug (game_slug) - much more specific!
    polymarket_bbo_processor = PolymarketBBOProcessor()
    if polymarket_bbo_processor.load_data(start_timestamp, end_timestamp, market_slugs=[market_slug]):
        slug_matched_count = 0
        # Match fills for this market_slug
        for fill in slug_fills:
            outcome = fill.get("Side", "").strip().lower()
            timestamp_ms = fill.get("timestamp_ms")
            
            if outcome and timestamp_ms:
                # Match by game_slug (market_slug) and outcome - this is the correct matching!
                polymarket_data = polymarket_bbo_processor.get_polymarket_bbo_data(
                    game_slug=market_slug,  # Match by market_slug, not sport!
                    outcome=outcome,
                    fill_timestamp=timestamp_ms
                )
                fill["polymarket_bbo"] = polymarket_data
                if polymarket_data.get('polymarket_bbo_before') or polymarket_data.get('polymarket_bbo_after'):
                    slug_matched_count += 1
        
        loaded_records = len(polymarket_bbo_processor.processed_data)
        return (market_slug_index, market_slug, slug_fills, slug_matched_count, loaded_records, 
               f"✅ {sport} ({display_slug}): {slug_matched_count}/{len(slug_fills)} fills matched (loaded {loaded_records} records)")
    else:
        return (market_slug_index, market_slug, slug_fills, 0, 0, 
               f"⚠️  Failed to load Polymarket BBO data for {sport} ({display_slug})")

def process_single_slug_deltas_parallel(args):
    """
    Process Deltas matching for a single market_slug in parallel.
    
    For each slug:
    1. Extract league and both teams from fills
    2. Filter deltas database by league and both teams
    3. Match each fill by team, league, and timestamp
    """
    market_slug_index, market_slug, slug_fills, total_market_slugs = args
    
    # Extract sport/league from market_slug
    sport = ""
    if market_slug:
        parts = market_slug.split("-")
        if parts:
            sport = parts[0].upper()
    
    if not sport:
        # Fallback to League field
        sport = slug_fills[0].get("League", "").strip().upper() if slug_fills else ""
    
    if not sport:
        return (market_slug_index, market_slug, slug_fills, 0, 0, 
               f"⚠️  Skipped {market_slug} (no sport)")
    
    # Calculate time range for this slug
    timestamps = [f.get("timestamp_ms") for f in slug_fills if f.get("timestamp_ms")]
    if not timestamps:
        return (market_slug_index, market_slug, slug_fills, 0, 0, 
               f"⚠️  Skipped {market_slug} (no timestamps)")
    
    min_timestamp = min(timestamps)
    max_timestamp = max(timestamps)
    # Add buffer: 5 minutes before and after
    buffer_ms = 5 * 60 * 1000
    start_timestamp = min_timestamp - buffer_ms
    end_timestamp = max_timestamp + buffer_ms
    
    # Extract unique teams from fills in this slug
    # Convert fill Side to Deltas team abbreviations
    deltas_processor = DeltasProcessor()
    unique_team_abbrs = set()
    
    for fill in slug_fills:
        fill_side = fill.get("Side", "").strip()
        team_abbr = deltas_processor.get_deltas_team_name(fill_side, sport)
        if team_abbr:
            unique_team_abbrs.add(team_abbr)
    
    if not unique_team_abbrs:
        display_slug = market_slug[:50] + "..." if len(market_slug) > 50 else market_slug
        return (market_slug_index, market_slug, slug_fills, 0, 0, 
               f"⚠️  Skipped {display_slug} (no valid team mappings - teams: {[f.get('Side', '') for f in slug_fills[:3]]})")
    
    # Load deltas data filtered by:
    # - Sport (league)
    # - Both teams (team and opposing_team columns)
    team_names_list = list(unique_team_abbrs)
    
    if deltas_processor.load_data(
        start_timestamp_ms=start_timestamp,
        end_timestamp_ms=end_timestamp,
        sport=sport,
        team_names=team_names_list
    ):
        slug_matched_count = 0
        
        # Match each fill
        for fill in slug_fills:
            fill_side = fill.get("Side", "").strip()
            timestamp_ms = fill.get("timestamp_ms")
            
            if fill_side and timestamp_ms:
                # Convert fill side to team abbreviation
                team_abbr = deltas_processor.get_deltas_team_name(fill_side, sport)
                
                if team_abbr:
                    # Match by team abbreviation and timestamp
                    deltas_data = deltas_processor.get_deltas_data(
                        team_abbr=team_abbr,
                        fill_timestamp=timestamp_ms
                    )
                    fill["deltas"] = deltas_data
                    
                    if deltas_data.get('deltas_before') or deltas_data.get('deltas_after'):
                        slug_matched_count += 1
        
        loaded_records = len(deltas_processor.processed_data)
        display_slug = market_slug[:50] + "..." if len(market_slug) > 50 else market_slug
        
        return (market_slug_index, market_slug, slug_fills, slug_matched_count, loaded_records,
               f"✅ Deltas {sport} ({display_slug}): {slug_matched_count}/{len(slug_fills)} fills matched (loaded {loaded_records} records)")
    else:
        display_slug = market_slug[:50] + "..." if len(market_slug) > 50 else market_slug
        return (market_slug_index, market_slug, slug_fills, 0, 0,
               f"⚠️  Failed to load Deltas data for {sport} ({display_slug})")

class PolymarketBBOProcessor:
    """Processor for Polymarket BBO data from PostgreSQL database (market_data table)"""
    def __init__(self):
        """Initialize the Polymarket BBO processor"""
        self.raw_data = []
        self.processed_data = []
    
    def load_data(self, start_timestamp_ms: int, end_timestamp_ms: int, market_slugs: List[str] = None, sports: List[str] = None):
        """Load Polymarket BBO data from PostgreSQL database into memory
        
        Args:
            start_timestamp_ms: Start timestamp in milliseconds
            end_timestamp_ms: End timestamp in milliseconds
            market_slugs: List of market_slugs (game_slug) to filter by (preferred, more specific)
            sports: List of sports to filter by (fallback if market_slugs not provided)
        """
        conn = None
        cursor = None
        
        try:
            conn = get_poly_db_connection()
            cursor = conn.cursor()
            
            print(f"Loading Polymarket BBO data from {datetime.fromtimestamp(start_timestamp_ms/1000).strftime('%Y-%m-%d %H:%M:%S')} to {datetime.fromtimestamp(end_timestamp_ms/1000).strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Convert timestamps to datetime for query
            start_ts = datetime.fromtimestamp(start_timestamp_ms / 1000.0, tz=timezone.utc)
            end_ts = datetime.fromtimestamp(end_timestamp_ms / 1000.0, tz=timezone.utc)
            
            # Build query
            query = """
                SELECT 
                    bbo,
                    best_bid,
                    best_ask,
                    polymarket_timestamp,
                    EXTRACT(EPOCH FROM polymarket_timestamp) * 1000 as timestamp_ms,
                    UPPER(TRIM(sport)) as sport,
                    LOWER(TRIM(outcome)) as outcome,
                    game_slug
                FROM market_data
                WHERE polymarket_timestamp >= %s
                    AND polymarket_timestamp <= %s
                    AND bbo IS NOT NULL
                    AND bbo > 0
            """
            params = [start_ts, end_ts]
            
            # Add market_slug filter if provided (more specific than sport)
            if market_slugs and len(market_slugs) > 0:
                placeholders = ','.join(['%s'] * len(market_slugs))
                query += f" AND game_slug IN ({placeholders})"
                params.extend(market_slugs)
                print(f"Filtering Polymarket BBO by market_slugs: {market_slugs}")
            # Fallback to sport filter if market_slugs not provided
            elif sports and len(sports) > 0:
                placeholders = ','.join(['%s'] * len(sports))
                query += f" AND UPPER(TRIM(sport)) IN ({placeholders})"
                params.extend([s.upper() for s in sports])
                print(f"Filtering Polymarket BBO by sports: {sports}")
            
            query += " ORDER BY polymarket_timestamp ASC"
            cursor.execute(query, tuple(params))
            
            self.raw_data = cursor.fetchall()
            print(f"Loaded {len(self.raw_data)} Polymarket BBO records from database")
            
            # Process data: convert to timestamp_ms and normalize
            self.processed_data = []
            for row in self.raw_data:
                timestamp_ms = int(row['timestamp_ms']) if row['timestamp_ms'] else None
                if timestamp_ms:
                    record = {
                        'timestamp': timestamp_ms,
                        'timestamp_dt': row['polymarket_timestamp'],
                        'sport': row['sport'],
                        'outcome': row['outcome'],
                        'game_slug': row.get('game_slug', ''),  # Include game_slug for reference
                        'bbo': float(row['bbo']) if row['bbo'] else None,
                        'best_bid': float(row['best_bid']) if row['best_bid'] else None,
                        'best_ask': float(row['best_ask']) if row['best_ask'] else None,
                    }
                    self.processed_data.append(record)
            
            # Sort by timestamp for fast lookups
            self.processed_data.sort(key=lambda x: x.get('timestamp', 0) or 0)
            
            print(f"Processed {len(self.processed_data)} valid Polymarket BBO records")
            print(f"  ✓ Data sorted by timestamp for fast lookups")
            
            return len(self.processed_data) > 0
            
        except Exception as e:
            print(f"Error loading Polymarket BBO data from database: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def get_polymarket_bbo_data(self, game_slug: str, outcome: str, fill_timestamp: int) -> Dict:
        """Get Polymarket BBO data from in-memory data using game_slug + outcome matching
        
        Matching logic:
        - Match by game_slug (market_slug) and outcome (side)
        - Before: Closest record before fill timestamp (within 10 min window)
        - After: First record at least 10 seconds after fill timestamp
        
        OPTIMIZED: Uses sorted in-memory data with early termination
        """
        if not game_slug or not outcome or not fill_timestamp:
            return {'polymarket_bbo_before': None, 'polymarket_bbo_after': None}
        
        if not self.processed_data:
            return {'polymarket_bbo_before': None, 'polymarket_bbo_after': None}
        
        # Normalize game_slug and outcome for matching
        game_slug_normalized = game_slug.strip().lower()
        outcome_normalized = outcome.strip().lower()
        
        # Calculate time windows
        time_window_ms = 5 * 60 * 1000  # 5 minutes
        start_timestamp = fill_timestamp - time_window_ms
        end_timestamp = fill_timestamp + time_window_ms
        after_timestamp_preferred = fill_timestamp + 10 * 1000  # Prefer data at least 10 seconds after
        
        # Find records in time range matching game_slug and outcome
        before_records = []
        after_records_all = []  # All records after fill (for fallback)
        after_records_preferred = []  # Records >= 10 seconds after (preferred)
        
        # Since processed_data is sorted by timestamp, we can use early termination
        for record in self.processed_data:
            record_timestamp = record.get('timestamp')
            if not record_timestamp:
                continue
            
            # Early termination: if we've passed the end timestamp, stop
            if record_timestamp > end_timestamp:
                break
            
            # Check if record matches game_slug and outcome
            record_game_slug = record.get('game_slug', '').strip().lower() if record.get('game_slug') else ''
            record_outcome = record.get('outcome', '').strip().lower() if record.get('outcome') else ''
            
            if record_game_slug == game_slug_normalized and record_outcome == outcome_normalized:
                # Check if record is in time window
                if start_timestamp <= record_timestamp <= fill_timestamp:
                    before_records.append(record)
                elif record_timestamp > fill_timestamp:  # Any data after fill
                    after_records_all.append(record)
                    if record_timestamp >= after_timestamp_preferred:  # Preferred: >= 10 seconds
                        after_records_preferred.append(record)
        
        # Use preferred records if available, otherwise fall back to all after records
        after_records = after_records_preferred if after_records_preferred else after_records_all
        
        # Find closest before record
        before_data = None
        if before_records:
            closest_before = max(before_records, key=lambda x: x.get('timestamp', 0))
            time_diff = fill_timestamp - closest_before.get('timestamp', 0)
            # Convert datetime to ISO string for JSON serialization
            before_timestamp_dt = closest_before.get('timestamp_dt')
            before_timestamp_str = before_timestamp_dt.isoformat() if before_timestamp_dt and isinstance(before_timestamp_dt, datetime) else None
            
            before_data = {
                'bbo': closest_before.get('bbo'),
                'best_bid': closest_before.get('best_bid'),
                'best_ask': closest_before.get('best_ask'),
                'polymarket_timestamp': before_timestamp_str,
                'seconds_from_fill': round(time_diff / 1000.0, 3),
                'time_relation': 'before'
            }
        
        # Find closest after record
        after_data = None
        if after_records:
            closest_after = min(after_records, key=lambda x: x.get('timestamp', float('inf')))
            time_diff = closest_after.get('timestamp', 0) - fill_timestamp
            # Check if we're using preferred (>= 10s) or fallback (< 10s) data
            is_preferred = time_diff >= 10.0
            # Convert datetime to ISO string for JSON serialization
            after_timestamp_dt = closest_after.get('timestamp_dt')
            after_timestamp_str = after_timestamp_dt.isoformat() if after_timestamp_dt and isinstance(after_timestamp_dt, datetime) else None
            
            after_data = {
                'bbo': closest_after.get('bbo'),
                'best_bid': closest_after.get('best_bid'),
                'best_ask': closest_after.get('best_ask'),
                'polymarket_timestamp': after_timestamp_str,
                'seconds_from_fill': round(time_diff / 1000.0, 3),
                'time_relation': 'after',
                'is_preferred': is_preferred  # True if >= 10s, False if < 10s (fallback)
            }
        
        return {
            'polymarket_bbo_before': before_data,
            'polymarket_bbo_after': after_data
        }
        try:
            # Safely get trading_pair - handle None values
            trading_pair = trade.get("trading_pair") or {}
            primary_instrument = trading_pair.get("primary_instrument") or {}
            boltodds_game = primary_instrument.get("boltodds_game") or {}
            opticodds_game = primary_instrument.get("opticodds_game") or {}
            
            # Extract game slug from polymarket_exchange.market_slug
            game_slug = ""
            polymarket_exchange = trading_pair.get("polymarket_exchange") or {}
            if not polymarket_exchange:
                # Try inside primary_instrument
                polymarket_exchange = primary_instrument.get("polymarket_exchange") or {}
            
            market_slug = polymarket_exchange.get("market_slug", "")
            if market_slug:
                game_slug = market_slug
            
            # Parse timestamp
            fill_tx_ts = trade.get("fill_tx_ts", "")
            
            date_str = ""
            time_str = ""
            order_submission_str = ""
            
            if fill_tx_ts:
                try:
                    # Parse ISO timestamp - assume it's in UTC+0
                    if 'Z' in fill_tx_ts:
                        dt_utc = datetime.fromisoformat(fill_tx_ts.replace('Z', '+00:00'))
                    elif '+' in fill_tx_ts or fill_tx_ts.count('-') > 2:
                        # Already has timezone info
                        dt_utc = datetime.fromisoformat(fill_tx_ts)
                    else:
                        # No timezone info, assume UTC
                        dt_naive = datetime.fromisoformat(fill_tx_ts)
                        dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                    
                    # Convert from UTC to UTC+8
                    utc8 = timezone(timedelta(hours=8))
                    dt_utc8 = dt_utc.astimezone(utc8)
                    
                    # Format as M/D/YYYY
                    date_str = f"{dt_utc8.month}/{dt_utc8.day}/{dt_utc8.year}"
                    
                    # Format as H:M:S.mmm (with zero padding)
                    microseconds = dt_utc8.microsecond
                    milliseconds = microseconds // 1000
                    time_str = f"{dt_utc8.hour:02d}:{dt_utc8.minute:02d}:{dt_utc8.second:02d}.{milliseconds:03d}"
                    
                except Exception as e:
                    print(f"Error parsing timestamp {fill_tx_ts}: {e}")
            
            # Parse rest_start_bot_ts for Order submission
            rest_start_bot_ts = trade.get("rest_start_bot_ts")
            if rest_start_bot_ts:
                try:
                    # Ensure it's a string
                    rest_start_bot_ts = str(rest_start_bot_ts).strip()
                    
                    if rest_start_bot_ts:
                        # Parse ISO timestamp - assume it's in UTC+0
                        if 'Z' in rest_start_bot_ts:
                            dt_utc = datetime.fromisoformat(rest_start_bot_ts.replace('Z', '+00:00'))
                        elif '+' in rest_start_bot_ts or (rest_start_bot_ts.count('-') > 2 and 'T' in rest_start_bot_ts):
                            # Already has timezone info or is ISO format with timezone
                            try:
                                dt_utc = datetime.fromisoformat(rest_start_bot_ts)
                                if dt_utc.tzinfo is None:
                                    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                            except ValueError:
                                # Try parsing without timezone and assume UTC
                                dt_naive = datetime.fromisoformat(rest_start_bot_ts)
                                dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                        else:
                            # No timezone info, assume UTC
                            dt_naive = datetime.fromisoformat(rest_start_bot_ts)
                            dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                        
                        # Convert from UTC to UTC+8
                        utc8 = timezone(timedelta(hours=8))
                        dt_utc8 = dt_utc.astimezone(utc8)
                        
                        # Format as H:M:S.mmm (same format as Fill Time)
                        microseconds = dt_utc8.microsecond
                        milliseconds = microseconds // 1000
                        order_submission_str = f"{dt_utc8.hour:02d}:{dt_utc8.minute:02d}:{dt_utc8.second:02d}.{milliseconds:03d}"
                    
                except Exception as e:
                    print(f"Error parsing rest_start_bot_ts '{rest_start_bot_ts}': {e}")
            
            # Extract side (team name)
            side = trade.get("side", "")
            
            # Extract league/sport from trading_pair.polymarket_exchange.market_slug
            # Extract the word before the first dash (e.g., "nba-was-lac-2026-01-14" -> "nba")
            league = ""
            
            # First try: Get from polymarket_exchange.market_slug (primary method)
            polymarket_exchange = trading_pair.get("polymarket_exchange") or {}
            if not polymarket_exchange:
                # Try inside primary_instrument
                polymarket_exchange = primary_instrument.get("polymarket_exchange") or {}
            
            market_slug = polymarket_exchange.get("market_slug", "")
            if market_slug:
                # Extract word before first dash
                parts = market_slug.split("-")
                if parts:
                    league = parts[0].upper()  # Convert to uppercase (e.g., "nba" -> "NBA")
            
            # Fallback to boltodds_game if market_slug extraction fails
            if not league and boltodds_game:
                league = boltodds_game.get("sport", "") or boltodds_game.get("league", "")
            
            # Additional fallback to opticodds_game
            if not league and opticodds_game:
                league = opticodds_game.get("league", "") or opticodds_game.get("sport", "")
            
            # Extract fill price - handle None values
            last_fill_price = trade.get("last_fill_price") or {}
            fill_price = last_fill_price.get("amount", "") if isinstance(last_fill_price, dict) else ""
            
            # Extract fill value - handle None values
            last_fill_value = trade.get("last_fill_value") or {}
            fill_value = last_fill_value.get("amount", "") if isinstance(last_fill_value, dict) else ""
            
            # Extract implied_price and price_before_max_individual from placement_logic
            # Priority: placement_logic > top level of trade
            implied_price = None
            price_before_max_individual = None
            
            placement_logic = trade.get("placement_logic") or {}
            if placement_logic:
                if "implied_price" in placement_logic:
                    implied_price = placement_logic["implied_price"]
                if "price_before_max_individual" in placement_logic:
                    price_before_max_individual = placement_logic["price_before_max_individual"]
            
            # Fallback to top level if not in placement_logic
            if implied_price is None:
                implied_price = trade.get("implied_price")
            if price_before_max_individual is None:
                price_before_max_individual = trade.get("price_before_max_individual")
            
            # Calculate timestamp_ms for PolymarketBBO matching
            timestamp_ms = None
            if fill_tx_ts:
                try:
                    if 'Z' in fill_tx_ts:
                        dt_utc = datetime.fromisoformat(fill_tx_ts.replace('Z', '+00:00'))
                    elif '+' in fill_tx_ts or fill_tx_ts.count('-') > 2:
                        dt_utc = datetime.fromisoformat(fill_tx_ts)
                    else:
                        dt_naive = datetime.fromisoformat(fill_tx_ts)
                        dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                    timestamp_ms = int(dt_utc.timestamp() * 1000)
                except Exception as e:
                    print(f"Error calculating timestamp_ms: {e}")
            
            return {
                "Date": date_str,
                "Order submission": order_submission_str,
                "Fill Time": time_str,
                "Side": side,
                "League": league,
                "Game Slug": game_slug,
                "Fill Price": fill_price,
                "Implied Price": implied_price,
                "Price Before Max Individual": price_before_max_individual,
                "Fill Value": fill_value,
                "fill_tx_ts": fill_tx_ts,
                "timestamp_ms": timestamp_ms,  # For PolymarketBBO matching
                "full_trade_data": trade  # Include full trade data for expandable rows
            }
        except Exception as e:
            print(f"Error extracting fill data: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def read_fill_data(self, limit=3000, hours_back=None, progress_callback=None) -> List[Dict]:
        """Read fill data from Trade Feed API and return with full trade data
        Args:
            limit: Maximum number of trades to fetch (default 3000)
            hours_back: Optional hours to filter by. If None, no time filtering is applied.
            progress_callback: Optional callback function to report progress (receives dict with progress info)
        """
        read_start = time.time()
        try:
            fetch_start = time.time()
            # Fetch exactly limit trades (500 per page, 6 times = 3000)
            trades = self.fetch_trades(page_size=500, total_trades=limit)
            fetch_time = time.time() - fetch_start
            print(f"⏱️  fetch_trades(): {fetch_time:.2f}s ({len(trades)} trades)")
            
            fill_data = []
            filtered_out_count = 0
            
            # Only apply time filtering if hours_back is specified
            cutoff_time = None
            if hours_back:
                now_utc = datetime.now(timezone.utc)
                cutoff_time = now_utc - timedelta(hours=hours_back)
                print(f"📅 Filtering trades to last {hours_back} hours. Cutoff time (UTC): {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"📅 No time filtering - returning all {len(trades)} fetched trades")
            
            for idx, trade in enumerate(trades):
                extracted = self.extract_fill_data(trade)
                if not extracted:
                    if idx < 5:  # Only log first 5 failures to avoid spam
                        print(f"  ⚠️  Trade {idx}: extract_fill_data returned None")
                    continue
                
                # Include trade even if Date/Fill Time are missing (they might be empty strings)
                # Apply time filtering only if cutoff_time is set AND we have valid date/time
                if cutoff_time and extracted.get("Date") and extracted.get("Fill Time"):
                        try:
                            date_str = extracted.get("Date")  # M/D/YYYY
                            time_str = extracted.get("Fill Time")  # H:M:S.mmm
                            
                            # Parse date
                            month, day, year = map(int, date_str.split('/'))
                            
                            # Parse time (handle decimal seconds)
                            if '.' in time_str:
                                time_main, ms_str = time_str.split('.')
                                time_parts = time_main.split(':')
                                hours = int(time_parts[0])
                                minutes = int(time_parts[1])
                                seconds = int(time_parts[2])
                                milliseconds = int(ms_str.ljust(3, '0')[:3])
                            else:
                                time_parts = time_str.split(':')
                                hours = int(time_parts[0])
                                minutes = int(time_parts[1])
                                seconds = int(time_parts[2])
                                milliseconds = 0
                            
                            # Create datetime in UTC+8 (as per existing logic)
                            utc8 = timezone(timedelta(hours=8))
                            dt_utc8 = datetime(year, month, day, hours, minutes, seconds, 
                                             microsecond=milliseconds*1000, tzinfo=utc8)
                            
                            # Convert to UTC for comparison
                            dt_utc = dt_utc8.astimezone(timezone.utc)
                            
                            # Check if within time window
                            if dt_utc >= cutoff_time:
                                fill_data.append(extracted)
                            else:
                                filtered_out_count += 1
                                age_hours = (datetime.now(timezone.utc) - dt_utc).total_seconds() / 3600
                                if filtered_out_count <= 5:  # Only print first 5 filtered trades
                                    print(f"  ✗ Filtered: {extracted.get('Side')} - {date_str} {time_str} ({age_hours:.1f}h old)")
                        
                        except Exception as parse_error:
                            # If parsing fails, include it anyway (be permissive)
                            print(f"  ⚠ Warning: Could not parse date/time for filtering: {parse_error}")
                            fill_data.append(extracted)
                else:
                    # No time filtering OR missing date/time - include all trades
                    fill_data.append(extracted)
            
            # Match PolymarketBBO data if available - load by market_slug to reduce memory usage
            try:
                if fill_data:
                    # Group fills by market_slug (Game Slug) for more specific grouping
                    fills_by_market_slug = {}
                    for fill in fill_data:
                        market_slug = fill.get("Game Slug", "").strip()
                        if market_slug:
                            if market_slug not in fills_by_market_slug:
                                fills_by_market_slug[market_slug] = []
                            fills_by_market_slug[market_slug].append(fill)
                    
                    if fills_by_market_slug:
                        total_market_slugs = len(fills_by_market_slug)
                        print(f"📊 Matching {len(fill_data)} fills with Polymarket BBO data (grouped by {total_market_slugs} market slugs)...")
                        max_workers = min(os.cpu_count() or 4, total_market_slugs, 8)  # Limit to 8 workers max
                        print(f"🚀 Using parallel processing with {max_workers} workers")
                        total_matched_count = 0
                        
                        # Prepare tasks for parallel processing
                        tasks = [(idx, slug, fills, total_market_slugs) for idx, (slug, fills) in enumerate(fills_by_market_slug.items(), 1)]
                        
                        # Process market slugs in parallel using ThreadPoolExecutor
                        completed_count = 0
                        results = []
                        
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            # Submit all tasks
                            future_to_task = {executor.submit(process_single_market_slug_parallel, task): task for task in tasks}
                            
                            # Process completed tasks as they finish
                            for future in as_completed(future_to_task):
                                completed_count += 1
                                try:
                                    result = future.result()
                                    market_slug_index, market_slug, slug_fills, slug_matched_count, loaded_records, message = result
                                    
                                    # Update total matched count
                                    total_matched_count += slug_matched_count
                                    
                                    # Store result for later (to maintain order if needed)
                                    results.append(result)
                                    
                                    # Print result
                                    print(f"  [{completed_count}/{total_market_slugs}] {message}")
                                    
                                    # Send progress update via callback if provided
                                    if progress_callback:
                                        try:
                                            display_slug = market_slug[:50] + "..." if len(market_slug) > 50 else market_slug
                                            progress_callback({
                                                'type': 'progress',
                                                'current': completed_count,
                                                'total': total_market_slugs,
                                                'market_slug': display_slug,
                                                'fills_count': len(slug_fills),
                                                'message': f'Processing {completed_count}/{total_market_slugs}: {display_slug} (parallel)'
                                            })
                                        except Exception as e:
                                            print(f"  ⚠️  Error sending progress update: {e}")
                                    
                                except Exception as e:
                                    task = future_to_task[future]
                                    print(f"  ❌ Error processing market_slug {task[1]}: {e}")
                                    import traceback
                                    traceback.print_exc()
                        
                        print(f"✅ Polymarket BBO matching complete: {total_matched_count}/{len(fill_data)} fills matched across all market slugs")
                    else:
                        print(f"⚠️  No market slugs found in fill data, skipping Polymarket BBO matching")
            except Exception as e:
                print(f"⚠️  Error matching Polymarket BBO data: {e}")
                import traceback
                traceback.print_exc()
            
            # Match Deltas (Pinnacle) data if available - parallel processing by slug
            try:
                if fill_data:
                    # Group fills by market_slug (same grouping as Polymarket BBO)
                    fills_by_market_slug = {}
                    for fill in fill_data:
                        market_slug = fill.get("Game Slug", "").strip()
                        if market_slug:
                            if market_slug not in fills_by_market_slug:
                                fills_by_market_slug[market_slug] = []
                            fills_by_market_slug[market_slug].append(fill)
                    
                    if fills_by_market_slug:
                        total_market_slugs = len(fills_by_market_slug)
                        print(f"📊 Matching {len(fill_data)} fills with Deltas data (grouped by {total_market_slugs} market slugs)...")
                        max_workers = min(os.cpu_count() or 4, total_market_slugs, 8)
                        print(f"🚀 Using parallel processing with {max_workers} workers for Deltas")
                        total_deltas_matched = 0
                        
                        # Prepare tasks for parallel processing
                        tasks = [(idx, slug, fills, total_market_slugs) 
                                for idx, (slug, fills) in enumerate(fills_by_market_slug.items(), 1)]
                        
                        # Process market slugs in parallel
                        completed_count = 0
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            future_to_task = {
                                executor.submit(process_single_slug_deltas_parallel, task): task 
                                for task in tasks
                            }
                            
                            for future in as_completed(future_to_task):
                                completed_count += 1
                                try:
                                    result = future.result()
                                    market_slug_index, market_slug, slug_fills, slug_matched_count, loaded_records, message = result
                                    total_deltas_matched += slug_matched_count
                                    print(f"  [{completed_count}/{total_market_slugs}] {message}")
                                    
                                    if progress_callback:
                                        try:
                                            display_slug = market_slug[:50] + "..." if len(market_slug) > 50 else market_slug
                                            progress_callback({
                                                'type': 'progress',
                                                'current': completed_count,
                                                'total': total_market_slugs,
                                                'market_slug': display_slug,
                                                'fills_count': len(slug_fills),
                                                'message': f'Processing Deltas {completed_count}/{total_market_slugs}: {display_slug}'
                                            })
                                        except Exception as e:
                                            print(f"  ⚠️  Error sending progress update: {e}")
                                except Exception as e:
                                    task = future_to_task[future]
                                    print(f"  ❌ Error processing Deltas for market_slug {task[1]}: {e}")
                                    import traceback
                                    traceback.print_exc()
                        
                        print(f"✅ Deltas matching complete: {total_deltas_matched}/{len(fill_data)} fills matched")
                    else:
                        print(f"⚠️  No market slugs found, skipping Deltas matching")
            except Exception as e:
                print(f"⚠️  Error matching Deltas data: {e}")
                import traceback
                traceback.print_exc()
            
            total_time = time.time() - read_start
            failed_extractions = len(trades) - len(fill_data) - filtered_out_count - filtered_fresh_count
            print(f"✓ Successfully converted {len(fill_data)} trades to fill data format")
            print(f"📊 Summary: {len(trades)} total trades fetched, {len(fill_data)} extracted successfully")
            if failed_extractions > 0:
                print(f"⚠️  {failed_extractions} trades failed extraction (returned None)")
            if filtered_fresh_count > 0:
                print(f"⏰ Filtered out {filtered_fresh_count} trades newer than 3 minutes (too fresh for BBO matching)")
            if cutoff_time:
                print(f"✗ Filtered out {filtered_out_count} trades older than {hours_back} hours")
            print(f"⏱️  TOTAL read_fill_data(): {total_time:.2f}s")
            return fill_data
            
        except Exception as e:
            print(f"Error reading fill data from Trade Feed API: {e}")
            import traceback
            traceback.print_exc()
            return []

class DeltasProcessor:
    """Processor for Pinnacle Deltas data from PostgreSQL database (deltas table)"""
    
    def __init__(self):
        self.processed_data = []
        self.db_connection = None
    
    def get_db_connection(self):
        """Get connection to Deltas database"""
        if self.db_connection is None or self.db_connection.closed:
            database_url = os.getenv('DATABASE_URL')
            if not database_url:
                raise ValueError("DATABASE_URL environment variable not set")
            
            result = urlparse(database_url)
            self.db_connection = psycopg2.connect(
                database=result.path[1:],
                user=result.username,
                password=result.password,
                host=result.hostname,
                port=result.port,
                connect_timeout=10,
                cursor_factory=RealDictCursor,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
        return self.db_connection
    
    def create_team_mapping(self) -> Dict[str, Dict[str, str]]:
        """Create mapping from FILL team names (lowercase) to Deltas team abbreviations"""
        return {
            'MLB': {
                'mariners': 'SEA',
                'rockies': 'COL', 
                'giants': 'SF',
                'royals': 'KC',
                'astros': 'HOU',
                'orioles': 'BAL',
                'mets': 'NYM',
                'cardinals': 'STL',
                'yankees': 'NYY',
                'twins': 'MIN',
                'tigers': 'DET',
                'blue jays': 'TOR',
                'pirates': 'PIT',
                'guardians': 'CLE',
                'white sox': 'CWS',
                'red sox': 'BOS',
                'brewers': 'MIL',
                'braves': 'ATL',
                'marlins': 'MIA',
                'nationals': 'WSH',
                'phillies': 'PHI',
                'rangers': 'TEX',
                'rays': 'TB',
                'cubs': 'CHC',
                'diamondbacks': 'ARI',
                'dodgers': 'LAD',
                'padres': 'SD',
                'angels': 'LAA',
                'athletics': 'OAK',
                'reds': 'CIN',
            },
            'NFL': {
                'eagles': 'PHI',
                'cardinals': 'ARI',
                'giants': 'NYG',
                'falcons': 'ATL',
                'panthers': 'CAR',
                'bears': 'CHI',
                'cowboys': 'DAL',
                'lions': 'DET',
                'packers': 'GB',
                'rams': 'LAR',
                'vikings': 'MIN',
                'saints': 'NO',
                '49ers': 'SF',
                'seahawks': 'SEA',
                'buccaneers': 'TB',
                'commanders': 'WSH',
                'ravens': 'BAL',
                'bills': 'BUF',
                'bengals': 'CIN',
                'browns': 'CLE',
                'broncos': 'DEN',
                'texans': 'HOU',
                'colts': 'IND',
                'jaguars': 'JAX',
                'chiefs': 'KC',
                'chargers': 'LAC',
                'dolphins': 'MIA',
                'raiders': 'LV',
                'patriots': 'NE',
                'jets': 'NYJ',
                'steelers': 'PIT',
                'titans': 'TEN',
            },
            'NHL': {
                'bruins': 'BOS',
                'sabres': 'BUF',
                'hurricanes': 'CAR',
                'blue jackets': 'CBJ',
                'flames': 'CGY',
                'blackhawks': 'CHI',
                'avalanche': 'COL',
                'stars': 'DAL',
                'red wings': 'DET',
                'oilers': 'EDM',
                'panthers': 'FLA',
                'ducks': 'ANA',
                'golden knights': 'VGK',
                'kraken': 'SEA',
                'blues': 'STL',
                'lightning': 'TBL',
                'mammoth': 'UTA',
                'canucks': 'VAN',
                'jets': 'WPG',
                'capitals': 'WSH',
                'wild': 'MIN',
                'canadiens': 'MTL',
                'devils': 'NJD',
                'predators': 'NSH',
                'islanders': 'NYI',
                'rangers': 'NYR',
                'senators': 'OTT',
                'flyers': 'PHI',
                'penguins': 'PIT',
                'sharks': 'SJS',
                'maple leafs': 'TOR',
                'utah': 'UTA',
                'kings': 'LAK',
            },
            'NBA': {
                'hawks': 'ATL',
                'celtics': 'BOS',
                'nets': 'BKN',
                'hornets': 'CHA',
                'bulls': 'CHI',
                'cavaliers': 'CLE',
                'mavericks': 'DAL',
                'nuggets': 'DEN',
                'pistons': 'DET',
                'rockets': 'HOU',
                'pacers': 'IND',
                'clippers': 'LAC',
                'lakers': 'LAL',
                'grizzlies': 'MEM',
                'heat': 'MIA',
                'bucks': 'MIL',
                'timberwolves': 'MIN',
                'pelicans': 'NOP',
                'knicks': 'NYK',
                'thunder': 'OKC',
                'magic': 'ORL',
                '76ers': 'PHI',
                'suns': 'PHX',
                'trail blazers': 'POR',
                'kings': 'SAC',
                'spurs': 'SAS',
                'raptors': 'TOR',
                'jazz': 'UTA',
                'warriors': 'GSW',
                'wizards': 'WAS',
                'pho': 'PHX',
            },
            'ATP': {
                # Tennis players - map lowercase fill names to full database names
                'djokovic': 'Novak Djokovic',
                'musetti': 'Lorenzo Musetti',
                'alcaraz': 'Carlos Alcaraz',
                'minaur': 'Alex De Minaur',
                'de minaur': 'Alex De Minaur',
                'sinner': 'Jannik Sinner',
                'medvedev': 'Daniil Medvedev',
                'rublev': 'Andrey Rublev',
                'tsitsipas': 'Stefanos Tsitsipas',
                'zverev': 'Alexander Zverev',
                'fritz': 'Taylor Fritz',
                'paul': 'Tommy Paul',
                'tiafoe': 'Frances Tiafoe',
                'norrie': 'Cameron Norrie',
                'hurkacz': 'Hubert Hurkacz',
                'ruud': 'Casper Ruud',
                'rune': 'Holger Rune',
                'auger-aliassime': 'Felix Auger-Aliassime',
                'auger aliassime': 'Felix Auger-Aliassime',
                'dimitrov': 'Grigor Dimitrov',
                'khachanov': 'Karen Khachanov',
                'cerundolo': 'Francisco Cerundolo',
                'baez': 'Sebastian Baez',
                'shelton': 'Ben Shelton',
                'korda': 'Sebastian Korda',
                'eubanks': 'Christopher Eubanks',
                'giron': 'Marcos Giron',
                'nakashima': 'Brandon Nakashima',
                'tabur': 'Clement Tabur',
                'sachko': 'Vitaliy Sachko',
                'draper': 'Jack Draper',
                'bublik': 'Alexander Bublik',
                'davidovich fokina': 'Alejandro Davidovich Fokina',
                'lehecka': 'Jiri Lehecka',
                'mensik': 'Jakub Mensik',
                'cobolli': 'Flavio Cobolli',
                'shapovalov': 'Denis Shapovalov',
                'fonseca': 'Joao Fonseca',
                'griekspoor': 'Tallon Griekspoor',
                'darderi': 'Luciano Darderi',
                'rinderknech': 'Arthur Rinderknech',
                'vacherot': 'Valentin Vacherot',
                'moutet': 'Corentin Moutet',
                'machac': 'Tomas Machac',
                'michelsen': 'Alex Michelsen',
                'munar': 'Jaume Munar',
                'humbert': 'Ugo Humbert',
                'tien': 'Learner Tien',
                'fils': 'Arthur Fils',
                'bergs': 'Zizou Bergs',
                'diallo': 'Gabriel Diallo',
                'sonego': 'Lorenzo Sonego',
                'muller': 'Alexandre Muller',
                'altmaier': 'Daniel Altmaier',
                'borges': 'Nuno Borges',
                'carabelli': 'Camilo Ugo Carabelli',
                'marozsan': 'Fabian Marozsan',
                'opelka': 'Reilly Opelka',
                'brooksby': 'Jenson Brooksby',
                'popyrin': 'Alexei Popyrin',
                'kecmanovic': 'Miomir Kecmanovic',
                'fucsovics': 'Marton Fucsovics',
                'royer': 'Valentin Royer',
                'bonzi': 'Benjamin Bonzi',
                'dzumhur': 'Damir Dzumhur',
                'mpetshi perricard': 'Giovanni Mpetshi Perricard',
                'etcheverry': 'Tomas Martin Etcheverry',
                'comesana': 'Francisco Comesana',
                'kovacevic': 'Aleksandar Kovacevic',
                'berrettini': 'Matteo Berrettini',
                'arnaldi': 'Matteo Arnaldi',
                'medjedovic': 'Hamad Medjedovic',
                'atmane': 'Terence Atmane',
                'quinn': 'Ethan Quinn',
                'majchrzak': 'Kamil Majchrzak',
                'cazaux': 'Arthur Cazaux',
                'monfils': 'Gael Monfils',
                'mannarino': 'Adrian Mannarino',
                'fearnley': 'Jacob Fearnley',
                'navone': 'Mariano Navone',
                'bellucci': 'Mattia Bellucci',
                'collignon': 'Raphael Collignon',
                'jong': 'Jesper De Jong',
                'cilic': 'Marin Cilic',
                'zandschulp': 'Botic Van De Zandschulp',
                'nardi': 'Luca Nardi',
                'djere': 'Laslo Djere',
                'walton': 'Adam Walton',
                'halys': 'Quentin Halys',
                'misolic': 'Filip Misolic',
                'cerundolo_86': 'Juan Manuel Cerundolo',
                'vukic': 'Aleksandar Vukic',
                'svrcina': 'Dalibor Svrcina',
                'tabilo': 'Alejandro Tabilo',
                'nava': 'Emilio Nava',
                'busta': 'Pablo Carreno Busta',
                'kopriva': 'Vit Kopriva',
                'mochizuki': 'Shintaro Mochizuki',
                'agut': 'Roberto Bautista Agut',
                'martinez': 'Pedro Martinez',
                'spizzirri': 'Eliot Spizzirri',
                'shevchenko': 'Alexander Shevchenko',
                'gaston': 'Hugo Gaston',
                'schoolkate': 'Tristan Schoolkate',
                'tirante': 'Thiago Agustin Tirante',
                'struff': 'Jan-Lennard Struff',
                'blockx': 'Alexander Blockx',
                'taberner': 'Carlos Taberner',
                'garin': 'Cristian Garin',
                'burruchaga': 'Roman Andres Burruchaga',
                'thompson': 'Jordan Thompson',
                "o'connell": "Christopher O'Connell",
                'basilashvili': 'Nikoloz Basilashvili',
                'buse': 'Ignacio Buse',
                'mcdonald': 'Mackenzie McDonald',
                'holt': 'Brandon Holt',
                'coric': 'Borna Coric',
                'duckworth': 'James Duckworth',
                'bu': 'Yunchaokete Bu',
                'lajovic': 'Dusan Lajovic',
                'goffin': 'David Goffin',
                'hanfmann': 'Yannick Hanfmann',
                'draxl': 'Liam Draxl',
                'nishikori': 'Kei Nishikori',
                'jarry': 'Nicolas Jarry',
                'prizmic': 'Dino Prizmic',
                'vera': 'Tomas Barrios Vera',
                'klein': 'Lukas Klein',
                'harris': 'Billy Harris',
                'dellien': 'Hugo Dellien',
                'choinski': 'Jan Choinski',
                'hijikata': 'Rinky Hijikata',
                'gaubas': 'Vilius Gaubas',
                'passaro': 'Francesco Passaro',
                'landaluce': 'Martin Landaluce',
                'tseng': 'Chun-Hsin Tseng',
                'kjaer': 'Nicolai Budkov Kjaer',
                'baena': 'Roberto Carballes Baena',
                'nishioka': 'Yoshihito Nishioka',
                'basavareddy': 'Nishesh Basavareddy',
                'ofner': 'Sebastian Ofner',
                'trungelliti': 'Marco Trungelliti',
                'moller': 'Elmer Moller',
                'pellegrino': 'Andrea Pellegrino',
                'echargui': 'Moez Echargui',
                'virtanen': 'Otto Virtanen',
                'svajda': 'Zachary Svajda',
                'blanchet': 'Ugo Blanchet',
                'herbert': 'Pierre-Hugues Herbert',
                'boyer': 'Tristan Boyer',
                'kypson': 'Patrick Kypson',
                'smith': 'Colton Smith',
                'lajal': 'Mark Lajal',
                'galan': 'Daniel Elahi Galan',
                'droguet': 'Titouan Droguet',
                'gigante': 'Matteo Gigante',
                'wong': 'Coleman Wong',
                'faria': 'Jaime Faria',
                'maestrelli': 'Francesco Maestrelli',
                'zeppieri': 'Giulio Zeppieri',
                'jacquet': 'Kyrian Jacquet',
                'shimabukuro': 'Sho Shimabukuro',
                'ouden': 'Guy Den Ouden',
                'wawrinka': 'Stan Wawrinka',
                'mikrut': 'Luka Mikrut',
                'rodionov': 'Jurij Rodionov',
                'mayot': 'Harold Mayot',
                'safiullin': 'Roman Safiullin',
                'ficovich': 'Juan Pablo Ficovich',
                'piros': 'Zsombor Piros',
                'jodar': 'Rafael Jodar',
                'rocha': 'Henrique Rocha',
                'vallejo': 'Adolfo Daniel Vallejo',
                'holmgren': 'August Holmgren',
                'watanuki': 'Yosuke Watanuki',
                'merida': 'Daniel Merida',
                'assche': 'Luca Van Assche',
                'damm': 'Martin Damm',
                'habib': 'Hady Habib',
                'riedi': 'Leandro Riedi',
                'monteiro': 'Thiago Monteiro',
                'barrena': 'Alex Barrena',
                'kym': 'Jerome Kym',
                'grenier': 'Hugo Grenier',
                'sakamoto': 'Rei Sakamoto',
                'wu': 'Yibing Wu',
                'rodesch': 'Chris Rodesch',
                'evans': 'Daniel Evans',
                'zheng': 'Michael Zheng',
                'tomic': 'Bernard Tomic',
                'neumayer': 'Lukas Neumayer',
                'galarneau': 'Alexis Galarneau',
                'kubler': 'Jason Kubler',
                'ymer': 'Elias Ymer',
                'jones': 'Jack Pinnington Jones',
                'coppejans': 'Kimmer Coppejans',
                'crawford': 'Oliver Crawford',
                'engel': 'Justin Engel',
                'peniston': 'Ryan Peniston',
                'mccabe': 'James Mccabe',
                'fery': 'Arthur Fery',
                'bolt': 'Alex Bolt',
                'molcan': 'Alex Molcan',
                'loffhagen': 'George Loffhagen',
            },
            'WTA': {
                # Women's tennis players - map lowercase fill names to full database names
                'sabalenka': 'Aryna Sabalenka',
                'swiatek': 'Iga Swiatek',
                'gauff': 'Coco Gauff',
                'anisimova': 'Amanda Anisimova',
                'pegula': 'Jessica Pegula',
                'rybakina': 'Elena Rybakina',
                'keys': 'Madison Keys',
                'paolini': 'Jasmine Paolini',
                'andreeva': 'Mirra Andreeva',
                'alexandrova': 'Ekaterina Alexandrova',
                'bencic': 'Belinda Bencic',
                'tauson': 'Clara Tauson',
                'noskova': 'Linda Noskova',
                'svitolina': 'Elina Svitolina',
                'navarro': 'Emma Navarro',
                'osaka': 'Naomi Osaka',
                'samsonova': 'Liudmila Samsonova',
                'mboko': 'Victoria Mboko',
                'muchova': 'Karolina Muchova',
                'mertens': 'Elise Mertens',
                'shnaider': 'Diana Shnaider',
                'fernandez': 'Leylah Fernandez',
                'ostapenko': 'Jelena Ostapenko',
                'zheng': 'Qinwen Zheng',
                'badosa': 'Paula Badosa',
                'kostyuk': 'Marta Kostyuk',
                'yastremska': 'Dayana Yastremska',
                'kenin': 'Sofia Kenin',
                'raducanu': 'Emma Raducanu',
                'kudermetova': 'Veronika Kudermetova',
                'kessler': 'Mccartney Kessler',
                'joint': 'Maya Joint',
                'kalinskaya': 'Anna Kalinskaya',
                'vondrousova': 'Marketa Vondrousova',
                'jovic': 'Iva Jovic',
                'boisson': 'Lois Boisson',
                'kasatkina': 'Daria Kasatkina',
                'li': 'Ann Li',
                'cristian': 'Jaqueline Cristian',
                'lys': 'Eva Lys',
                'maria': 'Tatjana Maria',
                'bouzas maneiro': 'Jessica Bouzas Maneiro',
                'bouzkova': 'Marie Bouzkova',
                'cirstea': 'Sorana Cirstea',
                'krueger': 'Ashlyn Krueger',
                'siegemund': 'Laura Siegemund',
                'pavlyuchenkova': 'Anastasia Pavlyuchenkova',
                'arango': 'Emiliana Arango',
                'siniakova': 'Katerina Siniakova',
                'eala': 'Alexandra Eala',
                'potapova': 'Anastasia Potapova',
                'sakkari': 'Maria Sakkari',
                'tjen': 'Janice Tjen',
                'bucsa': 'Cristina Bucsa',
                'linette': 'Magda Linette',
                'valentova': 'Tereza Valentova',
                'wang_x': 'Xinyu Wang',
                'haddad maia': 'Beatriz Haddad Maia',
                'frech': 'Magdalena Frech',
                'jacquemot': 'Elsa Jacquemot',
                'parks': 'Alycia Parks',
                'baptiste': 'Hailey Baptiste',
                'blinkova': 'Anna Blinkova',
                'stearns': 'Peyton Stearns',
                'collins': 'Danielle Collins',
                'krejcikova': 'Barbora Krejcikova',
                'sierra': 'Solana Sierra',
                'danilovic': 'Olga Danilovic',
                'golubic': 'Viktorija Golubic',
                'ruzic': 'Antonia Ruzic',
                'kartal': 'Sonay Kartal',
                'vekic': 'Donna Vekic',
                'putintseva': 'Yulia Putintseva',
                'sramkova': 'Rebecca Sramkova',
                'bejlek': 'Sara Bejlek',
                'jones_f': 'Francesca Jones',
                'bondar': 'Anna Bondar',
                'jabeur': 'Ons Jabeur',
                'gracheva': 'Varvara Gracheva',
                'osorio': 'Camila Osorio',
                'mcnally': 'Caty Mcnally',
                'zarazua': 'Renata Zarazua',
                'tomljanovic': 'Ajla Tomljanovic',
                'cocciaretto': 'Elisabetta Cocciaretto',
                'seidel': 'Ella Seidel',
                'lamens': 'Suzan Lamens',
                'sun': 'Lulu Sun',
                'kudermetova_p': 'Polina Kudermetova',
                'volynets': 'Katie Volynets',
                'uchijima': 'Moyuka Uchijima',
                'grabher': 'Julia Grabher',
                'semenistaja': 'Darja Semenistaja',
                'walter': 'Simona Waltert',
                'birrell': 'Kimberly Birrell',
                'galfi': 'Dalma Galfi',
                'selekhmeteva': 'Oksana Selekhmeteva',
                'juvan': 'Kaja Juvan',
                'erjavec': 'Veronika Erjavec',
                'ruse': 'Elena-Gabriela Ruse',
                'azarenka': 'Victoria Azarenka',
                'pliskova': 'Karolina Pliskova',
                'stephens': 'Sloane Stephens',
                'garcia': 'Caroline Garcia',
                'kvitova': 'Petra Kvitova',
                'halep': 'Simona Halep',
                'kerber': 'Angelique Kerber',
                'muguruza': 'Garbine Muguruza',
            },
        }
    
    def normalize_league_name(self, league: str) -> Optional[str]:
        """Normalize league name from FILL to match sport in Deltas"""
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
    
    def get_deltas_team_name(self, fill_side: str, league: str) -> Optional[str]:
        """
        Convert fill side name to Deltas team abbreviation or player name.
        
        For team sports (NBA, NHL, etc.): Returns abbreviation (e.g., "LAL")
        For tennis (ATP, WTA): Returns capitalized player name (e.g., "Djokovic")
        
        Example:
            fill_side = "Lakers", league = "NBA" → Returns "LAL"
            fill_side = "djokovic", league = "ATP" → Returns "Djokovic"
        """
        if not fill_side or not league:
            return None
        
        normalized_sport = self.normalize_league_name(league)
        if not normalized_sport:
            return None
        
        team_mappings = self.create_team_mapping()
        if normalized_sport not in team_mappings:
            return None
        
        fill_lower = str(fill_side).strip().lower()
        league_teams = team_mappings[normalized_sport]
        
        # Check if there's a direct mapping
        mapped_name = league_teams.get(fill_lower)
        if mapped_name:
            return mapped_name
        
        # For tennis, if no direct mapping found, try to find a partial match
        # This handles cases where the player name might not be in our mapping
        if normalized_sport in ['ATP', 'WTA']:
            # Try to find a match where the fill name is part of the database name
            # e.g., "djokovic" might match "Novak Djokovic"
            for db_name in league_teams.values():
                db_name_lower = db_name.lower()
                # Check if fill name appears in database name (as last name)
                if fill_lower in db_name_lower:
                    # Extract last name from full name (last word)
                    last_name = db_name.split()[-1] if ' ' in db_name else db_name
                    if fill_lower == last_name.lower():
                        return db_name
            
            # Fallback: try capitalizing first letter of each word
            # This handles cases where the player name might not be in our mapping
            words = fill_lower.split()
            capitalized = ' '.join(word.capitalize() for word in words)
            return capitalized
        
        return None
    
    def load_data(self, start_timestamp_ms: int, end_timestamp_ms: int, 
                  sport: str = None, team_names: List[str] = None) -> bool:
        """
        Load deltas data filtered by:
        - Timestamp range
        - Sport (league)
        - Team names (abbreviations like 'LAL', 'BOS')
        """
        conn = None
        cursor = None
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    id,
                    timestamp_ms,
                    timestamp,
                    sport,
                    sportsbook,
                    team,
                    opposing_team,
                    percentage,
                    bbo_price,
                    polymarket_skew,
                    total_spread,
                    notes
                FROM deltas
                WHERE sportsbook = 'Pinnacle'
                    AND timestamp_ms >= %s
                    AND timestamp_ms <= %s
            """
            params = [start_timestamp_ms, end_timestamp_ms]
            
            # Add sport filter
            if sport:
                query += " AND UPPER(sport) = %s"
                params.append(sport.upper())
            
            # Add team filter - match either team or opposing_team
            # Use case-insensitive matching for tennis (ATP/WTA) to handle name variations
            if team_names and len(team_names) > 0:
                if sport and sport.upper() in ['ATP', 'WTA']:
                    # For tennis, use case-insensitive matching
                    conditions = []
                    for team_name in team_names:
                        conditions.append(f"(UPPER(team) = UPPER(%s) OR UPPER(opposing_team) = UPPER(%s))")
                        params.append(team_name)
                        params.append(team_name)
                    query += " AND (" + " OR ".join(conditions) + ")"
                else:
                    # For team sports, use exact match (case-sensitive)
                    placeholders = ','.join(['%s'] * len(team_names))
                    query += f" AND (team IN ({placeholders}) OR opposing_team IN ({placeholders}))"
                    params.extend(team_names)
                    params.extend(team_names)  # Add twice for both team and opposing_team
            
            query += " ORDER BY timestamp_ms ASC"
            
            cursor.execute(query, tuple(params))
            self.processed_data = cursor.fetchall()
            
            # Process to remove LOCKED records
            self.process_deltas()
            
            return len(self.processed_data) > 0
            
        except Exception as e:
            print(f"Error loading Deltas data: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def process_deltas(self):
        """Remove LOCKED and invalid records"""
        valid_records = []
        for record in self.processed_data:
            bbo_price = record.get('bbo_price')
            percentage = record.get('percentage')
            total_spread = record.get('total_spread')
            
            # Check for LOCKED (case-insensitive) in percentage and total_spread
            percentage_str = str(percentage).upper() if percentage is not None else ''
            total_spread_str = str(total_spread).upper() if total_spread is not None else ''
            
            is_valid = (
                'LOCKED' not in percentage_str and
                'LOCKED' not in total_spread_str and
                bbo_price is not None and
                bbo_price != 0 and
                bbo_price != 0.0
            )
            
            if is_valid:
                valid_records.append(record)
        
        self.processed_data = valid_records
    
    def get_deltas_data(self, team_abbr: str, fill_timestamp: int) -> Dict:
        """
        Get closest before/after deltas data for a team or player.
        
        Args:
            team_abbr: Team abbreviation (e.g., 'LAL') or player name (e.g., 'Djokovic')
            fill_timestamp: Fill timestamp in milliseconds
        
        Returns:
            Dict with 'deltas_before' and 'deltas_after' data
        """
        def is_team_match(record, team_name):
            return record.get('team') == team_name or record.get('team', '').upper() == team_name.upper()

        def get_closest_record(team_name, target_timestamp):
            candidate_records = [
                r for r in self.processed_data
                if is_team_match(r, team_name)
            ]
            if not candidate_records:
                return None
            return min(
                candidate_records,
                key=lambda r: abs(r.get('timestamp_ms', 0) - target_timestamp)
            )
        
        # Filter by team (matches team column)
        # For tennis, team_abbr is a player name; for team sports, it's an abbreviation
        matching_records = [
            r for r in self.processed_data
            if is_team_match(r, team_abbr)
        ]
        
        if not matching_records:
            return {'deltas_before': None, 'deltas_after': None}
        
        # Find closest before and after
        before_records = [r for r in matching_records if r.get('timestamp_ms', 0) < fill_timestamp]
        after_records = [r for r in matching_records if r.get('timestamp_ms', 0) > fill_timestamp]
        
        deltas_before = max(before_records, key=lambda x: x.get('timestamp_ms', 0)) if before_records else None
        deltas_after = min(after_records, key=lambda x: x.get('timestamp_ms', float('inf'))) if after_records else None
        
        # Add opponent percentage (matching opposing team record at closest timestamp)
        if deltas_before:
            opposing_team = deltas_before.get('opposing_team')
            if opposing_team:
                opponent_record = get_closest_record(opposing_team, deltas_before.get('timestamp_ms', 0))
                if opponent_record:
                    deltas_before['opponent_percentage'] = opponent_record.get('percentage')
        
        if deltas_after:
            opposing_team = deltas_after.get('opposing_team')
            if opposing_team:
                opponent_record = get_closest_record(opposing_team, deltas_after.get('timestamp_ms', 0))
                if opponent_record:
                    deltas_after['opponent_percentage'] = opponent_record.get('percentage')
        
        # Add seconds_from_fill calculation and convert datetime to string for JSON serialization
        if deltas_before:
            time_diff = fill_timestamp - deltas_before.get('timestamp_ms', 0)
            deltas_before['seconds_from_fill'] = round(time_diff / 1000.0, 3)
            # Convert datetime to ISO string if present
            if 'timestamp' in deltas_before and isinstance(deltas_before['timestamp'], datetime):
                deltas_before['timestamp'] = deltas_before['timestamp'].isoformat()
        
        if deltas_after:
            time_diff = deltas_after.get('timestamp_ms', 0) - fill_timestamp
            deltas_after['seconds_from_fill'] = round(time_diff / 1000.0, 3)
            # Convert datetime to ISO string if present
            if 'timestamp' in deltas_after and isinstance(deltas_after['timestamp'], datetime):
                deltas_after['timestamp'] = deltas_after['timestamp'].isoformat()
        
        return {
            'deltas_before': deltas_before,
            'deltas_after': deltas_after
        }

# Global loader instance
trade_feed_loader = None

# Cache for processed results
cache_lock = threading.Lock()
cached_data = None
cache_timestamp = None
cache_params = None
CACHE_DURATION = 300  # Cache for 5 minutes
last_internal_id = None  # Track last internal_id for incremental fetching

# Request locking to prevent concurrent requests
request_lock = threading.Lock()
is_processing = False

def convert_datetime_to_string(obj):
    """
    Recursively convert all datetime objects in a data structure to ISO format strings.
    This ensures JSON serialization works correctly.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_datetime_to_string(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_to_string(item) for item in obj]
    else:
        return obj


def deduplicate_fills(fills_data):
    """
    Deduplicate fills using the same composite key as the database: (timestamp_ms, internal_id, fill_value)
    Returns a list of unique fills, keeping the first occurrence of each duplicate.
    """
    if not fills_data:
        return []
    
    seen = set()
    unique_fills = []
    duplicates_count = 0
    
    for fill in fills_data:
        # Extract timestamp_ms
        timestamp_ms = fill.get('timestamp_ms')
        if timestamp_ms is None:
            # Try to calculate from fill_tx_ts
            fill_tx_ts = fill.get('fill_tx_ts') or fill.get('full_trade_data', {}).get('fill_tx_ts')
            if fill_tx_ts:
                try:
                    if 'Z' in fill_tx_ts:
                        dt_utc = datetime.fromisoformat(fill_tx_ts.replace('Z', '+00:00'))
                    elif '+' in fill_tx_ts or fill_tx_ts.count('-') > 2:
                        dt_utc = datetime.fromisoformat(fill_tx_ts)
                    else:
                        dt_naive = datetime.fromisoformat(fill_tx_ts)
                        dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                    timestamp_ms = int(dt_utc.timestamp() * 1000)
                except:
                    timestamp_ms = None
        
        # Extract internal_id (always extract, regardless of timestamp_ms)
        full_trade = fill.get('full_trade_data', {})
        internal_id = full_trade.get('id') or full_trade.get('internal_id')
        if not internal_id:
            internal_id = fill.get('id') or fill.get('internal_id')
        
        # Extract fill_value (always extract, regardless of timestamp_ms)
        fill_value = fill.get('Fill Value')
        if fill_value is None or fill_value == '':
            last_fill_value = full_trade.get('last_fill_value', {})
            if isinstance(last_fill_value, dict):
                fill_value = last_fill_value.get('amount')
            else:
                fill_value = last_fill_value
        if fill_value is None or fill_value == '':
            fill_value = 0
        
        # Convert fill_value to numeric for comparison
        try:
            fill_value = float(fill_value) if fill_value else 0.0
        except:
            fill_value = 0.0
        
        # Create composite key (same as database)
        if timestamp_ms is not None and internal_id is not None:
            composite_key = (timestamp_ms, internal_id, fill_value)
            if composite_key not in seen:
                seen.add(composite_key)
                unique_fills.append(fill)
            else:
                duplicates_count += 1
        else:
            # If we can't create a proper key, include it anyway (better to show than hide)
            unique_fills.append(fill)
    
    if duplicates_count > 0:
        print(f"🔍 Deduplicated {duplicates_count} duplicate fills (kept {len(unique_fills)} unique)")
    
    return unique_fills


def initialize_trade_feed_loader():
    """Initialize the trade feed loader"""
    global trade_feed_loader
    try:
        trade_feed_loader = TradeFeedDataLoader()
        print("✅ Trade Feed Data Loader initialized")
        return True
    except Exception as e:
        print(f"❌ Failed to initialize Trade Feed Data Loader: {e}")
        return False

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        'message': 'Analyzer3 API - Simple TradeFeed Loader',
        'endpoints': {
            '/tradefeed': 'Get tradefeed data (no deltas, no polymarket matching)',
            '/health': 'Health check'
        }
    })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'loader_initialized': trade_feed_loader is not None
    })

@app.route('/fills', methods=['GET', 'OPTIONS'])
def get_fills():
    """Get fills from in-memory cache - simple endpoint for frontend"""
    # Handle OPTIONS request for CORS preflight
    if request.method == 'OPTIONS':
        return '', 200
    
    global cached_data, cache_timestamp
    
    # Fast path - make a quick copy to avoid holding lock during JSON serialization
    try:
        with cache_lock:
            # Make a copy of the data to avoid holding lock during JSON serialization
            if cached_data is not None:
                data_copy = list(cached_data)  # Create a new list copy
            else:
                data_copy = None
            timestamp_copy = cache_timestamp
        
        if data_copy is not None and len(data_copy) > 0:
            cache_age = time.time() - timestamp_copy if timestamp_copy else 0
            print(f"📦 /fills: Returning {len(data_copy)} records from in-memory cache (age: {cache_age:.1f}s)")
            return jsonify({
                'data': data_copy,
                'count': len(data_copy),
                'cache_age_seconds': int(cache_age),
                'loaded': True
            })
        else:
            print(f"📦 /fills: No cached data available (cached_data={data_copy}, cache_timestamp={timestamp_copy})")
            return jsonify({
                'data': [],
                'count': 0,
                'loaded': False,
                'message': 'No data available. Process data first using /tradefeed endpoint.'
            }), 200
    except Exception as e:
        print(f"❌ Error in /fills endpoint: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'data': [],
            'count': 0,
            'loaded': False,
            'error': str(e)
        }), 500

@app.route('/tradefeed/stream', methods=['GET'])
def get_tradefeed_stream():
    """Stream tradefeed data with SSE progress updates"""
    hours_back = request.args.get('hours_back', type=int)
    limit = request.args.get('limit', default=3000, type=int)
    
    def generate():
        try:
            # Send initial message
            yield f"data: {json.dumps({'type': 'start', 'message': 'Starting tradefeed fetch...'})}\n\n"
            
            # Initialize loader if needed
            global trade_feed_loader
            if trade_feed_loader is None:
                if not initialize_trade_feed_loader():
                    yield f"data: {json.dumps({'type': 'error', 'message': 'Failed to initialize trade feed loader'})}\n\n"
                    return
            
            # Create a queue to collect progress updates
            import queue
            progress_queue = queue.Queue()
            
            def progress_callback_wrapper(progress_info):
                progress_queue.put(progress_info)
            
            # Start processing in a separate thread
            result_data = []
            error_occurred = [False]
            error_message = [None]
            
            def process_data():
                try:
                    # Get last_internal_id from database cache for incremental fetching
                    global last_internal_id, cached_data, cache_params
                    incremental_last_id = None
                    current_params = (hours_back, limit)
                    has_cached_data = False
                    
                    # Check if we have cached data
                    with cache_lock:
                        if cached_data and len(cached_data) > 0 and cache_params == current_params:
                            has_cached_data = True
                            # Fallback to in-memory cache if database didn't have it
                            if not incremental_last_id:
                                # Find the maximum internal_id from cached data
                                max_id = None
                                for fill in cached_data:
                                    full_trade = fill.get('full_trade_data', {})
                                    trade_id = full_trade.get('id') or full_trade.get('internal_id')
                                    if trade_id:
                                        if max_id is None or trade_id > max_id:
                                            max_id = trade_id
                                if max_id:
                                    incremental_last_id = max_id
                                    print(f"🔄 SSE: Using last_internal_id={incremental_last_id} from in-memory cache")
                    
                    # Quick check: If we have cached data and incremental_last_id, do a quick fetch to see if there are new trades
                    if has_cached_data and incremental_last_id:
                        # Quick check: Fetch just 1 page (500 trades) to see if there are any new trades
                        print(f"🔍 SSE: Quick check for new trades (id > {incremental_last_id})...")
                        quick_trades = trade_feed_loader.fetch_trades(
                            page_size=500,
                            total_trades=500,  # Just check first page
                            last_internal_id=incremental_last_id
                        )
                        
                        if not quick_trades or len(quick_trades) == 0:
                            # No new trades found - return cached data immediately without processing
                            print(f"✅ SSE: No new trades found (id > {incremental_last_id}), returning cached data immediately")
                            with cache_lock:
                                result_data.extend(cached_data)
                            return
                        else:
                            print(f"🆕 SSE: Found {len(quick_trades)} new trades, will process only new trades...")
                    
                    # Fetch only new trades (incremental) - this will process market slugs
                    # But only process market slugs that have new trades, not all of them
                    fill_data = trade_feed_loader.read_fill_data(
                        limit=limit, 
                        hours_back=hours_back,
                        progress_callback=progress_callback_wrapper,
                        last_internal_id=incremental_last_id
                    )
                    
                    # If we got no new fills after processing, just return cached data
                    if not fill_data or len(fill_data) == 0:
                        print(f"✅ SSE: No new fills after processing, returning cached data")
                        with cache_lock:
                            result_data.extend(cached_data)
                        return
                    
                    # Merge with cached data if doing incremental fetch
                    if incremental_last_id and cached_data and cache_params == current_params:
                        # Combine cached data with new data, avoiding duplicates
                        existing_ids = {fill.get('full_trade_data', {}).get('id') or fill.get('full_trade_data', {}).get('internal_id') 
                                       for fill in cached_data if fill.get('full_trade_data')}
                        
                        new_fills = []
                        for fill in fill_data:
                            trade_id = fill.get('full_trade_data', {}).get('id') or fill.get('full_trade_data', {}).get('internal_id')
                            if trade_id and trade_id not in existing_ids:
                                new_fills.append(fill)
                        
                        if new_fills:
                            print(f"🔄 SSE: Merging {len(new_fills)} new fills with {len(cached_data)} cached fills")
                            result_data.extend(cached_data + new_fills)
                        else:
                            print(f"🔄 SSE: No new fills found, using cached data ({len(cached_data)} fills)")
                            result_data.extend(cached_data)
                    else:
                        # No cached data or not incremental - use fetched data as-is
                        result_data.extend(fill_data)
                except Exception as e:
                    error_occurred[0] = True
                    error_message[0] = str(e)
                    import traceback
                    traceback.print_exc()
            
            import threading
            process_thread = threading.Thread(target=process_data)
            process_thread.start()
            
            # Stream progress updates while processing
            while process_thread.is_alive():
                try:
                    # Check for progress updates (non-blocking)
                    try:
                        progress_info = progress_queue.get_nowait()
                        progress_json = json.dumps(progress_info)
                        yield f"data: {progress_json}\n\n"
                        # Force flush by yielding empty line
                        import sys
                        sys.stdout.flush()
                    except queue.Empty:
                        pass
                    
                    time.sleep(0.05)  # Smaller delay for more responsive updates
                except Exception as e:
                    print(f"Error in progress streaming: {e}")
                    break
            
            # Wait for thread to complete
            process_thread.join()
            
            # Get any remaining progress updates
            while not progress_queue.empty():
                try:
                    progress_info = progress_queue.get_nowait()
                    yield f"data: {json.dumps(progress_info)}\n\n"
                except queue.Empty:
                    break
            
            if error_occurred[0]:
                yield f"data: {json.dumps({'type': 'error', 'message': error_message[0]})}\n\n"
                return
            
            # Check if we have data (either from quick check or full processing)
            if not result_data or len(result_data) == 0:
                # No data at all - try to return cached data if available
                global cached_data, cache_params
                current_params = (hours_back, limit)
                with cache_lock:
                    if cached_data and len(cached_data) > 0 and cache_params == current_params:
                        print(f"⚠️  SSE: No new data found, returning cached data")
                        result_data = cached_data
                    else:
                        yield f"data: {json.dumps({'type': 'error', 'message': 'No data available'})}\n\n"
                        return
            
            # Send completion message with data
            yield f"data: {json.dumps({'type': 'complete', 'message': 'Processing complete'})}\n\n"
            
            # Filter merged data by hours_back if specified (to ensure we only return data within the requested window)
            if hours_back:
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
                original_count = len(result_data)
                filtered_result_data = []
                for fill in result_data:
                    fill_tx_ts = fill.get('fill_tx_ts') or fill.get('full_trade_data', {}).get('fill_tx_ts')
                    if fill_tx_ts:
                        try:
                            if isinstance(fill_tx_ts, str):
                                if 'Z' in fill_tx_ts:
                                    fill_dt = datetime.fromisoformat(fill_tx_ts.replace('Z', '+00:00'))
                                elif '+' in fill_tx_ts or fill_tx_ts.count('-') > 2:
                                    fill_dt = datetime.fromisoformat(fill_tx_ts)
                                else:
                                    dt_naive = datetime.fromisoformat(fill_tx_ts)
                                    fill_dt = dt_naive.replace(tzinfo=timezone.utc)
                            else:
                                fill_dt = fill_tx_ts
                            
                            if fill_dt >= cutoff_time:
                                filtered_result_data.append(fill)
                        except:
                            # If we can't parse timestamp, include it (better to show than hide)
                            filtered_result_data.append(fill)
                    else:
                        # No timestamp, include it anyway
                        filtered_result_data.append(fill)
                
                result_data = filtered_result_data
                if original_count != len(result_data):
                    print(f"📅 SSE: Filtered merged data to last {hours_back} hours: {len(result_data)}/{original_count} records")
            
            # Deduplicate fills using composite key (timestamp_ms, internal_id, fill_value)
            print(f"🔄 SSE: Starting deduplication of {len(result_data)} records...")
            fill_data = deduplicate_fills(result_data)
            print(f"✅ SSE: Deduplication complete: {len(fill_data)} unique records")
            
            # Sort final data by fill_tx_ts (newest first)
            print(f"🔄 SSE: Sorting {len(fill_data)} records...")
            fill_data.sort(key=lambda x: x.get('fill_tx_ts', ''), reverse=True)
            print(f"✅ SSE: Sorting complete")
            
            # Update in-memory cache with merged data
            # Convert all datetime objects to strings for JSON serialization
            fill_data_serializable = convert_datetime_to_string(fill_data)
            
            global cache_timestamp, last_internal_id
            with cache_lock:
                current_params = (hours_back, limit)
                # Only update cache if we have data (don't overwrite good cache with empty data)
                if fill_data_serializable and len(fill_data_serializable) > 0:
                    cached_data = fill_data_serializable
                    cache_timestamp = time.time()
                    cache_params = current_params
                else:
                    # If no data, keep existing cache but update timestamp to indicate we tried
                    if cached_data is None or len(cached_data) == 0:
                        cached_data = fill_data_serializable
                    cache_timestamp = time.time()
                    cache_params = current_params
                
                # Update last_internal_id from the newest fill
                if fill_data and len(fill_data) > 0:
                    newest_fill = fill_data[0]
                    newest_trade = newest_fill.get('full_trade_data', {})
                    newest_id = newest_trade.get('id') or newest_trade.get('internal_id')
                    if newest_id:
                        last_internal_id = newest_id
                
                print(f"💾 SSE: Saved {len(fill_data_serializable)} records to in-memory cache (available at /fills endpoint)")
                print(f"✅ VERIFY SSE: cached_data length = {len(cached_data) if cached_data else 0}, cache_timestamp = {cache_timestamp}")
            
            yield f"data: {json.dumps({'type': 'data', 'data': fill_data_serializable, 'count': len(fill_data_serializable)})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
            import traceback
            traceback.print_exc()
    
    return Response(stream_with_context(generate()), mimetype='text/event-stream')

@app.route('/tradefeed', methods=['GET', 'OPTIONS'])
def get_tradefeed():
    """Get tradefeed data - simple loader without deltas or polymarket matching
    
    Query Parameters:
    - hours_back: Number of hours to look back (optional)
    - limit: Maximum number of trades to fetch (default: 3000)
    - cache_only: If 'true', return cached data only without processing (default: 'false')
    """
    # Handle OPTIONS request for CORS preflight
    if request.method == 'OPTIONS':
        return '', 200
    
    global trade_feed_loader, cached_data, cache_timestamp, cache_params, is_processing
    
    # Get query parameters
    hours_back_param = request.args.get('hours_back', None)
    hours_back = int(hours_back_param) if hours_back_param else None
    limit = request.args.get('limit', 3000, type=int)
    cache_only = request.args.get('cache_only', 'false').lower() == 'true'
    
    # Create cache key from parameters
    current_params = (hours_back, limit)
    
    # Check if we should use cache only (no refresh)
    if cache_only:
        # Return cached data only - filter by hours_back if provided
        with cache_lock:
            if cached_data is not None and len(cached_data) > 0:
                cache_age = time.time() - cache_timestamp if cache_timestamp else float('inf')
                
                # Filter cached data by hours_back if requested (even if cache was built with different params)
                filtered_data = cached_data
                if hours_back:
                    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
                    filtered_data = []
                    for fill in cached_data:
                        fill_tx_ts = fill.get('fill_tx_ts') or fill.get('full_trade_data', {}).get('fill_tx_ts')
                        if fill_tx_ts:
                            try:
                                if isinstance(fill_tx_ts, str):
                                    if 'Z' in fill_tx_ts:
                                        fill_dt = datetime.fromisoformat(fill_tx_ts.replace('Z', '+00:00'))
                                    elif '+' in fill_tx_ts or fill_tx_ts.count('-') > 2:
                                        fill_dt = datetime.fromisoformat(fill_tx_ts)
                                    else:
                                        dt_naive = datetime.fromisoformat(fill_tx_ts)
                                        fill_dt = dt_naive.replace(tzinfo=timezone.utc)
                                else:
                                    fill_dt = fill_tx_ts
                                
                                if fill_dt >= cutoff_time:
                                    filtered_data.append(fill)
                            except:
                                # If we can't parse timestamp, include it (better to show than hide)
                                filtered_data.append(fill)
                        else:
                            # No timestamp, include it anyway
                            filtered_data.append(fill)
                
                # Deduplicate before returning
                filtered_data = deduplicate_fills(filtered_data)
                
                print(f"📦 Returning cached data only (cache_only=true, age: {cache_age:.1f}s, filtered to {len(filtered_data)}/{len(cached_data)} records for {hours_back or 'all'} hours)")
                return jsonify({
                    'data': filtered_data,
                    'pagination': {
                        'page': 1,
                        'per_page': len(filtered_data),
                        'total_records': len(filtered_data),
                        'total_pages': 1
                    },
                    'data_source': 'Trade Feed API (Simple Loader - Cached Only)',
                    'loaded': True,
                    'from_cache': True,
                    'cache_age_seconds': int(cache_age),
                    'message': 'Returning cached data only (cache_only=true)'
                })
            else:
                # No cached data available
                return jsonify({
                    'data': [],
                    'pagination': {
                        'page': 1,
                        'per_page': 0,
                        'total_records': 0,
                        'total_pages': 0
                    },
                    'data_source': 'Trade Feed API (Simple Loader)',
                    'loaded': False,
                    'from_cache': False,
                    'message': 'No cached data available. Use cache_only=false to fetch new data.'
                }), 200
    
    # Check cache first - always return cached data if available (even if expired)
    # This prevents browser refresh from triggering a new fetch
    with cache_lock:
        if cached_data is not None and cache_params == current_params:
            cache_age = time.time() - cache_timestamp if cache_timestamp else float('inf')
            # Always return cached data if available, regardless of age
            # This ensures browser refresh doesn't trigger a new fetch
            # Deduplicate before returning
            deduplicated_cached_data = deduplicate_fills(cached_data)
            print(f"📦 Returning cached data (age: {cache_age:.1f}s, {len(deduplicated_cached_data)} unique records)")
            return jsonify({
                'data': deduplicated_cached_data,
                'pagination': {
                    'page': 1,
                    'per_page': len(cached_data),
                    'total_records': len(cached_data),
                    'total_pages': 1
                },
                'data_source': 'Trade Feed API (Simple Loader)',
                'loaded': True,
                'from_cache': True,
                'cache_age_seconds': int(cache_age)
            })
    
    # Initialize loader if not already initialized
    if trade_feed_loader is None:
        if not initialize_trade_feed_loader():
            return jsonify({
                'data': [],
                'message': 'Failed to initialize trade feed loader. Check EMAIL, PASSWORD, or BEARER_TOKEN in .env',
                'loaded': False
            }), 500
    
    # Process request with locking - check and set is_processing atomically
    with request_lock:
        # Check if already processing (atomic check within lock)
        if is_processing:
            # If processing, always return cached data if available (even if expired)
            # This matches analyzer.py behavior - always return cache when processing
            with cache_lock:
                if cached_data is not None and cache_params == current_params:
                    cache_age = time.time() - cache_timestamp if cache_timestamp else 0
                    print(f"⚠️  Request already processing, returning cached data (age: {int(cache_age)}s)")
                    return jsonify({
                        'data': cached_data,
                        'pagination': {
                            'page': 1,
                            'per_page': len(cached_data),
                            'total_records': len(cached_data),
                            'total_pages': 1
                        },
                        'data_source': 'Trade Feed API (Simple Loader - Cached - Processing in progress)',
                        'loaded': True,
                        'from_cache': True,
                        'cache_age_seconds': int(cache_age),
                        'message': 'Another request is processing. Returning cached data.'
                    })
                else:
                    # No cached data available, but still return empty array with 200 (not 429)
                    # This matches analyzer.py behavior - graceful degradation
                    print(f"⚠️  Request already processing, no cached data available")
                    return jsonify({
                        'data': [],
                        'pagination': {
                            'page': 1,
                            'per_page': 0,
                            'total_records': 0,
                            'total_pages': 0
                        },
                        'data_source': 'Trade Feed API (Simple Loader)',
                        'loaded': False,
                        'from_cache': False,
                        'message': 'Another request is processing. Please wait and refresh.'
                    }), 200  # Return 200 instead of 429 for better UX
        
        # Set processing flag atomically (within the same lock)
        is_processing = True
        try:
            if hours_back:
                print(f"📊 Fetching tradefeed data (last {hours_back} hours, limit {limit})...")
            else:
                print(f"📊 Fetching tradefeed data (last {limit} trades, no time filter)...")
            
            # Get last_internal_id from cached data for incremental fetching
            global last_internal_id
            incremental_last_id = None
            with cache_lock:
                if cached_data and len(cached_data) > 0:
                    # Find the maximum internal_id from cached data
                    # Try to get id from full_trade_data if available
                    max_id = None
                    for fill in cached_data:
                        full_trade = fill.get('full_trade_data', {})
                        trade_id = full_trade.get('id') or full_trade.get('internal_id')
                        if trade_id:
                            if max_id is None or trade_id > max_id:
                                max_id = trade_id
                    if max_id:
                        incremental_last_id = max_id
                        print(f"🔄 Incremental fetch: Using last_internal_id={incremental_last_id} from cached data")
            
            # Fetch trades - use incremental fetching if we have cached data
            fill_data = trade_feed_loader.read_fill_data(
                limit=limit, 
                hours_back=hours_back,
                last_internal_id=incremental_last_id
            )
            
            # Merge with cached data if doing incremental fetch
            if incremental_last_id and cached_data:
                # Combine cached data with new data
                print(f"🔄 Merging {len(fill_data)} new fills with {len(cached_data)} cached fills")
                fill_data = cached_data + fill_data
            
            # Filter merged data by hours_back if specified (to ensure we only return data within the requested window)
            if hours_back:
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
                original_count = len(fill_data)
                filtered_fill_data = []
                for fill in fill_data:
                    fill_tx_ts = fill.get('fill_tx_ts') or fill.get('full_trade_data', {}).get('fill_tx_ts')
                    if fill_tx_ts:
                        try:
                            if isinstance(fill_tx_ts, str):
                                if 'Z' in fill_tx_ts:
                                    fill_dt = datetime.fromisoformat(fill_tx_ts.replace('Z', '+00:00'))
                                elif '+' in fill_tx_ts or fill_tx_ts.count('-') > 2:
                                    fill_dt = datetime.fromisoformat(fill_tx_ts)
                                else:
                                    dt_naive = datetime.fromisoformat(fill_tx_ts)
                                    fill_dt = dt_naive.replace(tzinfo=timezone.utc)
                            else:
                                fill_dt = fill_tx_ts
                            
                            if fill_dt >= cutoff_time:
                                filtered_fill_data.append(fill)
                        except:
                            # If we can't parse timestamp, include it (better to show than hide)
                            filtered_fill_data.append(fill)
                else:
                        # No timestamp, include it anyway
                        filtered_fill_data.append(fill)
                
                fill_data = filtered_fill_data
                if original_count != len(fill_data):
                    print(f"📅 Filtered merged data to last {hours_back} hours: {len(fill_data)}/{original_count} records")
            
            # Deduplicate fills using composite key (timestamp_ms, internal_id, fill_value)
            print(f"🔄 Starting deduplication of {len(fill_data)} records...")
            fill_data = deduplicate_fills(fill_data)
            print(f"✅ Deduplication complete: {len(fill_data)} unique records")
            
            # Sort by fill_tx_ts (newest first)
            print(f"🔄 Sorting {len(fill_data)} records...")
            fill_data.sort(key=lambda x: x.get('fill_tx_ts', ''), reverse=True)
            print(f"✅ Sorting complete")
            
            # Update last_internal_id and cache
            with cache_lock:
                # Update last_internal_id from the newest fill
                if fill_data and len(fill_data) > 0:
                    newest_fill = fill_data[0]
                    newest_trade = newest_fill.get('full_trade_data', {})
                    newest_id = newest_trade.get('id') or newest_trade.get('internal_id')
                    if newest_id:
                        last_internal_id = newest_id
                
                # Only update cache if we have data (don't overwrite good cache with empty data)
                if fill_data and len(fill_data) > 0:
                    cached_data = fill_data
                    cache_timestamp = time.time()
                    cache_params = current_params
                    print(f"💾 Cached {len(fill_data)} records to in-memory cache (available at /fills endpoint)")
                else:
                    # If no data, keep existing cache
                    if cached_data is None or len(cached_data) == 0:
                        cached_data = fill_data
                    print(f"⚠️  No new data to cache, keeping existing cache ({len(cached_data) if cached_data else 0} records)")
                print(f"✅ VERIFY: cached_data length = {len(cached_data) if cached_data else 0}, cache_timestamp = {cache_timestamp}")
            
            return jsonify({
                'data': fill_data,
                'pagination': {
                    'page': 1,
                    'per_page': len(fill_data),
                    'total_records': len(fill_data),
                    'total_pages': 1
                },
                'data_source': 'Trade Feed API (Simple Loader)',
                'loaded': True,
                'from_cache': False
            })
            
        except Exception as e:
            print(f"Error fetching tradefeed: {e}")
            import traceback
            traceback.print_exc()
            
            # On error, return cached data if available
            with cache_lock:
                if cached_data is not None and cache_params == current_params:
                    # Deduplicate before returning
                    deduplicated_error_data = deduplicate_fills(cached_data)
                    print(f"⚠️  Error occurred, returning cached data ({len(deduplicated_error_data)} unique records)")
                    return jsonify({
                        'data': deduplicated_error_data,
                        'pagination': {
                            'page': 1,
                            'per_page': len(cached_data),
                            'total_records': len(cached_data),
                            'total_pages': 1
                        },
                        'data_source': 'Trade Feed API (Simple Loader)',
                        'loaded': True,
                        'from_cache': True,
                        'message': f'Error occurred: {str(e)}. Returning cached data.'
                    })
            
            return jsonify({
                'data': [],
                'message': f'Error fetching tradefeed: {str(e)}',
                'loaded': False
            }), 500
        finally:
            is_processing = False

if __name__ == '__main__':
    # Initialize loader on startup
    initialize_trade_feed_loader()
    
    # Run on port from environment (Render sets this automatically)
    port = int(os.getenv('PORT', 9006))
    # Debug mode should be False in production
    debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    print(f"🚀 Starting Analyzer3 on port {port} (debug={debug_mode})")
    # Disable reloader to prevent duplicate executions
    app.run(host='0.0.0.0', port=port, debug=debug_mode, use_reloader=False)
