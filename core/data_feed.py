import aiohttp
import ccxt.async_support as ccxt
import pandas as pd
import yaml
import asyncio
import time

class DataFeed:
    """Centralized market data provider for Jakarta Exhaustion System"""
    
    def __init__(self, api_config):
        self.config = api_config          # API configuration
        self.bitget = self._initialize_bitget()    # CCXT exchange instance
        self.session = aiohttp.ClientSession()     # HTTP client for API calls
        self.last_fetch_time = 0                   # Rate limiting tracker
        
    def _initialize_bitget(self):
        """Initialize CCXT Bitget exchange instance"""
        return ccxt.bitget({
            'enableRateLimit': True,               # Built-in rate limiting
            'options': {'defaultType': 'swap'}    # Futures trading
        })
    
        # Safely add credentials if they exist
        if self.config['bitget'].get('apiKey'):
            config.update({
                'apiKey': self.config['bitget']['apiKey'],
                'secret': self.config['bitget']['secret'],
                'password': self.config['bitget']['password']
            })
        return ccxt.bitget(config)
    
    async def _rate_limited_fetch(self, url: str):
        """
        Enforce 1 request per second policy
        Returns JSON response or None on failure
        """
        # Calculate time since last request
        elapsed = time.time() - self.last_fetch_time
        if elapsed < 1.1:  # Enforce 1.1s delay between requests
            await asyncio.sleep(1.1 - elapsed)
        self.last_fetch_time = time.time()
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                print(f"⚠️ API Error: HTTP {response.status} for {url}")
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"🚨 Network Error: {str(e)}")
            return None
        except Exception as e:
            print(f"🚨 Unexpected Error: {str(e)}")
            return None
    
    async def fetch_market_tickers(self) -> list:
        """Fetch all USDT futures tickers from Bitget"""
        url = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
        data = await self._rate_limited_fetch(url)
        
        if not data or 'data' not in data:
            return []
        
        processed_tickers = []
        for item in data['data']:
            try:
                # Skip test symbols and invalid entries
                if "TEST" in item['symbol'] or not item.get('lastPr'):
                    continue
                    
                # Extract and convert values
                ticker_data = {
                    'symbol': item['symbol'],
                    'last_price': float(item['lastPr']),
                    '24h_volume': float(item['usdtVolume']),
                    '24h_change': float(item['changeUtc24h']),
                    'bid_ask_spread': (float(item['askPr']) - float(item['bidPr'])) / float(item['askPr']),
                    'order_book_depth': float(item['baseVolume'])
                }
                processed_tickers.append(ticker_data)
            except (KeyError, TypeError, ValueError) as e:
                print(f"⏩ Skipping {item.get('symbol')}: {str(e)}")
                continue
                
        return processed_tickers

    async def fetch_candles(self, symbol: str, timeframe: str = '15m', limit: int = 100) -> list:
        """Fetch OHLCV data for a symbol"""
        try:
            return await self.bitget.fetch_ohlcv(
                symbol, 
                timeframe, 
                limit=limit
            )
        except ccxt.NetworkError as e:
            print(f"🌐 Network Error fetching candles: {str(e)}")
            return []
        except ccxt.ExchangeError as e:
            print(f"💱 Exchange Error fetching candles: {str(e)}")
            return []
        except Exception as e:
            print(f"🚨 Unexpected Error: {str(e)}")
            return []
    
    async def fetch_funding_rate(self, symbol: str) -> float:
        """Fetch current funding rate for a symbol"""
        url = (f"https://api.bitget.com/api/v2/mix/market/current-fund-rate"
               f"?symbol={symbol}&productType=usdt-futures")
        data = await self._rate_limited_fetch(url)
        
        # Validate response structure
        if not data or data.get('code') != '00000' or not data.get('data'):
            error_msg = data.get('msg', 'No data') if data else 'No response'
            print(f"⏩ Funding rate error for {symbol}: {error_msg}")
            return None
            
        try:
            # Handle list response structure
            if isinstance(data['data'], list) and data['data']:
                return float(data['data'][0]['fundingRate'])
            return None
        except (KeyError, TypeError, ValueError, IndexError) as e:
            print(f"⏩ Funding rate parse error: {str(e)}")
            return None
    
    async def fetch_liquidation_clusters(self, symbol: str) -> list:
        """Fetch recent liquidation clusters near current price"""
        url = (f"https://api.bitget.com/api/v2/mix/market/liquidation-history"
               f"?symbol={symbol}&productType=USDT-FUTURES&limit=50")
        data = await self._rate_limited_fetch(url)
        
        # Validate response
        if not data or data.get('code') != '00000' or not data.get('data'):
            error_msg = data.get('msg', 'No data') if data else 'No response'
            print(f"⏩ Liquidation error for {symbol}: {error_msg}")
            return []
        
        # Process liquidations
        liquidations = []
        for item in data['data']:
            try:
                liquidations.append({
                    'price': float(item['price']),
                    'size': float(item['size']),
                    'side': item['side'],
                    'time': pd.to_datetime(item['time'], unit='ms')
                })
            except (KeyError, TypeError, ValueError):
                continue
        
        return liquidations
    
    async def fetch_btc_dominance(self) -> float:
        """Fetch BTC market dominance from CoinGecko"""
        url = "https://api.coingecko.com/api/v3/global"
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data['data']['market_cap_percentage']['btc']
                print(f"⚠️ BTC Dominance HTTP Error: {response.status}")
                return 48.3  # Fallback to threshold value
        except Exception as e:
            print(f"⚠️ BTC Dominance Error: {str(e)}")
            return 48.3
    
    async def fetch_cryptopanic_news(self) -> list:
        """Fetch FUD news from CryptoPanic (without 'HUGE' tags)"""
        url = "https://cryptopanic.com/api/v1/posts/?auth_token=anonymous&filter=fud"
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return [
                        post for post in data.get('results', []) 
                        if 'HUGE' not in post.get('kind', '')
                    ]
                return []
        except Exception as e:
            print(f"⚠️ News Fetch Error: {str(e)}")
            return []
    
    async def fetch_spot_volume(self, symbol: str) -> float:
        """
        ESTIMATE spot volume for a symbol
        Note: Actual implementation would require proper spot market API
        """
        tickers = await self.fetch_market_tickers()
        futures_vol = next(
            (t['24h_volume'] for t in tickers if t['symbol'] == symbol), 
            0.0
        )
        return futures_vol * 0.8  # Placeholder estimation
    
    async def fetch_volatility_index(self) -> float:
        """Calculate simplified volatility index based on BTC hourly returns"""
        btc_candles = await self.fetch_candles('BTCUSDT', '1h', 24)
        if not btc_candles:
            return 0.0
            
        df = pd.DataFrame(
            btc_candles, 
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )
        df['returns'] = df['close'].pct_change()
        return df['returns'].std() * 100  # Percentage volatility
    
    async def fetch_usdt_minting(self) -> float:
        """Fetch USDT total supply from CoinGecko"""
        url = "https://api.coingecko.com/api/v3/coins/tether"
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return float(data['market_data']['total_supply'])
                return 0.0
        except Exception as e:
            print(f"⚠️ USDT Supply Error: {str(e)}")
            return 0.0
    
    async def close(self):
        """Cleanup resources: close connections"""
        await self.bitget.close()
        await self.session.close()


def process_candles(candles: list) -> pd.DataFrame:
    """Convert raw OHLCV data into structured DataFrame"""
    if not candles:
        return pd.DataFrame()
        
    df = pd.DataFrame(
        candles,
        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
    )
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df.set_index('timestamp')


async def test_data_feed():
    """Comprehensive test function for data feed components"""
    print("=== Jakarta Exhaustion Data Feed Test ===")
    feed = DataFeed()
    
    try:
        print("Testing critical endpoints...")
        
        # 1. Test market tickers
        print("\n🔍 Testing market tickers...")
        tickers = await feed.fetch_market_tickers()
        print(f"Retrieved {len(tickers)} tickers")
        if tickers:
            print(f"Sample: {tickers[0]['symbol']} - ${tickers[0]['last_price']}")
        
        # 2. Test BTC funding rate
        print("\n🔍 Testing funding rate...")
        funding = await feed.fetch_funding_rate('BTCUSDT')
        print(f"BTC Funding Rate: {funding*100 if funding else 'None'}%")
        
        # 3. Test liquidation clusters
        print("\n🔍 Testing liquidation clusters...")
        liq_clusters = await feed.fetch_liquidation_clusters('BTCUSDT')
        print(f"Retrieved {len(liq_clusters)} liquidation clusters")
        if liq_clusters:
            print(f"Nearest liquidation: ${liq_clusters[0]['price']}")
        
        # 4. Test BTC dominance
        print("\n🔍 Testing BTC dominance...")
        btc_dom = await feed.fetch_btc_dominance()
        print(f"BTC Dominance: {btc_dom}%")
        
    except Exception as e:
        print(f"🚨 Test failed: {str(e)}")
    finally:
        await feed.close()
        print("\n✅ Data feed closed successfully")


if __name__ == "__main__":
    asyncio.run(test_data_feed())