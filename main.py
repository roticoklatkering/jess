import asyncio
import argparse
import time
import yaml  
import pandas as pd
from core.data_feed import DataFeed, process_candles
from core.analytics_engine import AnalyticsEngine
from core.session_manager import SessionManager
from core.execution_handler import ExecutionHandler
from core.risk_system import RiskSystem

class JakartaTradingSystem:
    def __init__(self, test_mode: bool = False):
        """Initialize trading system components with configuration"""
        self.test_mode = test_mode
        
        # Load configuration files
        try:
            # Step 1: Load API configuration
            with open('config/api_config.yaml', 'r') as f:
                self.api_config = yaml.safe_load(f)
            
            # Step 2: Load trading thresholds
            with open('config/thresholds.yaml', 'r') as f:
                self.thresholds = yaml.safe_load(f)
                
            print("✅ Configuration files loaded successfully")
            
        except FileNotFoundError as e:
            print(f"⚠️ Critical error: Missing configuration file - {str(e)}")
            raise SystemExit("System cannot start without configuration files")
        except yaml.YAMLError as e:
            print(f"⚠️ Configuration syntax error: {str(e)}")
            raise SystemExit("Fix configuration files before restarting")
        except Exception as e:
            print(f"⚠️ Unexpected configuration error: {str(e)}")
            raise SystemExit("Fatal configuration error")
        
        # Core system modules with dependency injection
        # Step 3: Initialize components with configurations
        self.data_feed = DataFeed(self.api_config)          # Market data provider
        self.analytics = AnalyticsEngine(self.thresholds)   # Technical analysis engine
        self.session = SessionManager()                     # Session state manager
        self.execution = ExecutionHandler()                 # Order execution handler
        
        # Step 4: Initialize risk system with thresholds
        self.risk = RiskSystem(
            max_daily_drawdown=self.thresholds.get('max_daily_drawdown', 0.015),
            btc_atr_threshold=self.thresholds['btc_atr_1h']
        )
        
        # Session state variables
        self.selected_coins = []              # Selected coins for trading
        self.btc_volatility = 0.0            # BTC volatility metric
        self.test_start_time = time.time()    # Test mode timer
        
        if test_mode:
            print("\n*** TEST MODE ACTIVATED ***")
            print("Running with simulated orders and flexible session timing\n")
            # Print thresholds for verification
            print("Loaded Thresholds:")
            for key, value in self.thresholds.items():
                print(f"  {key}: {value}")

    async def run(self):
        """Main trading loop controlling state machine"""
        print("🚀 Starting Jakarta Exhaustion Trading System")
        
        while True:
            try:
                # Determine current session state
                if self.test_mode:
                    state = self.simulate_session_state()
                    next_state = self.get_next_test_state(state)
                    print(f"\n[TEST MODE] State: {state} | Next: {next_state}")
                else:
                    state = self.session.determine_session_state()
                    next_state, (hours, mins) = self.session.get_next_state_info()
                    print(f"\n[{state}] | Next: {next_state} in {hours}h {mins}m")
                
                # Execute state-specific actions
                if state == "PRE_SESSION":
                    await self.pre_session_checks()
                
                elif state == "SCANNING":
                    await self.coin_selection()
                
                elif state == "GOLDEN_HOUR" and self.risk.trading_allowed():
                    await self.execute_trades()
                
                elif state == "MANAGEMENT":
                    await self.manage_positions()
                
                elif state == "EXIT_WINDOW":
                    await self.exit_positions(partial=True)
                
                elif state == "SHUTDOWN":
                    await self.exit_positions(partial=False)
                    if not self.test_mode:
                        await self.session.wait_until_next_session()
                
                # State transition timing
                sleep_time = 10 if self.test_mode else min(
                    self.session.get_time_until_next_state() * 3600, 30
                )
                await asyncio.sleep(sleep_time)
            
            except Exception as e:
                print(f"⚠️ System error: {str(e)}")
                await asyncio.sleep(30)
    
    def simulate_session_state(self):
        """Simulate session state progression for testing"""
        elapsed = time.time() - self.test_start_time
        state_index = int(elapsed / 60) % 6  # Rotate states every 60 seconds
        states = [
            "PRE_SESSION", "SCANNING", "GOLDEN_HOUR", 
            "MANAGEMENT", "EXIT_WINDOW", "SHUTDOWN"
        ]
        return states[state_index]
    
    def get_next_test_state(self, current_state):
        """Get next state in test sequence"""
        states = ["PRE_SESSION", "SCANNING", "GOLDEN_HOUR", 
                "MANAGEMENT", "EXIT_WINDOW", "SHUTDOWN"]
        current_index = states.index(current_state)
        return states[(current_index + 1) % len(states)]
    
    async def pre_session_checks(self):
        """Perform pre-session checks per Jakarta v3.0 spec"""
        print("🔍 Running pre-session checks...")
        
        # 1. BTC Dominance check
        btc_dominance = await self.data_feed.fetch_btc_dominance()
        print(f"• BTC Dominance: {btc_dominance:.2f}%")
        
        # 2. BTC Volatility (ATR) check
        candles = await self.data_feed.fetch_candles('BTCUSDT', '1h', 2)
        if candles and not candles.empty and 'close' in candles.columns and 'atr' in candles.columns:
            self.btc_volatility = candles['atr'].iloc[-1] / candles['close'].iloc[-1]
            print(f"• BTC 1h ATR: {self.btc_volatility*100:.2f}%")
        else:
            self.btc_volatility = 0.01  # Default 1% volatility
            print("• Using default BTC volatility: 1.00%")
        
        # 3. Funding Rate check
        funding = await self.data_feed.fetch_funding_rate('BTCUSDT')
        print(f"• BTC Funding: {funding*100:.4f}%" if funding else "• Funding rate not available")
        
        # Update risk parameters
        self.risk.reset_daily()
        self.risk.check_btc_volatility(self.btc_volatility)
        
        # Test mode diagnostics
        if self.test_mode and not candles.empty:
            print("\n[TEST] Sample BTC Data:")
            print(candles[['open', 'high', 'low', 'close', 'volume']].tail())
    
    async def coin_selection(self):
        """Select coins according to v3.0 protocol"""
        print("🎯 Selecting coins...")
        self.selected_coins = []
        
        tickers = await self.data_feed.fetch_market_tickers()
        if not tickers:
            print("⚠️ No tickers received, using test coins")
            self.selected_coins = ['PEPEUSDT', 'WIFUSDT', 'BONKUSDT']
            return
            
        for ticker in tickers:
            # Volume filter ($20M-$35M)
            if not 20_000_000 < ticker.get('24h_volume', 0) < 35_000_000:
                continue
                
            # Price change filter (20-35%)
            if not 0.20 < ticker.get('24h_change', 0) < 0.35:
                continue
                
            # Priority to MEME coins (PEPE, WIF, BONK)
            symbol = ticker['symbol']
            if any(coin in symbol for coin in ['PEPE', 'WIF', 'BONK']):
                self.selected_coins.insert(0, symbol)  # High priority
            else:
                self.selected_coins.append(symbol)     # Standard priority
        
        # Limit for testing
        self.selected_coins = self.selected_coins[:5]
        print(f"✅ Selected coins: {self.selected_coins}")
    
    async def execute_trades(self):
        """Execute trades during golden hour window"""
        print("💼 Executing trades...")
        if not self.selected_coins:
            print("⏩ No coins selected, skipping execution")
            return
            
        for symbol in self.selected_coins:
            print(f"\n🔎 Processing {symbol}...")
            
            try:
                # 1. Fetch required market data
                candles = await self.data_feed.fetch_candles(symbol, '15m', 100)
                liq_clusters = await self.data_feed.fetch_liquidation_clusters(symbol)
                
                if not candles:
                    print(f"⛔ No candle data for {symbol}")
                    continue
                if not liq_clusters:
                    print(f"⛔ No liquidation data for {symbol}")
                    continue
                    
                # 2. Process and analyze data
                df = process_candles(candles)
                df = self.analytics.calculate_indicators(df)
                if df.empty:
                    print(f"⛔ Empty dataframe after indicators for {symbol}")
                    continue
                    
                # 3. Get nearest liquidation cluster
                nearest_liq = liq_clusters[0]['price']
                
                # 4. Generate signals
                score = self.analytics.calculate_exhaustion_score(df, nearest_liq)
                signals = self.analytics.detect_entry_signals(df, nearest_liq)
                
                print(f"• Exhaustion Score: {score:.2f}/10")
                print(f"• Entry Signals: {signals}")
                
                # 5. Execute trade if conditions met
                if score >= 7.0 and signals['all_ok']:
                    entry_price = nearest_liq * 0.997  # 0.3% below cluster
                    atr = df['atr'].iloc[-1]
                    
                    # Calculate position size with risk management
                    size = self.execution.calculate_position_size(
                        entry_price, atr, self.btc_volatility
                    )
                    
                    # Generate take profit levels
                    tp_levels = self.execution.generate_tp_levels(entry_price, atr)
                    
                    # Place simulated order
                    self.execution.simulate_order(
                        symbol=symbol,
                        side='sell',
                        entry_price=entry_price,
                        size=size,
                        sl_distance=entry_price * (0.0042 * atr),
                        tp_levels=tp_levels
                    )
                    print(f"📨 Placed SIMULATED order for {symbol} at {entry_price}")
                else:
                    print("⏩ Conditions not met for trade")
                    
                # Test mode diagnostics
                if self.test_mode:
                    last_candle = df.iloc[-1]
                    print(f"\n[TEST] Last candle for {symbol}:")
                    print(f"Open: {last_candle['open']}, High: {last_candle['high']}")
                    print(f"Low: {last_candle['low']}, Close: {last_candle['close']}")
                    print(f"Volume: {last_candle['volume']}, ATR: {last_candle['atr']}")
            
            except Exception as e:
                print(f"🔥 Trade execution error for {symbol}: {str(e)}")
    
    async def exit_positions(self, partial: bool = True):
        """Exit positions according to protocol"""
        action = "50%" if partial else "100%"
        print(f"🚪 Closing {action} of positions...")
        
        if not self.execution.open_positions:
            print("⏩ No open positions to close")
            return
            
        for symbol in list(self.execution.open_positions.keys()):
            percentage = 50 if partial else 100
            self.execution.simulate_close(symbol, percentage=percentage)
    
    async def manage_positions(self):
        """Monitor and manage open positions"""
        print("👀 Monitoring positions...")
        if self.execution.open_positions:
            for symbol, pos in self.execution.open_positions.items():
                print(f"• {symbol}: {pos['size']} contracts @ {pos['entry']}")
        else:
            print("⏩ No open positions")

async def main():
    """System entry point with command-line handling"""
    parser = argparse.ArgumentParser(description='Jakarta Exhaustion Trading System v3.0')
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    args = parser.parse_args()
    
    system = JakartaTradingSystem(test_mode=args.test)
    await system.run()

if __name__ == "__main__":
    asyncio.run(main())