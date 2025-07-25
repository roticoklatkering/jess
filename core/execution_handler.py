import time
from typing import Dict, List

class ExecutionHandler:
    """Trade execution manager with position sizing and simulated order handling"""
    
    def __init__(self, risk_per_trade: float = 100.0):
        """
        Initialize execution handler
        :param risk_per_trade: USD amount to risk per trade (default: $100)
        """
        self.risk_per_trade = risk_per_trade
        self.open_positions = {}    # Currently open positions {symbol: order}
        self.trade_history = []     # Historical trade records
    
    def calculate_position_size(self, entry_price: float, atr: float, 
                               btc_volatility: float) -> float:
        """
        Calculate position size based on volatility parameters
        Implements Jakarta v3.0 position sizing rules
        
        :param entry_price: Asset entry price
        :param atr: Average True Range (volatility measure)
        :param btc_volatility: BTC 1-hour ATR as percentage
        :return: Position size in contract units
        """
        # Determine SL multiplier based on BTC volatility
        if btc_volatility < 0.01:       # Low volatility
            multiplier = 0.50
        elif btc_volatility < 0.02:     # Medium volatility
            multiplier = 0.42
        else:                           # High volatility
            multiplier = 0.30
        
        # Calculate stop loss distance
        sl_distance = entry_price * (multiplier * atr)
        
        # Calculate base position size
        size = self.risk_per_trade / sl_distance
        
        # Apply high volatility reduction
        if btc_volatility > 0.02:
            size *= 0.6  # Reduce position size by 40%
            
        return size
    
    def generate_tp_levels(self, entry: float, atr: float) -> List[Dict]:
        """
        Generate take profit levels according to v3.0 protocol
        
        :param entry: Entry price
        :param atr: Average True Range
        :return: List of take profit levels with size percentages
        """
        return [
            {'price': entry - (1 * atr), 'size': 50},  # TP1: 1× ATR (50% position)
            {'price': entry - (2 * atr), 'size': 30},  # TP2: 2× ATR (30% position)
            {'price': entry - (3 * atr), 'size': 20}   # TP3: 3× ATR (20% position)
        ]
    
    def simulate_order(self, symbol: str, side: str, entry_price: float, 
                      size: float, sl_distance: float, tp_levels: List[Dict]) -> Dict:
        """
        Simulate order placement (no real exchange interaction)
        
        :param symbol: Trading symbol (e.g., 'BTCUSDT')
        :param side: 'buy' or 'sell'
        :param entry_price: Order entry price
        :param size: Position size in contract units
        :param sl_distance: Stop loss distance in price units
        :param tp_levels: Take profit levels from generate_tp_levels
        :return: Simulated order dictionary
        """
        # Calculate stop loss price based on trade direction
        if side == 'sell':
            sl_price = entry_price - sl_distance  # SL above entry for shorts
        else:
            sl_price = entry_price + sl_distance  # SL below entry for longs
        
        # Generate unique order ID
        order_id = f"SIM_{int(time.time()*1000)}"
        
        # Create order structure
        order = {
            'id': order_id,
            'symbol': symbol,
            'side': side,
            'price': entry_price,
            'size': size,
            'sl': sl_price,
            'tps': tp_levels,
            'status': 'open',
            'timestamp': time.time()
        }
        
        # Track position and history
        self.open_positions[symbol] = order
        self.trade_history.append(order)
        
        print(f"📊 SIMULATED ORDER: {side.upper()} {symbol} @ {entry_price}, "
              f"Size: {size:.6f}, SL: {sl_price:.2f}")
        return order
    
    def simulate_close(self, symbol: str, percentage: float = 100.0) -> Dict:
        """
        Simulate closing all or part of a position
        
        :param symbol: Trading symbol to close
        :param percentage: Percentage of position to close (default: 100%)
        :return: Close order dictionary or None if no position
        """
        # Validate position exists
        if symbol not in self.open_positions:
            print(f"⏩ No open position for {symbol}")
            return None
            
        position = self.open_positions[symbol]
        close_size = position['size'] * (percentage / 100.0)
        
        # Generate close order ID
        close_id = f"CLOSE_{int(time.time()*1000)}"
        
        # Create close order (opposite direction)
        close_side = 'buy' if position['side'] == 'sell' else 'sell'
        close_order = {
            'id': close_id,
            'symbol': symbol,
            'side': close_side,
            'size': close_size,
            'percentage': percentage,
            'status': 'closed',
            'timestamp': time.time()
        }
        
        # Update position
        if percentage >= 100:
            # Full closure - remove position
            del self.open_positions[symbol]
            position['status'] = 'closed'
            print(f"🚪 Closed 100% of {symbol} position")
        else:
            # Partial closure - reduce position size
            position['size'] -= close_size
            print(f"🚪 Closed {percentage}% of {symbol} position")
        
        self.trade_history.append(close_order)
        return close_order


async def test_execution_handler():
    """Comprehensive test of execution handler functionality"""
    print("\n=== Jakarta Exhaustion Execution Handler Test ===")
    handler = ExecutionHandler(risk_per_trade=100)
    
    # Test position sizing
    print("\n🔍 Testing position sizing...")
    test_cases = [
        (60000, 1500, 0.008),   # Low volatility
        (60000, 1500, 0.015),   # Medium volatility
        (60000, 1500, 0.025)    # High volatility
    ]
    
    for entry, atr, vol in test_cases:
        size = handler.calculate_position_size(entry, atr, vol)
        print(f"- Entry: ${entry}, ATR: {atr}, Vol: {vol*100:.1f}% → Size: {size:.6f}")
    
    # Test TP levels generation
    print("\n🔍 Testing take profit levels...")
    entry_price = 60000
    atr = 1500
    tp_levels = handler.generate_tp_levels(entry_price, atr)
    print(f"Entry: ${entry_price}, ATR: {atr}")
    for i, tp in enumerate(tp_levels):
        print(f"  TP{i+1}: ${tp['price']:.1f} ({tp['size']}%)")
    
    # Test order simulation
    print("\n🔍 Testing order simulation...")
    order = handler.simulate_order(
        symbol='BTCUSDT',
        side='sell',
        entry_price=entry_price,
        size=0.005,
        sl_distance=entry_price * 0.01,  # 1% SL distance
        tp_levels=tp_levels
    )
    print(f"Open positions: {list(handler.open_positions.keys())}")
    
    # Test partial close
    print("\n🔍 Testing partial close...")
    handler.simulate_close('BTCUSDT', percentage=50)
    print(f"Remaining position size: {handler.open_positions['BTCUSDT']['size']}")
    
    # Test full close
    print("\n🔍 Testing full close...")
    handler.simulate_close('BTCUSDT', percentage=100)
    print(f"Open positions after close: {handler.open_positions}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_execution_handler())