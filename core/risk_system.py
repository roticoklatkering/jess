import time

class RiskSystem:
    """Risk management system for Jakarta Exhaustion Trading System"""
    
    def __init__(self, max_daily_drawdown: float, btc_atr_threshold: float):
        """
        Initialize risk management parameters
        :param max_daily_drawdown: Maximum allowed daily drawdown percentage (default: 1.5%)
        """
        self.max_daily_drawdown = max_daily_drawdown  # Daily loss limit
        self.btc_atr_threshold = btc_atr_threshold    # Store threshold
        self.daily_pnl = 0.0                          # Daily profit/loss tracker
        self.starting_balance = None                  # Balance at session start
        self.circuit_breakers = {                     # Risk control flags
            'high_volatility': False,    # BTC volatility > 2%
            'api_failure': False,         # Excessive API errors
            'exceeded_drawdown': False    # Daily loss > max_daily_drawdown
        }
    
    def reset_daily(self):
        """Reset daily metrics at the start of a trading session"""
        self.daily_pnl = 0.0
        self.starting_balance = self.get_current_balance()  # Capture starting balance
        # Reset all circuit breakers to inactive
        for key in self.circuit_breakers:
            self.circuit_breakers[key] = False
        print("🔁 Daily risk metrics reset")
    
    def get_current_balance(self) -> float:
        """Get current account balance (simulated)"""
        # In production, replace with exchange API call
        return 10000.0  # Static example balance
    
    def update_pnl(self, amount: float):
        """
        Update daily profit/loss and check drawdown limits
        :param amount: Profit/loss amount (positive for profit, negative for loss)
        """
        self.daily_pnl += amount
        
        # Calculate current account balance
        current_balance = self.get_current_balance()
        
        # Calculate drawdown percentage
        drawdown = (self.starting_balance - current_balance) / self.starting_balance
        
        # Check if drawdown exceeds maximum allowed
        if drawdown >= self.max_daily_drawdown:
            self.circuit_breakers['exceeded_drawdown'] = True
            print(f"🚨 Drawdown limit exceeded: {drawdown*100:.2f}% ≥ {self.max_daily_drawdown*100}%")
    
    def check_btc_volatility(self, btc_atr_1h: float):
        """
        Check BTC volatility against threshold and set circuit breaker
        :param btc_atr_1h: BTC 1-hour ATR as percentage (0.02 = 2%)
        """
        # Jakarta v3.0 volatility threshold: 2%
        if btc_atr_1h > self.btc_atr_threshold:
            self.circuit_breakers['high_volatility'] = True
            print(f"⚠️ High volatility circuit: BTC ATR {btc_atr_1h*100:.2f}% > {self.btc_atr_threshold*100}%")
        else:
            self.circuit_breakers['high_volatility'] = False
    
    def check_api_failures(self, error_count: int):
        """
        Check API error count and set circuit breaker
        :param error_count: Number of API errors encountered
        """
        # Jakarta v3.0 threshold: 5 errors
        if error_count > 5:
            self.circuit_breakers['api_failure'] = True
            print(f"⚠️ API failure circuit: {error_count} errors > 5")
    
    def trading_allowed(self) -> bool:
        """Check if trading is permitted based on circuit breakers"""
        # Trading allowed only if ALL circuit breakers are inactive
        return not any(self.circuit_breakers.values())


def test_risk_system():
    """Comprehensive test of risk system functionality"""
    print("\n=== Jakarta Exhaustion Risk System Test ===")
    risk = RiskSystem()
    
    # Test daily reset
    print("\n🔁 Testing daily reset...")
    risk.reset_daily()
    print(f"Starting balance: ${risk.starting_balance:.2f}")
    
    # Test P&L updates
    print("\n💰 Testing P&L updates...")
    risk.update_pnl(-300)  # Loss
    risk.update_pnl(150)   # Profit
    print(f"Daily P&L: ${risk.daily_pnl:.2f}")
    
    # Test drawdown calculation
    print("\n📉 Testing drawdown...")
    # Simulate larger loss to trigger drawdown circuit
    risk.update_pnl(-200)  # Total P&L now -$350
    # Drawdown = (10000 - 9650) / 10000 = 0.035 (3.5%)
    print(f"Drawdown circuit active: {risk.circuit_breakers['exceeded_drawdown']}")
    
    # Test volatility circuit
    print("\n⚡ Testing volatility circuit...")
    risk.check_btc_volatility(0.025)  # 2.5% volatility
    print(f"High volatility circuit active: {risk.circuit_breakers['high_volatility']}")
    
    # Test API failure circuit
    print("\n📡 Testing API failure circuit...")
    risk.check_api_failures(6)  # Exceeds threshold
    print(f"API failure circuit active: {risk.circuit_breakers['api_failure']}")
    
    # Test trading permission
    print("\n🟢 Trading allowed check...")
    print(f"Trading permitted: {risk.trading_allowed()} (should be False)")


if __name__ == "__main__":
    test_risk_system()