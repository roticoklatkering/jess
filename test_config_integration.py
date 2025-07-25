# test_config_integration.py
from main import JakartaTradingSystem
import asyncio

async def test_config_integration():
    print("=== Testing Configuration Integration ===")
    try:
        system = JakartaTradingSystem(test_mode=True)
        print("✅ System initialized successfully")
        
        # Verify configuration loading
        assert system.thresholds['ema_distance'] == 4.8
        assert system.api_config['bitget']['apiKey'] == ""
        print("✅ Configurations loaded correctly")
        
        # Verify components received configurations
        assert system.analytics.thresholds == system.thresholds
        assert system.risk.btc_atr_threshold == system.thresholds['btc_atr_1h']
        print("✅ Components configured properly")
        
        # Test threshold usage
        print("\nTesting analytics engine with thresholds:")
        print(f"EMA Threshold: {system.analytics.thresholds['ema_distance']}%")
        print(f"RSI Threshold: {system.analytics.thresholds['rsi_threshold']}")
        
        print("\nAll tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(test_config_integration())