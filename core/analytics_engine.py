import pandas as pd
import pandas_ta as ta
import numpy as np

class AnalyticsEngine:
    """Core technical analysis engine for Jakarta Exhaustion System"""
    def __init__(self, thresholds):  # Add thresholds parameter
        self.thresholds = thresholds

    @staticmethod
    def calculate_indicators(df: pd.DataFrame):
        """
        Calculates all required technical indicators for the trading strategy
        - Handles both datetime and range indexes properly
        - Returns DataFrame with added indicator columns
        """
        # Validate input data
        if df.empty or len(df) < 20:
            return df  # Insufficient data for reliable indicators
            
        df = df.copy()  # Avoid modifying original DataFrame
        
        # Temporary datetime index creation for indicator compatibility
        original_index = df.index
        needs_reset = not isinstance(df.index, pd.DatetimeIndex)
        
        if needs_reset:
            df = df.reset_index(drop=True)
            df.index = pd.date_range(start='2023-01-01', periods=len(df), freq='15min')
        
        try:
            # 1. Exponential Moving Average (5-period)
            df['ema5'] = ta.ema(df['close'], length=5)
            
            # 2. Relative Strength Index (14-period)
            df['rsi'] = ta.rsi(df['close'], length=14)
            
            # 3. Volume Weighted Average Price (VWAP)
            typical_price = (df['high'] + df['low'] + df['close']) / 3
            cumulative_typical = (typical_price * df['volume']).cumsum()
            cumulative_volume = df['volume'].cumsum()
            df['vwap'] = cumulative_typical / cumulative_volume
            
            # 4. Average True Range (14-period)
            df['atr'] = ta.atr(
                high=df['high'], 
                low=df['low'], 
                close=df['close'], 
                length=14
            )
            
            # 5. Volume Simple Moving Average (20-period)
            df['volume_sma20'] = ta.sma(df['volume'], length=20)
            
            # 6. Wick Ratio: (High - Close) / (High - Low)
            with np.errstate(divide='ignore', invalid='ignore'):
                wick_ratio = (df['high'] - df['close']) / (df['high'] - df['low'])
            # Handle division errors and NaN values
            wick_ratio = wick_ratio.replace([np.inf, -np.inf], np.nan).fillna(0)
            df['wick_ratio'] = wick_ratio
            
        except Exception as e:
            print(f"⚠️ Indicator calculation error: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on critical error
            
        # Restore original index if modified
        if needs_reset:
            df.index = original_index
            
        return df.dropna()  # Remove rows with missing values

    @staticmethod
    def detect_rsi_divergence(df: pd.DataFrame, lookback: int = 5) -> bool:
        """
        Detects bearish RSI divergence in the most recent candles
        Conditions:
        1. Current RSI > 70 (overbought)
        2. Current price high > previous high in lookback window
        3. Current RSI < RSI at previous high point
        """
        if len(df) < lookback + 1:
            return False  # Insufficient data
            
        # Extract recent candles including current
        recent_data = df.iloc[-(lookback+1):]
        
        # Identify previous high (excluding current candle)
        previous_highs = recent_data.iloc[:-1]['high']
        previous_max_high = previous_highs.max()
        previous_high_index = previous_highs.idxmax()
        
        # Current candle values
        current_candle = recent_data.iloc[-1]
        current_high = current_candle['high']
        current_rsi = current_candle['rsi']
        
        # RSI value at previous high point
        previous_high_rsi = df.at[previous_high_index, 'rsi']
        
        # Diagnostic output
        print(f"\n🔍 RSI Divergence Check:")
        print(f"- Current High: {current_high:.4f}, Previous Max High: {previous_max_high:.4f}")
        print(f"- Current RSI: {current_rsi:.2f}, Previous High RSI: {previous_high_rsi:.2f}")
        
        # Validate divergence conditions
        return (
            current_rsi > 70 and 
            current_high > previous_max_high and 
            current_rsi < previous_high_rsi
        )

    @staticmethod
    def calculate_exhaustion_score(self, df: pd.DataFrame, liquidation_price: float) -> float:
        """
        Computes exhaustion score (0-10) based on v3.0 scoring matrix:
        - RSI Divergence: 30% weight
        - Wick Ratio: 25% weight
        - Volume Spike: 20% weight
        - EMA Distance: 15% weight
        - Liquidation Proximity: 10% weight
        """
        if len(df) < 20:
            return 0.0  # Insufficient data
            
        current = df.iloc[-1]  # Most recent candle
        
        # 1. RSI Component (30% weight)
        rsi_component = min((current['rsi'] - self.thresholds['rsi_threshold']) * 0.3, 3.0) if current['rsi'] > self.thresholds['rsi_threshold'] else 0
        
        # 2. Wick Ratio (25% weight)
        wick_component = min(current['wick_ratio'] * 25, 2.5)  # 0.1 wick ratio = 2.5 points
        
        # 3. Volume Spike (20% weight)
        volume_component = 0
        volume_ratio = current['volume'] / current['volume_sma20']
        if volume_ratio > self.thresholds['volume_spike']:
            volume_component = min((volume_ratio - self.thresholds['volume_spike']) * 5, 2.0)
        
        # 4. EMA Distance (15% weight)
        ema_component = 0
        ema_distance = ((current['ema5'] - current['close']) / current['close']) * 100
        if ema_distance > self.thresholds['ema_distance']:
            ema_component = min((ema_distance - self.thresholds['ema_distance']) * 3.125, 1.5)
        
        # 5. Liquidation Proximity (10% weight)
        price_distance = abs(liquidation_price - current['close']) / current['close'] * 100
        liquidation_component = min((self.thresholds['liquidation_proximity'] - min(price_distance, self.thresholds['liquidation_proximity'])) * 66.67, 1.0)
        # Combine components with weighted sum
        total_score = sum([
            rsi_component,
            wick_component,
            volume_component,
            ema_component,
            liquidation_component
        ])
        
        return min(total_score, 10.0)  # Cap maximum score at 10

    @staticmethod
    def detect_entry_signals(self, df: pd.DataFrame, liquidation_price: float) -> dict:
        """
        Evaluates all entry conditions for the most recent candle
        Returns dictionary with individual condition statuses and overall 'all_ok' flag
        """
        # Initialize default response for insufficient data
        default_result = {
            'ema_ok': False, 'rsi_ok': False, 'vol_ok': False,
            'wick_ok': False, 'vwap_ok': False, 'liq_ok': False,
            'all_ok': False
        }
        
        if len(df) < 20:
            return default_result
            
        current = df.iloc[-1]  # Most recent candle
        
        # EMA5 distance > threshold
        ema_distance = ((current['ema5'] - current['close']) / current['close']) * 100
        ema_ok = ema_distance > self.thresholds['ema_distance']
        
        # RSI > threshold and showing divergence
        rsi_ok = current['rsi'] > self.thresholds['rsi_threshold'] and self.detect_rsi_divergence(df)
        
        # Volume > threshold x 20-period average
        vol_ok = current['volume'] > current['volume_sma20'] * self.thresholds['volume_spike']
        
        # Wick ratio > threshold
        wick_ok = current['wick_ratio'] > self.thresholds['wick_ratio']
        
        # Price > threshold% above VWAP
        vwap_ok = current['close'] > current['vwap'] * (1 + self.thresholds['vwap_deviation'])
        
        # Liquidation cluster within threshold%
        price_distance = abs(liquidation_price - current['close']) / current['close'] * 100
        liq_ok = price_distance < self.thresholds['liquidation_proximity']
        
        return {
            'ema_ok': ema_ok,
            'rsi_ok': rsi_ok,
            'vol_ok': vol_ok,
            'wick_ok': wick_ok,
            'vwap_ok': vwap_ok,
            'liq_ok': liq_ok,
            'all_ok': all([ema_ok, rsi_ok, vol_ok, wick_ok, vwap_ok, liq_ok])
        }


def test_analytics_engine():
    """Comprehensive test function for analytics engine components"""
    print("\n=== Jakarta Exhaustion Analytics Engine Test ===")
    print("Creating test data with signal-triggering conditions...")
    
    # Create test dataset (30 periods)
    n_periods = 30
    dates = pd.date_range(start='2023-01-01', periods=n_periods, freq='15min')
    
    # Base price trend (upward)
    base_price = np.linspace(100, 150, n_periods)
    
    # Construct OHLCV data
    test_data = {
        'open': base_price - 1,
        'high': base_price + 2,
        'low': base_price - 2,
        'close': base_price,
        'volume': [10000] * (n_periods - 1) + [125000]  # Final candle volume spike
    }
    
    df = pd.DataFrame(test_data, index=dates)
    last_index = df.index[-1]
    
    # --- Configure specific signal conditions ---
    
    # 1. EMA5 Distance >4.8%
    df['ema5'] = df['close'] * 1.10  # 10% above close
    
    # 2. RSI Divergence Setup
    df['rsi'] = 65  # Baseline RSI
    
    # Previous high setup (candle -2)
    prev_high_index = df.index[-2]
    df.loc[prev_high_index, 'high'] = 149  # Previous high
    df.loc[prev_high_index, 'rsi'] = 85    # Higher RSI at previous high
    
    # Current candle setup (candle -1)
    df.loc[last_index, 'high'] = 151  # New high
    df.loc[last_index, 'rsi'] = 80    # Lower RSI at new high
    
    # 3. Volume Spike (5x average)
    df['volume_sma20'] = 25000  # 20-period SMA
    df.loc[last_index, 'volume'] = 125000  # 5x SMA
    
    # 4. Wick Ratio >0.6
    df.loc[last_index, 'low'] = 140
    df.loc[last_index, 'close'] = 142
    # Calculate actual wick ratio
    current_high = df.loc[last_index, 'high']
    current_low = df.loc[last_index, 'low']
    current_close = df.loc[last_index, 'close']
    wick_ratio = (current_high - current_close) / (current_high - current_low)
    df['wick_ratio'] = 0.5  # Baseline
    df.loc[last_index, 'wick_ratio'] = wick_ratio
    
    # 5. VWAP Calculation
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    cumulative_typical = (typical_price * df['volume']).cumsum()
    cumulative_volume = df['volume'].cumsum()
    df['vwap'] = cumulative_typical / cumulative_volume
    
    # 6. Liquidation Price Setup
    liquidation_price = df.loc[last_index, 'close'] * 1.005  # 0.5% above
    
    # --- Execute tests ---
    engine = AnalyticsEngine()
    
    # Manual verification
    current = df.iloc[-1]
    print("\n📊 Manual Condition Verification:")
    print(f"• EMA Distance: {((current['ema5']-current['close'])/current['close']*100):.2f}% (Req: >4.8%)")
    print(f"• RSI: {current['rsi']} (Req: >70)")
    print(f"• Volume Ratio: {(current['volume']/current['volume_sma20']):.2f}x (Req: >3.7)")
    print(f"• Wick Ratio: {current['wick_ratio']:.3f} (Req: >0.6)")
    print(f"• VWAP Distance: {(current['close']/current['vwap']-1)*100:.2f}% (Req: >5%)")
    print(f"• Liquidation Distance: {abs(liquidation_price-current['close'])/current['close']*100:.2f}% (Req: <1.5%)")
    
    # RSI Divergence Test
    divergence_detected = engine.detect_rsi_divergence(df)
    print(f"\n🔍 RSI Divergence Result: {divergence_detected} (Expected: True)")
    
    # Exhaustion Score Test
    score = engine.calculate_exhaustion_score(df, liquidation_price)
    print(f"⭐ Exhaustion Score: {score:.2f}/10 (Expected: >8.5)")
    
    # Entry Signals Test
    signals = engine.detect_entry_signals(df, liquidation_price)
    print("\n🚦 Entry Signal Status:")
    for signal, status in signals.items():
        print(f"- {signal}: {status}")

if __name__ == "__main__":
    test_analytics_engine()