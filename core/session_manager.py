import pytz
from datetime import datetime, time, timedelta
import asyncio

class SessionManager:
    """Jakarta trading session manager based on UTC+7 timezone"""
    
    def __init__(self):
        # Configure Jakarta timezone (UTC+7)
        self.jakarta_tz = pytz.timezone('Asia/Jakarta')
        self.current_state = None       # Current session state
        self.next_state_change = None   # Datetime of next state transition
        self.next_state_name = None     # Name of next state
    
    def get_current_time(self) -> datetime:
        """Get current datetime in Jakarta timezone"""
        return datetime.now(self.jakarta_tz)
    
    def _time_in_range(self, start: time, end: time, current: time) -> bool:
        """
        Check if current time is within a specified range
        :param start: Start time (inclusive)
        :param end: End time (inclusive)
        :param current: Current time to check
        :return: True if current time is between start and end
        """
        return start <= current <= end
    
    def determine_session_state(self) -> str:
        """
        Determine the current trading session state based on Jakarta time
        Returns one of: PRE_SESSION, SCANNING, GOLDEN_HOUR, MANAGEMENT, EXIT_WINDOW, SHUTDOWN
        """
        now = self.get_current_time()
        current_time = now.time()
        
        # Define Jakarta session time boundaries
        PRE_SESSION_START = time(18, 45)
        PRE_SESSION_END = time(19, 0)
        
        SCANNING_START = time(19, 0)
        SCANNING_END = time(19, 15)
        
        GOLDEN_HOUR_START = time(19, 30)
        GOLDEN_HOUR_END = time(20, 30)
        
        MANAGEMENT_START = time(20, 30)
        MANAGEMENT_END = time(22, 30)
        
        EXIT_WINDOW_START = time(22, 30)
        EXIT_WINDOW_END = time(22, 45)
        
        # Determine current state based on time ranges
        if self._time_in_range(PRE_SESSION_START, PRE_SESSION_END, current_time):
            state = "PRE_SESSION"
            next_change = now.replace(hour=19, minute=0, second=0, microsecond=0)
            next_state = "SCANNING"
        
        elif self._time_in_range(SCANNING_START, SCANNING_END, current_time):
            state = "SCANNING"
            next_change = now.replace(hour=19, minute=30, second=0, microsecond=0)
            next_state = "GOLDEN_HOUR"
        
        elif self._time_in_range(GOLDEN_HOUR_START, GOLDEN_HOUR_END, current_time):
            state = "GOLDEN_HOUR"
            next_change = now.replace(hour=20, minute=30, second=0, microsecond=0)
            next_state = "MANAGEMENT"
        
        elif self._time_in_range(MANAGEMENT_START, MANAGEMENT_END, current_time):
            state = "MANAGEMENT"
            next_change = now.replace(hour=22, minute=30, second=0, microsecond=0)
            next_state = "EXIT_WINDOW"
        
        elif self._time_in_range(EXIT_WINDOW_START, EXIT_WINDOW_END, current_time):
            state = "EXIT_WINDOW"
            next_change = now.replace(hour=22, minute=45, second=0, microsecond=0)
            next_state = "SHUTDOWN"
        
        else:  # Outside active trading hours
            state = "SHUTDOWN"
            # Calculate next session start (18:45 today or tomorrow)
            next_session = now.replace(hour=18, minute=45, second=0, microsecond=0)
            
            # If before 18:45 today
            if current_time < PRE_SESSION_START:
                next_change = next_session
            else:  # After 22:45, so next session is tomorrow
                next_change = next_session + timedelta(days=1)
            
            next_state = "PRE_SESSION"
        
        # Update state if changed
        if state != self.current_state:
            print(f"🔄 Session state changed to: {state}")
            self.current_state = state
            self.next_state_change = next_change
            self.next_state_name = next_state
        
        return state
    
    def get_next_state_info(self) -> tuple:
        """
        Get information about the next state transition
        :return: Tuple (next_state_name, (hours_until, minutes_until))
        """
        if not self.next_state_change:
            return "N/A", (0, 0)
        
        now = self.get_current_time()
        time_diff = self.next_state_change - now
        
        # Handle case where next state is in the past
        if time_diff.total_seconds() < 0:
            return "N/A", (0, 0)
        
        total_minutes = int(time_diff.total_seconds() // 60)
        hours = total_minutes // 60
        minutes = total_minutes % 60
        
        return self.next_state_name, (hours, minutes)
    
    def get_time_until_next_state(self) -> tuple:
        """Get time until next state change as (hours, minutes)"""
        _, time_tuple = self.get_next_state_info()
        return time_tuple
    
    def get_next_state_name(self) -> str:
        """Get the name of the next state"""
        name, _ = self.get_next_state_info()
        return name
    
    def is_golden_hour(self) -> bool:
        """Check if current state is GOLDEN_HOUR"""
        return self.determine_session_state() == "GOLDEN_HOUR"
    
    def is_exit_window(self) -> bool:
        """Check if current state is EXIT_WINDOW"""
        return self.determine_session_state() == "EXIT_WINDOW"
    
    def should_trade(self) -> bool:
        """Check if trading is allowed in current state"""
        state = self.determine_session_state()
        return state in ["GOLDEN_HOUR", "MANAGEMENT"]
    
    async def wait_until_next_session(self) -> datetime:
        """
        Wait until the next trading session starts (18:45 Jakarta time)
        :return: The datetime when the next session starts
        """
        now = self.get_current_time()
        
        # Calculate next session start time (today at 18:45)
        next_session = now.replace(hour=18, minute=45, second=0, microsecond=0)
        
        # If it's already past 18:45, wait until tomorrow
        if now > next_session:
            next_session += timedelta(days=1)
        
        # Calculate wait time in seconds
        wait_seconds = (next_session - now).total_seconds()
        
        if wait_seconds > 0:
            # Convert to hours and minutes for display
            hours = wait_seconds // 3600
            minutes = (wait_seconds % 3600) // 60
            print(f"⏳ Waiting {int(hours)} hours {int(minutes)} minutes until next session")
            await asyncio.sleep(wait_seconds)
        
        return next_session


def test_session_manager():
    """Comprehensive test of session manager functionality"""
    print("\n=== Jakarta Session Manager Test ===")
    manager = SessionManager()
    
    # Test current state determination
    current_state = manager.determine_session_state()
    print(f"Current state: {current_state}")
    
    # Test next state info
    next_state, (hours, mins) = manager.get_next_state_info()
    print(f"Next state: {next_state} in {hours} hours {mins} minutes")
    
    # Test state checks
    print("\n🔍 State checks:")
    print(f"Is Golden Hour? {manager.is_golden_hour()}")
    print(f"Is Exit Window? {manager.is_exit_window()}")
    print(f"Should trade? {manager.should_trade()}")
    
    # Test time until next state
    print("\n⏱️ Time until next state:")
    print(f"Time tuple: {manager.get_time_until_next_state()}")
    print(f"Next state name: {manager.get_next_state_name()}")
    
    # Test waiting (simulated)
    print("\n⏳ Simulating wait until next session...")
    # Note: Actual wait would be commented out in tests
    # asyncio.run(manager.wait_until_next_session())


if __name__ == "__main__":
    test_session_manager()