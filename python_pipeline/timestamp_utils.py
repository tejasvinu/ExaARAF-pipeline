from datetime import datetime, timezone
import pytz
import time
import os

# Set timezone to IST
os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

class TimestampManager:
    """Centralized timestamp management for the metrics pipeline"""
    
    def __init__(self):
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.utc_tz = timezone.utc
        
    def get_current_timestamp(self):
        """Get current timestamp in UTC"""
        return datetime.now(self.utc_tz)
        
    def get_current_timestamp_ist(self):
        """Get current timestamp in IST"""
        return datetime.now(self.ist_tz)
        
    def parse_timestamp(self, timestamp_str, default_time=None):
        """Parse timestamp string to datetime object
        
        Args:
            timestamp_str: String or datetime object to parse
            default_time: Default time to use if parsing fails (defaults to current UTC time)
        
        Returns:
            datetime object in UTC
        """
        if default_time is None:
            default_time = self.get_current_timestamp()
            
        if not timestamp_str:
            return default_time
            
        try:
            if isinstance(timestamp_str, str):
                # Handle various timestamp formats
                if 'Z' in timestamp_str:
                    ts = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                elif '+' in timestamp_str or '-' in timestamp_str:
                    ts = datetime.fromisoformat(timestamp_str)
                else:
                    ts = datetime.fromisoformat(timestamp_str)
                    ts = ts.replace(tzinfo=self.utc_tz)
                return ts.astimezone(self.utc_tz)
            elif isinstance(timestamp_str, datetime):
                # Handle datetime objects
                if timestamp_str.tzinfo is None:
                    return timestamp_str.replace(tzinfo=self.utc_tz)
                return timestamp_str.astimezone(self.utc_tz)
            else:
                return default_time
        except (ValueError, AttributeError):
            return default_time
            
    def format_timestamp(self, dt, timezone=None, format_str=None):
        """Format timestamp for output
        
        Args:
            dt: datetime object to format
            timezone: Target timezone (defaults to IST)
            format_str: Optional format string (defaults to ISO format)
            
        Returns:
            Formatted timestamp string
        """
        if not dt:
            return None
            
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=self.utc_tz)
            
        target_tz = timezone or self.ist_tz
        dt = dt.astimezone(target_tz)
        
        if format_str:
            return dt.strftime(format_str)
        return dt.isoformat()
        
    def to_unix_timestamp(self, dt):
        """Convert datetime to Unix timestamp (seconds since epoch)"""
        if not dt:
            return None
            
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=self.utc_tz)
        return dt.timestamp()
        
    def from_unix_timestamp(self, timestamp):
        """Convert Unix timestamp to datetime object in UTC"""
        if not timestamp:
            return None
            
        return datetime.fromtimestamp(float(timestamp), self.utc_tz)
        
    def convert_timezone(self, dt, target_tz=None):
        """Convert datetime object to target timezone
        
        Args:
            dt: datetime object to convert
            target_tz: Target timezone (defaults to IST)
            
        Returns:
            datetime object in target timezone
        """
        if not dt:
            return None
            
        target_tz = target_tz or self.ist_tz
        
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=self.utc_tz)
            
        return dt.astimezone(target_tz)
        
    def is_naive(self, dt):
        """Check if datetime object has timezone information"""
        return dt.tzinfo is None
        
    def ensure_utc(self, dt):
        """Ensure datetime object is in UTC"""
        if not dt:
            return None
            
        if self.is_naive(dt):
            return dt.replace(tzinfo=self.utc_tz)
        return dt.astimezone(self.utc_tz)
        
    def ensure_ist(self, dt):
        """Ensure datetime object is in IST"""
        if not dt:
            return None
            
        return self.convert_timezone(dt, self.ist_tz)

# Create a singleton instance
timestamp_manager = TimestampManager()