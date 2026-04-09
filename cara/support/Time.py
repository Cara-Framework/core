"""Time related helpers."""

import pendulum


def parse_human_time(str_time):
    """
    Take a string like 1 month or 5 minutes and returns a pendulum instance.

    Arguments:
        str_time {string} -- Could be values like 1 second or 3 minutes

    Returns:
        pendulum -- Returns Pendulum instance
    """
    if str_time == "now":
        return pendulum.now("GMT")

    if str_time != "expired":
        number = int(str_time.split(" ")[0])
        length = str_time.split(" ")[1]

        if length in ("second", "seconds"):
            return pendulum.now("GMT").add(seconds=number)
        elif length in ("minute", "minutes"):
            return pendulum.now("GMT").add(minutes=number)
        elif length in ("hour", "hours"):
            return pendulum.now("GMT").add(hours=number)
        elif length in ("day", "days"):
            return pendulum.now("GMT").add(days=number)
        elif length in ("week", "weeks"):
            return pendulum.now("GMT").add(weeks=number)
        elif length in ("month", "months"):
            return pendulum.now("GMT").add(months=number)
        elif length in ("year", "years"):
            return pendulum.now("GMT").add(years=number)

        return None
    else:
        return pendulum.now("GMT").subtract(years=20)


def migration_timestamp():
    """
    Return current time formatted for creating migration filenames.

    Example: 2021_01_09_043202
    """
    return pendulum.now().format("YYYY_MM_DD_HHmmss")


def humanize_seconds(seconds: int) -> str:
    """
    Convert an integer number of seconds into a human-readable string.

    Breaks down into days, hours, minutes, seconds. Only includes non-zero components.
    
    Args:
        seconds: Number of seconds to convert
        
    Returns:
        Human-readable string representation
        
    Examples:
        >>> humanize_seconds(273132)
        '3 days 4 hours 12 minutes 12 seconds'
        >>> humanize_seconds(0)
        '0 seconds'
        >>> humanize_seconds(3661)
        '1 hour 1 minute 1 second'
    """
    if seconds == 0:
        return "0 seconds"
        
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    parts = []
    if days > 0:
        parts.append(f"{days} {'day' if days == 1 else 'days'}")
    if hours > 0:
        parts.append(f"{hours} {'hour' if hours == 1 else 'hours'}")
    if minutes > 0:
        parts.append(f"{minutes} {'minute' if minutes == 1 else 'minutes'}")
    if secs > 0:
        parts.append(f"{secs} {'second' if secs == 1 else 'seconds'}")
        
    return " ".join(parts)


def format_duration(seconds: int) -> str:
    """
    Format seconds into a compact duration string.

    Uses format like "1h 23m 45s". Only includes non-zero components.
    
    Args:
        seconds: Number of seconds to format
        
    Returns:
        Compact duration string
        
    Examples:
        >>> format_duration(273132)
        '3d 4h 12m 12s'
        >>> format_duration(0)
        '0s'
        >>> format_duration(45)
        '45s'
    """
    if seconds == 0:
        return "0s"
        
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0:
        parts.append(f"{secs}s")
        
    return " ".join(parts)
