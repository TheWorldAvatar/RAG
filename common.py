from time import strftime
import logging
logger = logging.getLogger(__name__)

TWA_BASE_IRI = "https://www.theworldavatar.com/kg/"

def log_msg(msg: str, level = logging.INFO) -> None:
    """
    Utility function that prints a message to the console and
    appends the same message to a log file for record keeping.
    """
    timestamp = strftime('%Y-%m-%dT%H:%M:%S')
    logger.log(level, f"{timestamp}: {msg}")
    print(f"{timestamp}: WARNING: {msg}" if level == logging.WARN
        else f"{timestamp}: {msg}")
