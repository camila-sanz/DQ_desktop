import logging
import sys

# Custom log levels
LOG_NONE = 0
LOG_ERROR = 1
LOG_WARNING = 2
LOG_MILESTONE = 3
LOG_RESULT = 4
LOG_CALC = 5
LOG_TECH = 6
LOG_VERBOSE = 7

CUSTOM_LEVELS = {
    LOG_ERROR: 40,
    LOG_WARNING: 30, 
    LOG_MILESTONE: 25,
    LOG_RESULT: 15,
    LOG_CALC: 12,
    LOG_TECH: 9,
    LOG_VERBOSE: 5
}

# Shared logger instance
logger = logging.getLogger("shared_logger")

def setup_logger(level=LOG_NONE, to_file=None):
    """
    Sets up the global shared logger with a given verbosity level.
    If already configured, does nothing.
    """
    if logger.hasHandlers():
        return  # Avoid duplicate handlers

    log_level = CUSTOM_LEVELS.get(level, 100)
    logger.setLevel(log_level)

    handler = logging.FileHandler(to_file) if to_file else logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Also register custom log levels so they display nicely
    for name, val in CUSTOM_LEVELS.items():
        logging.addLevelName(val, f"LEVEL{val}")

# Generic log function
def log(level, message):
    if level in CUSTOM_LEVELS:
        logger.log(CUSTOM_LEVELS[level], message)

# Convenience functions
def log_error(msg): log(LOG_ERROR, msg)
def log_warning(msg): log(LOG_WARNING, msg)
def log_milestone(msg): log(LOG_MILESTONE, msg)
def log_result(msg):    log(LOG_RESULT, msg)
def log_calc(msg):      log(LOG_CALC, msg)
def log_tech(msg):      log(LOG_TECH, msg)
def log_verbose(msg):   log(LOG_VERBOSE, msg)
