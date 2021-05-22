import logging

__all__ = ("root_logger",)

root_logger = logging.getLogger("pda")
root_logger.addHandler(logging.NullHandler())
