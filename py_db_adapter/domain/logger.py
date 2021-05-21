import logging

__all__ = ("root",)

root = logging.getLogger("pda")
root.addHandler(logging.NullHandler())
