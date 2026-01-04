import sys


def log(msg: str):
    print(msg, flush=True)
    sys.stdout.flush()
