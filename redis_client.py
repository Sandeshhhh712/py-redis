#!/usr/bin/env python3
"""Interactive client for miniredis (and any RESP2/RESP3 server).

Examples:
    python redis_client.py
    python redis_client.py --host 127.0.0.1 --port 6767
    python redis_client.py SET greeting "hello world"
"""

import sys

from miniredis.client import main

if __name__ == "__main__":
    sys.exit(main())
