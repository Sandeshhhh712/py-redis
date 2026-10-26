#!/usr/bin/env python3
"""Start the miniredis server.

Examples:
    python redis_server.py
    python redis_server.py --port 6379 --host 0.0.0.0
    python redis_server.py --no-persistence
"""

import sys

from miniredis.cli import main

if __name__ == "__main__":
    sys.exit(main())
