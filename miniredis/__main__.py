"""Allow ``python -m miniredis`` to start the server."""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
