"""\nFastAPI control server — binds to $PORT (default 8000) on the main thread.\nAll trading-bot logic (DB init, history download, WebSocket feed, strategy\nloop) starts in the FastAPI lifespan as async background tasks.\n"""

import asyncio
import json
import os
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional