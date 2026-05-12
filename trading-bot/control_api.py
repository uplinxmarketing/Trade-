""" 
FastAPI control server — binds to $PORT on the main thread so Railway's
health-check can reach it immediately.  All trading-bot logic (DB init,
history download, WebSocket feed, strategy loop) starts in the FastAPI
lifespan as async background tasks — nothing blocks the HTTP server.
"""

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

# Unique ID generated once per process start.  Railway restarts the process on
# every deploy, so this changes on every deployment — the browser can compare
# the stored value against the polled value to detect new deploys reliably.
_DEPLOY_ID = str(uuid.uuid4())

import uvicorn
from fastapi import FastAPI, Response, Body, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import config
import database
from connection import get_mode, get_live_error
