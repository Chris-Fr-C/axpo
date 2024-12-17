"""
This module will contain the required endpoints.
Note that if the project was bigger, we would be using a router, and dispatching the routes in different files.
"""
import fastapi
import http
from typing import *
import pydantic
import os
import datetime
import axpo.aemet

app = fastapi.FastAPI()
app.include_router(axpo.aemet.router)
