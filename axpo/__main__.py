"""
Entry point for the execution.
"""
import uvicorn
import os

import axpo.server

if __name__ == "__main__":
    port = os.environ.get("PORT", 8080)
    host = os.environ.get("HOST", "0.0.0.0")
    uvicorn.run("server:app", host=host, port=port, log_level="info")
    # You can visit: http://localhost:8080/docs by default.
