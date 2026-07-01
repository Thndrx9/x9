# start_data.py

import venv_setup
import asyncio
from engine_runtime import run_engine

if __name__ == "__main__":
    asyncio.run(
        run_engine(enable_trading=False)
    )