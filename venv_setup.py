import os
import sys
import site
import subprocess
import venv

VENV_DIR = "executor"

REQUIRED_PACKAGES = [
    "dotenv","openalgo","pandas",
    "websockets","pyarrow",
    "psycopg2-binary",
    "pytz; python_version<'3.9'",
]

def create_and_activate_venv():
    """
    Create virtual environment if it does not exist
    and inject site-packages into current interpreter.
    Safe for asyncio (runs before event loop starts).
    """

    if not os.path.exists(VENV_DIR):
        print(f"📦 Creating virtual environment '{VENV_DIR}'...")
        venv.create(VENV_DIR, with_pip=True)

        pip_path = os.path.join(
            VENV_DIR,
            "Scripts" if os.name == "nt" else "bin",
            "pip"
        )

        print("📥 Installing required packages...")
        subprocess.check_call([
            pip_path, "install", "--upgrade", "pip"
        ])

        subprocess.check_call([
            pip_path, "install", *REQUIRED_PACKAGES
        ])

        print(f"✅ Virtual environment '{VENV_DIR}' created successfully")

    # Inject venv site-packages into current Python process
    site_packages = os.path.join(
        VENV_DIR,
        "Lib" if os.name == "nt" else "lib",
        f"python{sys.version_info.major}.{sys.version_info.minor}",
        "site-packages"
    )

    if site_packages not in sys.path:
        site.addsitedir(site_packages)
        print(f"🔗 Venv site-packages added to sys.path")

    return True


# Auto-run when imported
create_and_activate_venv()