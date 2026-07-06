import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from settings import get_settings

_, _, _, cfg = get_settings()

try:
    import uvicorn
except ImportError:
    print("Установите зависимости: pip install -r requirements.txt")
    sys.exit(1)

host = cfg.web.host
port = cfg.web.port
print(f"laPG Web UI → http://{host}:{port}")
uvicorn.run("web.app:app", host=host, port=port)
