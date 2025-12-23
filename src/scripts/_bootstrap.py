from __future__ import annotations

import sys
from pathlib import Path


def setup_sys_path() -> None:
    """
    Garante que o diretório 'src/' esteja no sys.path,
    permitindo imports como: from common.settings import settings
    mesmo rodando via `python src/scripts/arquivo.py`.
    """
    # .../data_platform/src/scripts/_bootstrap.py -> .../data_platform/src
    src_dir = Path(__file__).resolve().parents[1]
    src_str = str(src_dir)

    if src_str not in sys.path:
        sys.path.insert(0, src_str)
