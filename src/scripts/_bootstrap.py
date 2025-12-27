from __future__ import annotations

import sys
from pathlib import Path

#Garantir que o diretório src/ esteja no sys.path. 
# def setup_sys_path() -> None:
#     src_dir = Path(__file__).resolve().parents[1]
#     src_str = str(src_dir)
#     if src_str not in sys.path:
#         sys.path.insert(0, src_str)

def setup_sys_path() -> None:
    root = Path(__file__).resolve().parents[1]   # /root/data_platform/src
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
