# ═══════════════════════════════════════════════════════════════════════
#  logger.py — Timestamped logging utilities
# ═══════════════════════════════════════════════════════════════════════

import time

def log(msg: str):
    """Standard timestamped log line."""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def log_section(title: str):
    """Prints a visible section separator."""
    bar = "─" * 55
    print(f"\n{bar}", flush=True)
    print(f"  {title}", flush=True)
    print(f"{bar}", flush=True)
