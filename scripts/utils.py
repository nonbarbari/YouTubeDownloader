# scripts/utils.py
import re
import json
import os
import hashlib
import time
import subprocess
from pathlib import Path
from typing import List, Optional, Dict, Any

def safe_name(text: str, maxlen: int = 40) -> str:
    safe = re.sub(r'[^\w]', '_', text)[:maxlen]
    safe = re.sub(r'_+', '_', safe).strip('_')
    if not safe:
        safe = hashlib.md5(text.encode()).hexdigest()[:8]
    return safe

def extract_youtube_urls(text: str) -> List[str]:
    pattern = r'(https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/v/|youtube\.com/embed/)[\w\-]{11})'
    return list(set(re.findall(pattern, text)))

def load_archive(archive_path: Path) -> Dict[str, Any]:
    if archive_path.exists():
        try:
            with open(archive_path, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_archive(archive_path: Path, data: Dict[str, Any]) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with open(archive_path, 'w') as f:
        json.dump(data, f, indent=2)

def run_command(cmd: List[str], cwd: Optional[Path] = None, timeout: int = 300) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, cwd=cwd, timeout=timeout)

def retry(func, *args, max_retries=5, backoff=10, **kwargs):
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if attempt == max_retries:
                break
            delay = backoff * (2 ** (attempt - 1))
            time.sleep(delay)
    raise last_exc

def get_channel_identifier(url: str) -> Optional[str]:
    # Extract @handle, channel/ID, c/name, user/name
    m = re.search(r'@([\w.-]+)', url)
    if m:
        return m.group(1)
    m = re.search(r'channel/([\w-]+)', url)
    if m:
        return m.group(1)
    m = re.search(r'c/([\w-]+)', url)
    if m:
        return m.group(1)
    m = re.search(r'user/([\w-]+)', url)
    if m:
        return m.group(1)
    return None
