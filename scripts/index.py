# scripts/index.py
# (unchanged – same as before)
import json
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict

def generate_markdown_index(entries: List[Dict], output_path: Path, title: str, mode: str):
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    md = f"# {title}\n\n**{now}** | **Mode: {mode}** | **{len(entries)} videos**\n\n"
    md += "| # | Thumbnail | Title | Duration | Views | File |\n"
    md += "|---|-----------|-------|----------|-------|------|\n"
    for e in entries:
        thumb = e.get('thumbnail', '')
        md += (
            f"| {e['index']} | "
            f"[<img src='{thumb}' width='80'>]({e['url']}) | "
            f"[{e['title']}]({e['url']}) | "
            f"{e.get('duration_str','?')} | "
            f"{e.get('views_str','?')} | "
            f"`{Path(e.get('filepath','')).name}` |\n"
        )
    md += "\n---\n\n## 📝 Details\n\n"
    for e in entries:
        md += f"### {e['index']}. {e['title']}\n\n"
        if e.get('thumbnail'):
            md += f"![Thumbnail]({e['thumbnail']})\n\n"
        md += f"- **Duration:** {e.get('duration_str','?')}\n"
        md += f"- **Views:** {e.get('views_str','?')}\n"
        md += f"- **Channel:** {e.get('channel','')}\n"
        if e.get('filepath'):
            md += f"- **File:** `{Path(e['filepath']).name}`\n"
        md += f"- **URL:** {e['url']}\n\n---\n\n"
    output_path.write_text(md, encoding='utf-8')

def generate_metadata_json(entries: List[Dict], output_path: Path):
    output_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding='utf-8')

def generate_playlist_csv(entries: List[Dict], output_path: Path):
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['index', 'title', 'channel', 'duration_sec', 'views', 'video_id', 'filepath'])
        for e in entries:
            writer.writerow([
                e.get('index', ''),
                e.get('title', ''),
                e.get('channel', ''),
                e.get('duration', ''),
                e.get('view_count', ''),
                e.get('id', ''),
                e.get('filepath', ''),
            ])
