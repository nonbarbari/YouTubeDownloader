# scripts/report.py
# (unchanged)
from datetime import datetime
from pathlib import Path
from typing import List, Dict

def generate_summary_report(
    output_path: Path,
    timestamp: str,
    mode: str,
    total: int,
    successful: int,
    failed: int,
    total_size_bytes: int,
    failures: List[Dict],
    skipped_duplicates: List[str],
    dry_run: bool = False,
):
    size_mb = total_size_bytes / (1024 * 1024) if total_size_bytes > 0 else 0
    md = f"# 📊 Download Summary\n\n"
    md += f"- **Timestamp:** {timestamp}\n"
    md += f"- **Mode:** {mode}\n"
    md += f"- **Dry Run:** {'Yes' if dry_run else 'No'}\n"
    md += f"- **Total URLs processed:** {total}\n"
    md += f"- **Successful:** {successful}\n"
    md += f"- **Failed:** {failed}\n"
    if total_size_bytes > 0:
        md += f"- **Total size:** {size_mb:.2f} MB\n"
    if skipped_duplicates:
        md += "\n## ⏭️ Skipped Duplicates\n\n"
        for vid in skipped_duplicates:
            md += f"- {vid}\n"
    if failures:
        md += "\n## ❌ Failed Downloads\n\n"
        for fail in failures:
            md += f"- **{fail.get('title', 'Unknown')}** ({fail.get('url', '')}): {fail.get('error', '')}\n"
    output_path.write_text(md, encoding='utf-8')
