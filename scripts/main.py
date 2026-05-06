# scripts/main.py
import os
import sys
import json
import shutil
import tempfile
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import subprocess

# Add scripts dir to path
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    safe_name, extract_youtube_urls, load_archive, save_archive,
    get_channel_identifier, run_command
)
from download import download_media, download_batch
from index import generate_markdown_index, generate_metadata_json, generate_playlist_csv
from report import generate_summary_report

CONFIG_PATH = Path(__file__).parent / 'config.json'
ARCHIVE_PATH = Path.cwd() / '.archive_state.json'

def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return json.load(f)

def set_env_vars():
    """Inject env vars from GitHub Actions inputs into os.environ for easy use."""
    for var in ['MODE', 'URL', 'TYPE', 'QUALITY', 'VIDEO_CODEC', 'CONTAINER', 'FRAME_RATE',
                'MAX_VIDEOS', 'SPLIT_THRESHOLD_MB', 'DRY_RUN', 'EMBED_THUMBNAIL', 'DOWNLOAD_SUBS']:
        if var not in os.environ:
            os.environ[var] = ''

def parse_mode() -> str:
    return os.environ.get('MODE', 'single')

def parse_url() -> str:
    return os.environ.get('URL', '')

def parse_int(key: str, default: int) -> int:
    val = os.environ.get(key, '').strip()
    if val.isdigit():
        return int(val)
    return default

def parse_bool(key: str, default: bool) -> bool:
    val = os.environ.get(key, '').strip().lower()
    if val in ('true', '1', 'yes'):
        return True
    elif val in ('false', '0', 'no'):
        return False
    return default

def get_run_base_dir(mode: str, identifier: Optional[str] = None) -> Path:
    downloads = Path('downloads')
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    if mode == 'single':
        return downloads / 'single' / f'{identifier}_videos' if identifier else downloads / 'single'
    elif mode == 'batch':
        return downloads / 'batch' / timestamp
    elif mode == 'playlist':
        return downloads / 'playlist' / safe_name(identifier, 40) if identifier else downloads / 'playlist'
    elif mode == 'channel':
        return downloads / 'channel' / safe_name(identifier, 40) if identifier else downloads / 'channel'
    else:
        return downloads / 'other' / timestamp

def get_video_list(mode: str, url: str, max_videos: int, archive: Dict) -> List[Dict]:
    """Return list of video dicts with 'id', 'url', maybe metadata."""
    if mode == 'batch':
        # Extract URLs, deduplicate, filter by archive
        urls = extract_youtube_urls(url)
        urls = list(dict.fromkeys(urls))  # preserve order, remove duplicates
        videos = []
        for u in urls:
            vid_id = None
            # Extract video ID from URL
            m = re.search(r'(?:v=|/)([\w-]{11})', u)
            if m:
                vid_id = m.group(1)
            if vid_id and vid_id in archive:
                continue
            videos.append({'id': vid_id, 'url': u})
        return videos
    elif mode in ('playlist', 'channel'):
        # Use yt-dlp flat playlist extraction
        cmd = ['yt-dlp', '--flat-playlist', '--dump-json', '--no-warnings', '--no-check-certificate']
        if max_videos > 0:
            cmd.extend(['--playlist-end', str(max_videos)])
        cmd.append(url)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        videos = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            try:
                data = json.loads(line)
                vid = data.get('id')
                if not vid or vid in archive:
                    continue
                videos.append({
                    'id': vid,
                    'url': f'https://youtube.com/watch?v={vid}',
                    'title': data.get('title', 'Untitled')[:100]
                })
            except:
                continue
        return videos
    elif mode == 'single':
        # Single video
        vid = None
        m = re.search(r'(?:v=|/)([\w-]{11})', url)
        if m:
            vid = m.group(1)
        if vid and vid in archive:
            return []
        return [{'id': vid, 'url': url}]
    elif mode == 'search':
        # Search mode doesn't download, only index
        return []  # handled separately
    return []

def do_downloads(
    videos: List[Dict],
    base_dir: Path,
    config: dict,
    options: dict,
    dry_run: bool,
    archive: Dict
) -> (List[Dict], List[Dict], int):
    """Perform downloads, return (success_entries, failures, total_size). Update archive."""
    all_success = []
    all_failures = []
    total_size = 0
    # Process in batches with ThreadPoolExecutor
    if not videos:
        return [], [], 0

    output_dir = base_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    success_entries = []
    for vid in videos:
        vid_id = vid.get('id')
        success, meta = download_media(
            vid['url'],
            output_dir,
            options['type'],
            options['quality'],
            options.get('video_codec', 'h264'),
            options.get('container', 'mp4'),
            options.get('frame_rate', ''),
            options.get('embed_thumbnail', False),
            options.get('download_subs', True),
            dry_run,
        )
        if success:
            # Update archive immediately
            archive[vid_id] = datetime.now().isoformat()
            success_entries.append({'video': vid, 'meta': meta})
            if meta and 'filepath' in meta and os.path.exists(meta['filepath']):
                total_size += os.path.getsize(meta['filepath'])
        else:
            all_failures.append({'url': vid['url'], 'title': vid.get('title', 'Unknown'), 'error': 'Download failed'})
    return success_entries, all_failures, total_size

def run_process_basic():
    """Handle single, search, batch modes directly."""
    config = load_config()
    set_env_vars()
    mode = parse_mode()
    url = parse_url()
    max_videos = parse_int('MAX_VIDEOS', 10)
    dry_run = parse_bool('DRY_RUN', False)
    archive = load_archive(ARCHIVE_PATH)

    if mode == 'search':
        # Search: generate markdown only, no downloads
        output_dir = Path('searches')
        output_dir.mkdir(parents=True, exist_ok=True)
        cmd = ['yt-dlp', f'ytsearch{max_videos}:{url}', '--dump-json', '--no-warnings']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        entries = []
        for line in result.stdout.strip().split('\n'):
            if line:
                data = json.loads(line)
                entries.append({
                    'id': data['id'],
                    'title': data['title'],
                    'channel': data.get('channel', 'Unknown'),
                    'duration': data.get('duration', 0),
                    'view_count': data.get('view_count', 0),
                    'thumbnail': f"https://i.ytimg.com/vi/{data['id']}/maxresdefault.jpg",
                    'url': f"https://youtube.com/watch?v={data['id']}",
                })
        date_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        idx_file = output_dir / f'search_{safe_name(url, 30)}_{date_str}.md'
        generate_markdown_index(
            [{'index': i+1, **e} for i, e in enumerate(entries)],
            idx_file,
            f'🔍 Search: {url}',
            'search'
        )
        # No push here, push step will handle
        return

    # For other modes: download
    videos = get_video_list(mode, url, max_videos, archive)
    if not videos:
        print('No videos to download (all already archived or invalid).')
        return

    identifier = None
    if mode == 'single':
        identifier = get_channel_identifier(url) or 'video'
    elif mode == 'batch':
        identifier = 'batch_' + datetime.now().strftime('%Y%m%d_%H%M')
    elif mode == 'playlist':
        identifier = safe_name(url.split('list=')[-1] if 'list=' in url else 'playlist', 40)
    elif mode == 'channel':
        identifier = get_channel_identifier(url) or 'channel'
    base_dir = get_run_base_dir(mode, identifier)

    options = {
        'type': os.environ.get('TYPE', 'video'),
        'quality': os.environ.get('QUALITY', '720'),
        'video_codec': os.environ.get('VIDEO_CODEC', 'h264'),
        'container': os.environ.get('CONTAINER', 'mp4'),
        'frame_rate': os.environ.get('FRAME_RATE', ''),
        'embed_thumbnail': parse_bool('EMBED_THUMBNAIL', False),
        'download_subs': parse_bool('DOWNLOAD_SUBS', True),
        'dry_run': dry_run,
    }

    success, failures, total_size = do_downloads(videos, base_dir, config, options, dry_run, archive)

    # Generate indices
    all_entries = []
    for s in success:
        meta = s['meta']
        vid = s['video']
        idx = len(all_entries) + 1
        all_entries.append({
            'index': idx,
            'id': meta.get('id', ''),
            'title': meta.get('title', ''),
            'channel': meta.get('channel', ''),
            'duration': meta.get('duration', 0),
            'view_count': meta.get('view_count', 0),
            'thumbnail': meta.get('thumbnail', ''),
            'url': vid['url'],
            'filepath': meta.get('filepath', ''),
            'duration_str': f'{meta["duration"]//60}:{meta["duration"]%60:02d}' if meta.get('duration') else '?',
            'views_str': f'{meta["view_count"]:,}' if meta.get('view_count') else '?',
        })

    idx_dir = base_dir
    idx_dir.mkdir(parents=True, exist_ok=True)
    generate_markdown_index(all_entries, idx_dir / 'index.md', f'{mode.upper()} Download', mode)
    generate_metadata_json(all_entries, idx_dir / 'metadata.json')
    generate_playlist_csv(all_entries, idx_dir / 'playlist.csv')

    # Summary report
    generate_summary_report(
        idx_dir / 'summary.md',
        datetime.now().isoformat(),
        mode,
        len(videos),
        len(success),
        len(failures),
        total_size,
        failures,
        [],  # skipped duplicates already filtered
        dry_run,
    )

    # Save archive
    save_archive(ARCHIVE_PATH, archive)

    # Cleanup temp files
    import atexit, shutil
    def cleanup():
        try:
            shutil.rmtree('/tmp/ytdlp_*', ignore_errors=True)
        except:
            pass
    atexit.register(cleanup)

# ─── Playlist / Channel matrix handling ─────────────────────
def preflight():
    """Extract video IDs for playlist/channel, output chunk info."""
    config = load_config()
    set_env_vars()
    url = parse_url()
    max_videos = parse_int('MAX_VIDEOS', 10)
    archive = load_archive(ARCHIVE_PATH)
    videos = get_video_list(parse_mode(), url, max_videos, archive)

    # Save video list to artifact
    with open('/tmp/video_list.json', 'w') as f:
        json.dump(videos, f)

    # Determine chunking
    chunk_size = 20
    total = len(videos)
    if total <= chunk_size:
        chunk_indices = [0]
    else:
        num_chunks = (total + chunk_size - 1) // chunk_size
        chunk_indices = list(range(num_chunks))

    # Write outputs for GitHub Actions
    with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
        f.write(f'chunk_count={len(chunk_indices)}\n')
        f.write(f'chunk_indices={json.dumps(chunk_indices)}\n')

def download_chunk(chunk_index_str: str):
    """Download a single chunk given its index (from matrix)."""
    chunk_index = int(chunk_index_str)
    config = load_config()
    set_env_vars()
    dry_run = parse_bool('DRY_RUN', False)
    archive = load_archive(ARCHIVE_PATH)

    with open('/tmp/video_list.json', 'r') as f:
        all_videos = json.load(f)

    chunk_size = 20
    start = chunk_index * chunk_size
    end = start + chunk_size
    chunk = all_videos[start:end]

    base_dir = Path('/tmp/chunk_output')
    base_dir.mkdir(parents=True, exist_ok=True)

    options = {
        'type': os.environ.get('TYPE', 'video'),
        'quality': os.environ.get('QUALITY', '720'),
        'video_codec': os.environ.get('VIDEO_CODEC', 'h264'),
        'container': os.environ.get('CONTAINER', 'mp4'),
        'frame_rate': os.environ.get('FRAME_RATE', ''),
        'embed_thumbnail': parse_bool('EMBED_THUMBNAIL', False),
        'download_subs': parse_bool('DOWNLOAD_SUBS', True),
        'dry_run': dry_run,
    }

    success, failures, _ = do_downloads(chunk, base_dir, config, options, dry_run, archive)

    # Save chunk metadata
    chunk_meta = {
        'chunk': chunk_index,
        'success': [s['meta'] for s in success],
        'failures': failures,
        'videos': chunk,
    }
    with open(base_dir / 'chunk_meta.json', 'w') as f:
        json.dump(chunk_meta, f, indent=2)

    # Update archive after chunk (will be merged later)
    # Actually archive updates should be atomic, so we write a local archive delta.
    archive_delta = {}
    for s in success:
        vid = s['video']['id']
        archive_delta[vid] = datetime.now().isoformat()
    with open(base_dir / 'archive_delta.json', 'w') as f:
        json.dump(archive_delta, f)

def assemble():
    """Merge all chunk outputs and push."""
    import glob
    config = load_config()
    set_env_vars()
    archive = load_archive(ARCHIVE_PATH)

    # Collect all chunk dirs
    chunk_dirs = glob.glob('/tmp/all_chunks/chunk-*')
    all_meta = []
    all_failures = []
    total_size = 0
    base_final = None

    for cdir in chunk_dirs:
        meta_file = Path(cdir) / 'chunk_meta.json'
        if meta_file.exists():
            with open(meta_file) as f:
                chunk_data = json.load(f)
                all_meta.extend(chunk_data.get('success', []))
                all_failures.extend(chunk_data.get('failures', []))
        # Move actual media files to final location
        for item in Path(cdir).iterdir():
            if item.is_file() and item.suffix not in ('.json',):
                pass  # Files already in chunk dirs, we need to merge into final dir. For simplicity, we'll just copy everything to downloads/playlist or channel based on mode.
    # Determine target base dir
    mode = os.environ['MODE']
    url = os.environ['URL']
    identifier = None
    if mode == 'playlist':
        identifier = safe_name(url.split('list=')[-1] if 'list=' in url else 'playlist', 40)
    elif mode == 'channel':
        identifier = get_channel_identifier(url) or 'channel'
    base_target = get_run_base_dir(mode, identifier)
    base_target.mkdir(parents=True, exist_ok=True)

    # Move all files from chunk dirs to target
    for cdir in chunk_dirs:
        for file in Path(cdir).iterdir():
            if file.is_file() and file.suffix not in ('.json',):
                shutil.move(str(file), str(base_target / file.name))
            elif file.is_dir():
                dest_sub = base_target / file.name
                dest_sub.mkdir(exist_ok=True)
                for subfile in file.iterdir():
                    shutil.move(str(subfile), str(dest_sub / subfile.name))

    # Update global archive with all deltas
    for cdir in chunk_dirs:
        delta_file = Path(cdir) / 'archive_delta.json'
        if delta_file.exists():
            with open(delta_file) as f:
                delta = json.load(f)
                archive.update(delta)

    # Build final index entries
    all_entries = []
    for idx, meta in enumerate(all_meta, 1):
        all_entries.append({
            'index': idx,
            'id': meta.get('id', ''),
            'title': meta.get('title', ''),
            'channel': meta.get('channel', ''),
            'duration': meta.get('duration', 0),
            'view_count': meta.get('view_count', 0),
            'thumbnail': meta.get('thumbnail', ''),
            'url': f"https://youtube.com/watch?v={meta.get('id', '')}",
            'filepath': meta.get('filepath', ''),
            'duration_str': f'{meta["duration"]//60}:{meta["duration"]%60:02d}' if meta.get('duration') else '?',
            'views_str': f'{meta["view_count"]:,}' if meta.get('view_count') else '?',
        })

    generate_markdown_index(all_entries, base_target / 'index.md', f'{mode.upper()} Download', mode)
    generate_metadata_json(all_entries, base_target / 'metadata.json')
    generate_playlist_csv(all_entries, base_target / 'playlist.csv')

    generate_summary_report(
        base_target / 'summary.md',
        datetime.now().isoformat(),
        mode,
        len(all_meta) + len(all_failures),
        len(all_meta),
        len(all_failures),
        total_size,
        all_failures,
        [],
        parse_bool('DRY_RUN', False),
    )

    save_archive(ARCHIVE_PATH, archive)

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--preflight':
        preflight()
    elif len(sys.argv) > 2 and sys.argv[1] == '--download-chunk':
        download_chunk(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == '--assemble':
        assemble()
    else:
        run_process_basic()
