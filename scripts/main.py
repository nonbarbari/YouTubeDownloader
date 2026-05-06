# scripts/main.py
import os
import sys
import json
import shutil
import glob
import re
import subprocess
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    safe_name, extract_youtube_urls, load_archive, save_archive,
    get_channel_identifier, set_log_file, log
)
from download import download_media, load_config as dl_load_config
from index import generate_markdown_index, generate_metadata_json, generate_playlist_csv
from report import generate_summary_report

CONFIG_PATH = Path(__file__).parent / 'config.json'
ARCHIVE_PATH = Path.cwd() / '.archive_state.json'

def load_config() -> dict:
    return dl_load_config()

def set_env_vars():
    env_map = {
        'ACTION': 'ACTION',
        'MODE': 'MODE', 'URL': 'URL', 'TYPE': 'TYPE',
        'VIDEO_QUALITY': 'VIDEO_QUALITY', 'AUDIO_QUALITY': 'AUDIO_QUALITY',
        'VIDEO_CODEC': 'VIDEO_CODEC', 'CONTAINER': 'CONTAINER',
        'FRAME_RATE': 'FRAME_RATE', 'DOWNLOAD_ENGINE': 'DOWNLOAD_ENGINE',
        'MAX_VIDEOS': 'MAX_VIDEOS', 'SPLIT_THRESHOLD_MB': 'SPLIT_THRESHOLD_MB',
        'DRY_RUN': 'DRY_RUN', 'EMBED_THUMBNAIL': 'EMBED_THUMBNAIL',
        'DOWNLOAD_SUBS': 'DOWNLOAD_SUBS',
    }
    for env_key, input_key in env_map.items():
        if env_key not in os.environ:
            os.environ[env_key] = ''

def parse_mode() -> str:
    return os.environ.get('MODE', 'single')

def parse_url() -> str:
    return os.environ.get('URL', '')

def parse_int(key: str, default: int) -> int:
    val = os.environ.get(key, '').strip()
    return int(val) if val.isdigit() else default

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
        return downloads / 'playlist' / (safe_name(identifier, 40) if identifier else 'playlist')
    elif mode == 'channel':
        return downloads / 'channel' / (safe_name(identifier, 40) if identifier else 'channel')
    else:
        return downloads / 'other' / timestamp

def get_video_list(mode: str, url: str, max_videos: int, archive: Dict) -> List[Dict]:
    if mode == 'batch':
        urls = extract_youtube_urls(url)
        videos = []
        for u in urls:
            m = re.search(r'(?:v=|/)([\w-]{11})', u)
            vid_id = m.group(1) if m else None
            if vid_id and vid_id not in archive:
                videos.append({'id': vid_id, 'url': u})
        log(f"Batch: {len(urls)} URLs extracted, {len(videos)} new after archive filter")
        return videos
    elif mode in ('playlist', 'channel'):
        cmd = ['yt-dlp', '--flat-playlist', '--dump-json', '--no-warnings', '--no-check-certificate']
        if max_videos > 0:
            cmd.extend(['--playlist-end', str(max_videos)])
        cmd.append(url)
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            videos = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    vid = data.get('id')
                    if vid and vid not in archive:
                        videos.append({
                            'id': vid,
                            'url': f'https://youtube.com/watch?v={vid}',
                            'title': data.get('title', 'Untitled')[:100]
                        })
                except:
                    continue
            log(f"Playlist: {len(videos)} new videos (max={max_videos})")
            return videos
        except Exception as e:
            log(f"yt-dlp playlist error: {e}")
            return []
    elif mode == 'single':
        m = re.search(r'(?:v=|/)([\w-]{11})', url)
        vid = m.group(1) if m else None
        if vid and vid in archive:
            log(f"Single video {vid} already archived, skipping")
            return []
        return [{'id': vid, 'url': url}]
    elif mode == 'search':
        return []
    return []

def do_downloads(
    videos: List[Dict],
    base_dir: Path,
    config: dict,
    options: dict,
    dry_run: bool,
    archive: Dict
) -> (List[Dict], List[Dict], int):
    all_success = []
    all_failures = []
    total_size = 0
    if not videos:
        log("No videos to download.")
        return [], [], 0

    base_dir.mkdir(parents=True, exist_ok=True)
    for vid in videos:
        log(f"Processing: {vid['id']} {vid.get('title','')}")
        success, meta = download_media(
            vid['url'],
            base_dir,
            options['type'],
            options['quality'],
            config,
            engine=options.get('engine', 'native'),
            video_codec=options.get('video_codec', 'h264'),
            container=options.get('container', 'mp4'),
            frame_rate=options.get('frame_rate', ''),
            embed_thumbnail=options.get('embed_thumbnail', False),
            download_subs=options.get('download_subs', True),
            dry_run=dry_run,
        )
        if success:
            archive[vid['id']] = datetime.now().isoformat()
            all_success.append({'video': vid, 'meta': meta})
            if meta and 'filepath' in meta and os.path.exists(meta['filepath']):
                size = os.path.getsize(meta['filepath'])
                total_size += size
                log(f"  -> OK ({size/1024/1024:.1f} MB)")
            else:
                log("  -> OK but file missing")
        else:
            log(f"  -> FAILED")
            all_failures.append({
                'url': vid['url'],
                'title': vid.get('title', 'Unknown'),
                'error': 'Download failed'
            })
    return all_success, all_failures, total_size

def run_process_basic():
    config = load_config()
    set_env_vars()
    # Init logging
    log_path = Path(os.environ.get('LOG_FILE', 'workflow_run.log'))
    set_log_file(log_path)
    log("Download session started")
    mode = parse_mode()
    url = parse_url()
    max_videos = parse_int('MAX_VIDEOS', 10)
    dry_run = parse_bool('DRY_RUN', False)
    archive = load_archive(ARCHIVE_PATH)

    if mode == 'search':
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
        log(f"Search index saved: {idx_file}")
        return

    videos = get_video_list(mode, url, max_videos, archive)
    if not videos:
        log("No new videos to process (all archived or invalid).")
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

    quality = os.environ.get('VIDEO_QUALITY', '720') if os.environ.get('TYPE') == 'video' else os.environ.get('AUDIO_QUALITY', 'highest')
    options = {
        'type': os.environ.get('TYPE', 'video'),
        'quality': quality,
        'video_codec': os.environ.get('VIDEO_CODEC', 'h264'),
        'container': os.environ.get('CONTAINER', 'mp4'),
        'frame_rate': os.environ.get('FRAME_RATE', ''),
        'engine': os.environ.get('DOWNLOAD_ENGINE', config['defaults']['download_engine']),
        'embed_thumbnail': parse_bool('EMBED_THUMBNAIL', False),
        'download_subs': parse_bool('DOWNLOAD_SUBS', True),
        'dry_run': dry_run,
    }

    success, failures, total_size = do_downloads(videos, base_dir, config, options, dry_run, archive)

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

    generate_summary_report(
        idx_dir / 'summary.md',
        datetime.now().isoformat(),
        mode,
        len(videos),
        len(success),
        len(failures),
        total_size,
        failures,
        [],
        dry_run,
    )

    save_archive(ARCHIVE_PATH, archive)
    log("Download session finished")

# ─── Playlist/Channel matrix handlers (unchanged except logging) ──
def preflight():
    config = load_config()
    set_env_vars()
    log_path = Path(os.environ.get('LOG_FILE', 'workflow_run.log'))
    set_log_file(log_path)
    log("Preflight started")
    url = parse_url()
    max_videos = parse_int('MAX_VIDEOS', 10)
    archive = load_archive(ARCHIVE_PATH)
    videos = get_video_list(parse_mode(), url, max_videos, archive)
    log(f"Total videos for chunking: {len(videos)}")
    with open('/tmp/video_list.json', 'w') as f:
        json.dump(videos, f)

    chunk_size = 20
    total = len(videos)
    if total <= chunk_size:
        chunk_indices = [0]
    else:
        num_chunks = (total + chunk_size - 1) // chunk_size
        chunk_indices = list(range(num_chunks))
    log(f"Chunk indices: {chunk_indices}")

    with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
        f.write(f'chunk_count={len(chunk_indices)}\n')
        f.write(f'chunk_indices={json.dumps(chunk_indices)}\n')

def download_chunk(chunk_index_str: str):
    chunk_index = int(chunk_index_str)
    config = load_config()
    set_env_vars()
    log_path = Path('/tmp/chunk_output') / f'chunk_{chunk_index}.log'
    set_log_file(log_path)
    log(f"Chunk {chunk_index} started")
    dry_run = parse_bool('DRY_RUN', False)
    archive = load_archive(ARCHIVE_PATH)

    with open('/tmp/video_list.json', 'r') as f:
        all_videos = json.load(f)

    chunk_size = 20
    start = chunk_index * chunk_size
    end = start + chunk_size
    chunk = all_videos[start:end]
    log(f"Videos in chunk: {len(chunk)}")

    base_dir = Path('/tmp/chunk_output')
    base_dir.mkdir(parents=True, exist_ok=True)

    quality = os.environ.get('VIDEO_QUALITY', '720') if os.environ.get('TYPE') == 'video' else os.environ.get('AUDIO_QUALITY', 'highest')
    options = {
        'type': os.environ.get('TYPE', 'video'),
        'quality': quality,
        'video_codec': os.environ.get('VIDEO_CODEC', 'h264'),
        'container': os.environ.get('CONTAINER', 'mp4'),
        'frame_rate': os.environ.get('FRAME_RATE', ''),
        'engine': os.environ.get('DOWNLOAD_ENGINE', config['defaults']['download_engine']),
        'embed_thumbnail': parse_bool('EMBED_THUMBNAIL', False),
        'download_subs': parse_bool('DOWNLOAD_SUBS', True),
        'dry_run': dry_run,
    }

    success, failures, _ = do_downloads(chunk, base_dir, config, options, dry_run, archive)

    chunk_meta = {
        'chunk': chunk_index,
        'success': [s['meta'] for s in success],
        'failures': failures,
        'videos': chunk,
    }
    with open(base_dir / 'chunk_meta.json', 'w') as f:
        json.dump(chunk_meta, f, indent=2)

    archive_delta = {s['video']['id']: datetime.now().isoformat() for s in success}
    with open(base_dir / 'archive_delta.json', 'w') as f:
        json.dump(archive_delta, f)
    log(f"Chunk {chunk_index} finished: {len(success)} ok, {len(failures)} failed")

def assemble():
    config = load_config()
    set_env_vars()
    log_path = Path(os.environ.get('LOG_FILE', 'workflow_run.log'))
    set_log_file(log_path)
    log("Assemble started")
    archive = load_archive(ARCHIVE_PATH)

    chunk_dirs = glob.glob('/tmp/all_chunks/chunk-*')
    log(f"Found {len(chunk_dirs)} chunk dirs")
    all_meta = []
    all_failures = []
    total_size = 0
    mode = parse_mode()
    url = parse_url()
    identifier = None
    if mode == 'playlist':
        identifier = safe_name(url.split('list=')[-1] if 'list=' in url else 'playlist', 40)
    elif mode == 'channel':
        identifier = get_channel_identifier(url) or 'channel'
    base_target = get_run_base_dir(mode, identifier)
    base_target.mkdir(parents=True, exist_ok=True)

    for cdir in chunk_dirs:
        meta_file = Path(cdir) / 'chunk_meta.json'
        if meta_file.exists():
            with open(meta_file) as f:
                chunk_data = json.load(f)
                all_meta.extend(chunk_data.get('success', []))
                all_failures.extend(chunk_data.get('failures', []))

        # Move media files
        for item in Path(cdir).iterdir():
            if item.is_file() and item.suffix not in ('.json',):
                shutil.move(str(item), str(base_target / item.name))
            elif item.is_dir():
                dest_sub = base_target / item.name
                dest_sub.mkdir(exist_ok=True)
                for subfile in item.iterdir():
                    shutil.move(str(subfile), str(dest_sub / subfile.name))

        # Update archive
        delta_file = Path(cdir) / 'archive_delta.json'
        if delta_file.exists():
            with open(delta_file) as f:
                archive.update(json.load(f))

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
    log("Assemble finished")

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--preflight':
        preflight()
    elif len(sys.argv) > 2 and sys.argv[1] == '--download-chunk':
        download_chunk(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == '--assemble':
        assemble()
    else:
        run_process_basic()
