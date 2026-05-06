# scripts/download.py
import json
import os
import subprocess
import time
import re
from pathlib import Path
from typing import Dict, Optional, List, Tuple
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent))
from utils import (
    safe_name, load_archive, save_archive, retry, run_command,
    get_channel_identifier
)

def load_config() -> dict:
    with open(Path(__file__).parent / 'config.json') as f:
        return json.load(f)

def get_ytdlp_format(
    quality: str,
    video_codec: str,
    container: str,
    frame_rate: str,
) -> str:
    """Build yt-dlp format string for video download."""
    height = int(quality) if quality.isdigit() else 720
    codec_priority = load_config()['codec_priority'].get(video_codec, ['h264'])
    # Build video filter: vcodec likely matches, height <= target, fps <= target (if given)
    vcodec_filter = '/'.join([f'vcodec~={c}' for c in codec_priority])
    fps_filter = f'[fps<={frame_rate}]' if frame_rate else ''
    # Prefer video+audio in one if possible
    # Use best single format if available
    # yt-dlp syntax: "bestvideo[height<=?HEIGHT][vcodec~=X]+bestaudio[ext=m4a]/best[height<=?HEIGHT]"
    # We'll compose:
    video_sel = f'bestvideo[height<={height}]{fps_filter}[{vcodec_filter}]+bestaudio[ext=m4a]/bestvideo[height<={height}]{fps_filter}+bestaudio/best[height<={height}]/best'
    # Then add container merge: --merge-output-format container
    return video_sel

def download_media(
    url: str,
    output_dir: Path,
    media_type: str,
    quality: str,
    video_codec: str = 'h264',
    container: str = 'mp4',
    frame_rate: str = '',
    embed_thumbnail: bool = False,
    download_subs: bool = True,
    dry_run: bool = False,
) -> Tuple[bool, Optional[Dict]]:
    """Download a single video/audio. Returns (success, metadata_dict)."""
    # Prepare output template
    output_template = str(output_dir / f'%(title).100s_%(channel)s_%(upload_date>%Y%m%d)s_%(id)s.%(ext)s')
    # yt-dlp options
    cmd = [
        'yt-dlp',
        '--no-playlist',
        '--no-warnings',
        '--no-check-certificate',
        '--downloader', 'aria2c',
        '--external-downloader-args', 'aria2c:-x 8 -s 8',
        '--write-thumbnail',
        '--convert-thumbnails', 'jpg',
        '--write-info-json',
        '--print', 'after_move:filepath',
        '-o', output_template,
    ]
    if media_type == 'audio':
        cmd += [
            '-x',
            '--audio-format', load_config()['default_audio_format'],
            '--audio-quality', '0' if quality == 'best' else f'{quality}',
            '--embed-thumbnail',
        ]
    else:
        cmd += [
            '-f', get_ytdlp_format(quality, video_codec, container, frame_rate),
            '--merge-output-format', container,
        ]
    if download_subs:
        cmd += [
            '--write-subs',
            '--write-auto-subs',
            '--sub-langs', 'fa,en',
            '--embed-subs',
            '--convert-subs', 'srt',
        ]
    if dry_run:
        cmd.insert(1, '--dump-json')  # will not download
        # We'll parse json and return metadata
        try:
            res = subprocess.run(cmd + [url], capture_output=True, text=True, timeout=60)
            if res.returncode == 0:
                data = json.loads(res.stdout.strip())
                return True, data
        except Exception as e:
            print(f'Dry-run metadata error: {e}')
        return False, None

    # Real download with retry
    attempt = 0
    while attempt < load_config()['max_retries']:
        attempt += 1
        try:
            # Run yt-dlp, capture output for filepath and info json
            res = subprocess.run(
                cmd + [url],
                capture_output=True, text=True, timeout=600,
                env={**os.environ, 'LC_ALL': 'C.UTF-8'}  # ensure clean
            )
            if res.returncode != 0:
                # Non-zero exit, treat as failure
                if attempt < load_config()['max_retries']:
                    time.sleep(load_config()['retry_backoff_base'] * (2 ** (attempt-1)))
                    continue
                return False, None

            # Parse the printed filepath (the last line)
            lines = res.stdout.strip().split('\n')
            downloaded_file = None
            for line in reversed(lines):
                if line.endswith(('.mp4', '.mkv', '.webm', '.mp3', '.m4a')):
                    downloaded_file = line.strip()
                    break
            if not downloaded_file:
                # Fallback: find newest file in output_dir
                files = list(output_dir.glob('*'))
                if files:
                    downloaded_file = str(max(files, key=lambda p: p.stat().st_mtime))

            # Find info json
            info_jsons = list(output_dir.glob('*.info.json'))
            info_data = {}
            if info_jsons:
                with open(info_jsons[0], 'r') as f:
                    info_data = json.load(f)

            # Rename file to include quality suffix
            if downloaded_file and os.path.exists(downloaded_file):
                filepath = Path(downloaded_file)
                # Determine quality suffix
                quality_suffix = quality
                if media_type == 'video':
                    # Extract height from info if possible
                    pass
                new_name = (
                    f'{filepath.stem}_{quality_suffix}{filepath.suffix}'
                )
                new_path = filepath.parent / new_name
                shutil.move(str(filepath), str(new_path))
                downloaded_file = str(new_path)

            # Separate subtitle files into subtitles/ folder
            subs_dir = output_dir / 'subtitles'
            subs_dir.mkdir(exist_ok=True)
            for sub in output_dir.glob('*.srt'):
                shutil.move(str(sub), str(subs_dir / sub.name))

            # Thumbnail handling
            thumb_jpg = output_dir.glob('*.jpg')
            thumb_dir = output_dir / 'thumbnails'
            thumb_dir.mkdir(exist_ok=True)
            for jpg in thumb_jpg:
                shutil.move(str(jpg), str(thumb_dir / jpg.name))

            # Optional video thumbnail embedding
            if media_type == 'video' and embed_thumbnail and downloaded_file:
                thumb_files = list(thumb_dir.glob('*.jpg'))
                if thumb_files:
                    thumb_input = str(thumb_files[0])
                    temp_out = str(Path(downloaded_file).with_suffix('.tmp.mp4'))
                    ffmpeg_cmd = [
                        'ffmpeg', '-y',
                        '-i', downloaded_file,
                        '-i', thumb_input,
                        '-map', '0:v', '-map', '0:a?', '-map', '1:v',
                        '-c', 'copy', '-disposition:v:1', 'attached_pic',
                        temp_out
                    ]
                    try:
                        subprocess.run(ffmpeg_cmd, check=True, timeout=120)
                        os.replace(temp_out, downloaded_file)
                    except Exception:
                        pass

            # Collect metadata for index
            meta = {
                'id': info_data.get('id', ''),
                'title': info_data.get('title', 'Untitled'),
                'channel': info_data.get('channel') or info_data.get('uploader') or 'Unknown',
                'upload_date': info_data.get('upload_date', ''),
                'duration': info_data.get('duration', 0),
                'view_count': info_data.get('view_count', 0),
                'thumbnail': str(thumb_dir / (info_data.get('id', '') + '.jpg')),
                'filepath': downloaded_file,
                'media_type': media_type,
                'quality': quality,
            }
            if not meta['thumbnail'] or not os.path.exists(meta['thumbnail']):
                meta['thumbnail'] = f'https://i.ytimg.com/vi/{meta["id"]}/maxresdefault.jpg'

            return True, meta

        except Exception as e:
            print(f'Download attempt {attempt} error: {e}')
            time.sleep(load_config()['retry_backoff_base'] * (2 ** (attempt-1)))
    return False, None

def download_batch(videos: List[Dict], base_dir: Path, config: dict, options: dict) -> Tuple[List, List]:
    """Download a list of videos in parallel, return (successes, failures)."""
    output_dir = base_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    results_success = []
    results_failure = []

    with ThreadPoolExecutor(max_workers=config['parallel_downloads']) as executor:
        future_to_video = {}
        for v in videos:
            future = executor.submit(
                download_media,
                v['url'],
                output_dir,
                options['type'],
                options['quality'],
                options.get('video_codec', 'h264'),
                options.get('container', 'mp4'),
                options.get('frame_rate', ''),
                options.get('embed_thumbnail', False),
                options.get('download_subs', True),
                options.get('dry_run', False),
            )
            future_to_video[future] = v

        for future in as_completed(future_to_video):
            v = future_to_video[future]
            try:
                success, meta = future.result()
                if success:
                    results_success.append({'video': v, 'meta': meta})
                else:
                    results_failure.append({'video': v, 'error': 'Download failed'})
            except Exception as e:
                results_failure.append({'video': v, 'error': str(e)})

    return results_success, results_failure
