# scripts/download.py
import json
import os
import subprocess
import time
import shutil
import requests
from pathlib import Path
from typing import Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import safe_name, retry, load_archive, save_archive

def load_config() -> dict:
    """Load config.json, fallback to defaults if missing or invalid."""
    config_path = Path(__file__).parent / 'config.json'
    defaults = {
        "parallel_downloads": 2,
        "max_retries": 5,
        "retry_backoff_base": 10,
        "api": {
            "base_url": "https://hub.ytconvert.org/api/download",
            "status_poll_interval_sec": 2,
            "status_max_attempts": 60,
            "headers": {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Referer": "https://media.ytmp3.gg/",
                "Content-Type": "application/json",
                "Origin": "https://media.ytmp3.gg"
            }
        },
        "codec_priority": {
            "h264": ["avc1.*", "h264"],
            "h265": ["hev1.*", "hvc1.*", "hevc"],
            "av1": ["av01.*", "av1"],
            "vp9": ["vp09.*", "vp9"]
        },
        "defaults": {
            "video_quality": "720",
            "audio_quality": "highest",
            "video_codec": "h264",
            "container": "mp4",
            "frame_rate": "",
            "download_engine": "native"
        }
    }
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                user_config = json.load(f)
                # shallow merge user overrides into defaults (nested)
                for key, value in user_config.items():
                    if isinstance(value, dict) and key in defaults:
                        defaults[key].update(value)
                    else:
                        defaults[key] = value
        except (json.JSONDecodeError, IOError):
            pass  # fallback to defaults entirely
    return defaults


def api_download(url: str, media_type: str, quality: str, config: dict) -> Tuple[bool, Optional[str]]:
    """Download via external API. Returns (success, temporary_file_path)."""
    output_format = 'mp3' if media_type == 'audio' else 'mp4'
    payload = {
        'url': url,
        'os': 'linux',
        'output': {
            'type': 'audio' if media_type == 'audio' else 'video',
            'format': output_format
        }
    }
    if media_type == 'audio':
        payload['audio'] = {'bitrate': f'{quality}k'} if quality != 'highest' else {'bitrate': '320k'}
    else:
        payload['output']['quality'] = f'{quality}p' if quality != 'highest' else '2160p'

    headers = config['api']['headers']
    base_url = config['api']['base_url']

    try:
        resp = requests.post(base_url, json=payload, headers=headers, timeout=15)
        if resp.status_code != 200:
            return False, None
        data = resp.json()
        status_url = data.get('statusUrl')
        if not status_url:
            return False, None

        # Poll for completion
        for _ in range(config['api']['status_max_attempts']):
            time.sleep(config['api']['status_poll_interval_sec'])
            sr = requests.get(status_url, headers=headers, timeout=10)
            if sr.status_code == 200:
                sd = sr.json()
                status = sd.get('status', '')
                if status == 'completed':
                    dl_url = sd.get('downloadUrl')
                    if dl_url:
                        # Download to a temp file
                        tmp_file = Path(f'/tmp/api_dl_{int(time.time())}.{output_format}')
                        with requests.get(dl_url, stream=True, timeout=600) as r:
                            r.raise_for_status()
                            with open(tmp_file, 'wb') as f:
                                for chunk in r.iter_content(chunk_size=65536):
                                    f.write(chunk)
                        return True, str(tmp_file)
                elif status in ('failed', 'error'):
                    break
    except Exception:
        pass
    return False, None


def get_ytdlp_format(quality: str, video_codec: str, container: str, frame_rate: str, config: dict) -> str:
    """Build yt-dlp format string for video download."""
    if quality == 'highest':
        height = 2160
    else:
        height = int(quality) if quality.isdigit() else 720

    codec_priority = config['codec_priority'].get(video_codec, ['h264'])
    vcodec_filter = '/'.join([f'vcodec~={c}' for c in codec_priority])
    fps_filter = f'[fps<={frame_rate}]' if frame_rate else ''
    return f'bestvideo[height<={height}]{fps_filter}[{vcodec_filter}]+bestaudio[ext=m4a]/bestvideo[height<={height}]{fps_filter}+bestaudio/best[height<={height}]/best'


def native_download(
    url: str,
    output_dir: Path,
    media_type: str,
    quality: str,
    config: dict,
    video_codec: str = 'h264',
    container: str = 'mp4',
    frame_rate: str = '',
    embed_thumbnail: bool = False,
    download_subs: bool = True,
    dry_run: bool = False,
) -> Tuple[bool, Optional[Dict]]:
    """Download with yt-dlp. Returns (success, metadata_dict)."""
    output_template = str(output_dir / f'%(title).100s_%(channel)s_%(upload_date>%Y%m%d)s_%(id)s.%(ext)s')
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
        quality_arg = '0' if quality == 'highest' else f'{quality}'
        cmd += [
            '-x',
            '--audio-format', 'mp3',
            '--audio-quality', quality_arg,
            '--embed-thumbnail',
        ]
    else:
        cmd += [
            '-f', get_ytdlp_format(quality, video_codec, container, frame_rate, config),
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
        cmd.insert(1, '--dump-json')
        try:
            res = subprocess.run(cmd + [url], capture_output=True, text=True, timeout=60)
            if res.returncode == 0:
                data = json.loads(res.stdout.strip())
                return True, data
        except Exception as e:
            print(f'Dry-run metadata error: {e}')
        return False, None

    # Real download with retries
    max_retries = config['max_retries']
    backoff = config['retry_backoff_base']
    for attempt in range(1, max_retries+1):
        try:
            res = subprocess.run(cmd + [url], capture_output=True, text=True, timeout=600)
            if res.returncode != 0:
                if attempt < max_retries:
                    time.sleep(backoff * (2 ** (attempt-1)))
                    continue
                return False, None

            # Parse printed filepath
            lines = res.stdout.strip().split('\n')
            downloaded_file = None
            for line in reversed(lines):
                if line.endswith(('.mp4', '.mkv', '.webm', '.mp3', '.m4a')):
                    downloaded_file = line.strip()
                    break

            # Metadata from info json
            info_jsons = list(output_dir.glob('*.info.json'))
            info = {}
            if info_jsons:
                with open(info_jsons[0], 'r') as f:
                    info = json.load(f)

            # Rename to include quality
            if downloaded_file and Path(downloaded_file).exists():
                p = Path(downloaded_file)
                suffix = quality if quality != 'highest' else 'best'
                new_name = f'{p.stem}_{suffix}{p.suffix}'
                new_path = p.parent / new_name
                shutil.move(str(p), str(new_path))
                downloaded_file = str(new_path)

            # Move subtitles to subtitles/
            subs_dir = output_dir / 'subtitles'
            subs_dir.mkdir(exist_ok=True)
            for sub in output_dir.glob('*.srt'):
                shutil.move(str(sub), str(subs_dir / sub.name))

            # Organize thumbnails
            thumb_dir = output_dir / 'thumbnails'
            thumb_dir.mkdir(exist_ok=True)
            for jpg in output_dir.glob('*.jpg'):
                shutil.move(str(jpg), str(thumb_dir / jpg.name))

            # Optional embed thumbnail into video
            if media_type == 'video' and embed_thumbnail and downloaded_file:
                thumb_files = list(thumb_dir.glob('*.jpg'))
                if thumb_files:
                    thumb_in = str(thumb_files[0])
                    tmp_out = str(Path(downloaded_file).with_suffix('.tmp.mp4'))
                    ffmpeg_cmd = [
                        'ffmpeg', '-y',
                        '-i', downloaded_file, '-i', thumb_in,
                        '-map', '0:v', '-map', '0:a?', '-map', '1:v',
                        '-c', 'copy', '-disposition:v:1', 'attached_pic',
                        tmp_out
                    ]
                    try:
                        subprocess.run(ffmpeg_cmd, check=True, timeout=120)
                        os.replace(tmp_out, downloaded_file)
                    except Exception:
                        pass

            meta = {
                'id': info.get('id', ''),
                'title': info.get('title', 'Untitled'),
                'channel': info.get('channel') or info.get('uploader') or 'Unknown',
                'upload_date': info.get('upload_date', ''),
                'duration': info.get('duration', 0),
                'view_count': info.get('view_count', 0),
                'thumbnail': str(thumb_dir / f'{info.get("id", "x")}.jpg'),
                'filepath': downloaded_file,
                'media_type': media_type,
                'quality': quality,
            }
            return True, meta

        except Exception as e:
            print(f'Download attempt {attempt} error: {e}')
            time.sleep(backoff * (2 ** (attempt-1)))
    return False, None


def download_media(
    url: str,
    output_dir: Path,
    media_type: str,
    quality: str,
    config: dict,
    engine: str = 'native',
    video_codec: str = 'h264',
    container: str = 'mp4',
    frame_rate: str = '',
    embed_thumbnail: bool = False,
    download_subs: bool = True,
    dry_run: bool = False,
) -> Tuple[bool, Optional[Dict]]:
    """Unified download dispatcher."""
    if engine == 'api':
        # For API, we still need metadata from yt-dlp; fetch it via dump-json
        meta = None
        try:
            info = subprocess.run(
                ['yt-dlp', '--dump-json', '--no-warnings', url],
                capture_output=True, text=True, timeout=30
            )
            if info.returncode == 0:
                meta = json.loads(info.stdout)
        except Exception:
            meta = {}
        success, tmp_file = api_download(url, media_type, quality, config)
        if success and tmp_file:
            # Determine final filename using safe metadata
            title = safe_name(meta.get('title', 'video')[:60]) if meta else 'video'
            channel = safe_name(meta.get('channel', 'channel')[:30]) if meta else 'channel'
            date = meta.get('upload_date', '')[:8] if meta else ''
            vid_id = meta.get('id', '')
            quality_suffix = quality if quality != 'highest' else 'best'
            ext = 'mp3' if media_type == 'audio' else 'mp4'
            final_name = f'{title}_{channel}_{date}_{vid_id}_{quality_suffix}.{ext}'
            dest = output_dir / final_name
            shutil.move(tmp_file, str(dest))
            # Build metadata dict
            out_meta = {
                'id': vid_id,
                'title': meta.get('title', 'Untitled') if meta else 'Untitled',
                'channel': meta.get('channel') or meta.get('uploader') or 'Unknown',
                'duration': meta.get('duration', 0) if meta else 0,
                'view_count': meta.get('view_count', 0) if meta else 0,
                'filepath': str(dest),
                'thumbnail': f'https://i.ytimg.com/vi/{vid_id}/maxresdefault.jpg',
                'media_type': media_type,
                'quality': quality,
            }
            return True, out_meta
        return False, None
    else:
        return native_download(
            url, output_dir, media_type, quality, config,
            video_codec, container, frame_rate, embed_thumbnail, download_subs, dry_run
        )
