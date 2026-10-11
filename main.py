import asyncio
import json
import os
import random
import subprocess
import traceback
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import aiohttp
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
PORT          = int(os.getenv("PORT", "8080"))
API_ID        = int(os.getenv("API_ID", "38074255"))
API_HASH      = os.getenv("API_HASH", "b24d8bf27bba4316a37c4bf2a7e9b9cf")
BOT_TOKEN     = os.getenv("BOT_TOKEN", "8712882922:AAHHVhnUGPPpVSUUx511QYg-Ttc2z-i1EzA")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "7513729138"))
STORAGE_CH    = int(os.getenv("STORAGE_CHANNEL_ID", "-1004383572894") or "0")

GROUP_IDS: list[int] = [
    int(os.getenv("GROUP_CHAT_ID_1") or os.getenv("GROUP_CHAT_ID") or "-1003710038549"),
    int(os.getenv("GROUP_CHAT_ID_2") or os.getenv("GROUP_CHAT_ID") or "-1004407035827"),
    int(os.getenv("GROUP_CHAT_ID_3") or os.getenv("GROUP_CHAT_ID") or "-1003744848287"),
]
GROUP_CHAT_ID = GROUP_IDS[0]

SESSIONS = [s.strip() for s in [
    os.getenv("STRING_SESSION_1") or os.getenv("BQJE948AxjlJfRIp_e9772JU6rn0Dbg5hSr1eqFuZvgG7RG1iHKeZjt1-8k72XDALn_5r4Jx0QXfZROL0ZxwFbi39DNlTQvU0t0dJIvs-SwJTyQmBEixyN2aGDlprrrRpRjdO4IFqPKgf30wj4tgMdWwlfg-6rmhKtQ5dojRcAhdqWXioibbYkB2ox43CfVNwXuK77b02H_LE22GDj5IV6xDVqbNsxcSR6OBXsrhJY11HdJrV5AkAxgbelkB5sWG_6GuiWaK0XD3WAIBoOop8f3vcn3opvO2QqytOTbHWnyuAkDwW3gbTLYfQEXXMZuRIEYyjmw-I0dCjppxB2vB3cjq9361xAAAAAHwf61bAA"),
    os.getenv("STRING_SESSION_2"),
    os.getenv("STRING_SESSION_3"),
] if s and s.strip()]

ACCOUNTS_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "accounts_config.json")

def _load_extra_sessions() -> list[dict]:
    """Load dynamically-added accounts from accounts_config.json"""
    try:
        if os.path.exists(ACCOUNTS_CONFIG):
            with open(ACCOUNTS_CONFIG) as f:
                return json.load(f)
    except Exception as e:
        print(f"[ACCOUNTS CFG] Load error: {e}")
    return []

def _save_extra_sessions(extra: list[dict]):
    """Save dynamically-added accounts to accounts_config.json"""
    try:
        with open(ACCOUNTS_CONFIG, "w") as f:
            json.dump(extra, f, indent=2)
    except Exception as e:
        print(f"[ACCOUNTS CFG] Save error: {e}")

# Extra accounts added at runtime (beyond env-var sessions)
_extra_sessions: list[dict] = _load_extra_sessions()  # [{session_string, group_id}]

API_BASE   = f"https://api.telegram.org/bot{BOT_TOKEN}"
CACHE_FILE  = "downloads/storage_cache.json"
HOSTED_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hosted_files")
os.makedirs("downloads", exist_ok=True)
os.makedirs(HOSTED_DIR, exist_ok=True)

print(f"[CONFIG] ADMIN={ADMIN_USER_ID} | PRIMARY_GROUP={GROUP_CHAT_ID} | "
      f"ACCOUNTS={len(SESSIONS)} | STORAGE={STORAGE_CH or 'disabled'} | PORT={PORT}")

from pyrogram import Client
from pytgcalls import PyTgCalls
from pytgcalls import types as tc


# ── Global state ──────────────────────────────────────────────────
@dataclass
class Track:
    path: str
    title: str
    duration: float
    width: int
    height: int
    size: int
    channel_msg_id: Optional[int] = None

@dataclass
class GroupState:
    queue: deque                    = field(default_factory=deque)
    now_playing: Optional[Track]    = None
    paused: bool                    = False
    status_chat: Optional[int]      = None
    status_msg: Optional[int]       = None
    account_idx: int                = 0
    last_failed_track: Optional[Track] = None

groups: dict[int, GroupState] = {}

# ── Process manager state ─────────────────────────────────────────
# { script_name: { proc, log, started, path, status, task } }
_procs: dict[str, dict] = {}

def gs(gid: int = None) -> GroupState:
    g = gid or GROUP_CHAT_ID
    if g not in groups:
        groups[g] = GroupState()
    return groups[g]

accounts:        list[dict]   = []
random_mode:     dict[int, bool] = {}
playing_msg_ids: set[int]     = set()
storage_files:   list[dict]   = []

# Web dashboard globals
_http_session:    Optional[aiohttp.ClientSession] = None
_bot_username:    str  = ""
_start_time:      float = time.time()
_url_task_status: Optional[dict] = None   # {log, done, error}


# ── Telegram helpers ──────────────────────────────────────────────
async def tg(session: aiohttp.ClientSession, method: str, **kw) -> dict:
    try:
        async with session.post(f"{API_BASE}/{method}", json=kw) as r:
            return await r.json()
    except Exception as e:
        print(f"[TG ERR] {method}: {e}")
        return {}

async def send(session, chat_id: int, text: str, buttons=None, **kw) -> dict:
    p = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown", **kw}
    if buttons is not None:
        p["reply_markup"] = {"inline_keyboard": buttons}
    return await tg(session, "sendMessage", **p)

async def edit(session, chat_id: int, msg_id: int, text: str, buttons=None):
    p = {"chat_id": chat_id, "message_id": msg_id,
         "text": text, "parse_mode": "Markdown"}
    if buttons is not None:
        p["reply_markup"] = {"inline_keyboard": buttons}
    await tg(session, "editMessageText", **p)

async def answer_cb(session, cb_id: str, text="", alert=False):
    await tg(session, "answerCallbackQuery",
             callback_query_id=cb_id, text=text, show_alert=alert)

async def tg_get_file(session, file_id: str) -> Optional[str]:
    r = await tg(session, "getFile", file_id=file_id)
    return r.get("result", {}).get("file_path")

async def tg_dl_file(session, file_path: str, save_path: str) -> bool:
    url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    async with session.get(url) as r:
        if r.status == 200:
            with open(save_path, "wb") as f:
                f.write(await r.read())
            return True
    return False


# ── Inline keyboards ──────────────────────────────────────────────
def btns_playing(gid: int) -> list:
    g = str(gid)
    return [[
        {"text": "⏸ Pause",  "callback_data": f"pause:{g}"},
        {"text": "⏭ Skip",   "callback_data": f"skip:{g}"},
        {"text": "⏹ Stop",   "callback_data": f"stop:{g}"},
    ], [
        {"text": "📋 Queue",  "callback_data": f"queue:{g}"},
        {"text": "📊 Status", "callback_data": f"status:{g}"},
    ]]

def btns_paused(gid: int) -> list:
    g = str(gid)
    return [[
        {"text": "▶️ Resume", "callback_data": f"resume:{g}"},
        {"text": "⏭ Skip",   "callback_data": f"skip:{g}"},
        {"text": "⏹ Stop",   "callback_data": f"stop:{g}"},
    ], [
        {"text": "📋 Queue",  "callback_data": f"queue:{g}"},
        {"text": "📊 Status", "callback_data": f"status:{g}"},
    ]]

def btns_main() -> list:
    gstr = str(GROUP_CHAT_ID)
    return [[
        {"text": "📊 Status",  "callback_data": f"status:{gstr}"},
        {"text": "📋 Queue",   "callback_data": f"queue:{gstr}"},
    ], [
        {"text": "🎲 Random",  "callback_data": "random_start"},
        {"text": "📁 Files",   "callback_data": "files"},
    ], [
        {"text": "⚙️ Settings","callback_data": "settings"},
    ]]

def btns_settings() -> list:
    rows = []
    for i, a in enumerate(accounts):
        gid = GROUP_IDS[i] if i < len(GROUP_IDS) else GROUP_CHAT_ID
        rnd = "🎲" if random_mode.get(i) else "👤"
        rows.append([{"text": f"{'✅' if gs(gid).account_idx == i else rnd} "
                               f"Acc#{i+1} @{a['me'].username or a['me'].id} → {gid}",
                      "callback_data": f"acct:{i}"}])
    rows.append([{"text": "◀️ Back", "callback_data": "back_main"}])
    return rows


# ── FFmpeg ────────────────────────────────────────────────────────
def probe(path: str) -> dict:
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_streams", "-show_format", path], timeout=30)
        info = json.loads(out)
        r = {"w": 0, "h": 0, "dur": 0.0, "has_video": False, "has_audio": False}
        for s in info.get("streams", []):
            if s.get("codec_type") == "video":
                r.update(has_video=True, w=s.get("width", 0), h=s.get("height", 0))
            elif s.get("codec_type") == "audio":
                r["has_audio"] = True
        r["dur"] = float(info.get("format", {}).get("duration", 0))
        return r
    except Exception as e:
        print(f"[PROBE ERR] {e}")
        return {"w": 0, "h": 0, "dur": 0.0, "has_video": False, "has_audio": True}

def pick_vq(h: int) -> tc.VideoQuality:
    if h >= 1080: return tc.VideoQuality.FHD_1080p
    if h >= 720:  return tc.VideoQuality.HD_720p
    if h >= 480:  return tc.VideoQuality.SD_480p
    return tc.VideoQuality.SD_360p

async def convert(src: str, dst: str) -> bool:
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-i", src,
        "-vf", ("scale=1920:1080:force_original_aspect_ratio=decrease,"
                "pad=1920:1080:(ow-iw)/2:(oh-ih)/2:black,setsar=1"),
        "-c:v", "libx264", "-preset", "fast", "-crf", "18",
        "-profile:v", "high", "-level", "4.1",
        "-c:a", "aac", "-b:a", "320k", "-ar", "48000", "-ac", "2",
        "-movflags", "+faststart", dst,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    _, err = await proc.communicate()
    if proc.returncode != 0:
        print(f"[FFMPEG ERR] {err.decode()[-400:]}")
        return False
    return True


# ── Formatters ────────────────────────────────────────────────────
def fdur(s: float) -> str:
    s = int(s or 0); m, s = divmod(s, 60); h, m = divmod(m, 60)
    return f"{h}h{m}m{s}s" if h else (f"{m}m{s}s" if m else f"{s}s")

def fsz(b) -> str:
    b = int(b or 0)
    if b >= 1 << 30: return f"{b/(1<<30):.1f} GB"
    if b >= 1 << 20: return f"{b/(1<<20):.1f} MB"
    return f"{b/(1<<10):.1f} KB"

def fres(w, h) -> str:
    if not h: return "?"
    lb = {2160:"4K",1440:"2K",1080:"FHD 1080p",720:"HD 720p",
          480:"SD 480p",360:"SD 360p"}.get(h, f"{h}p")
    return f"{w}×{h} ({lb})"

def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ── Storage channel ───────────────────────────────────────────────
def _save_cache():
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(storage_files, f)
    except Exception as e:
        print(f"[CACHE SAVE ERR] {e}")

def _load_cache():
    if not os.path.exists(CACHE_FILE):
        return
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        storage_files.clear()
        storage_files.extend(data)
        print(f"[STORAGE] Loaded {len(data)} files from cache")
    except Exception as e:
        print(f"[CACHE LOAD ERR] {e}")

async def fetch_storage_files_startup(bot_obj: Client):
    _load_cache()
    if not STORAGE_CH or not accounts:
        return
    ub = accounts[0].get("userbot")
    if not ub:
        return
    try:
        fetched = []
        async for m in ub.get_chat_history(STORAGE_CH, limit=50):
            if m.document or m.video:
                cap = (m.caption or "").split("\n")[0]
                cap = cap.replace("🎬 ", "").replace("**", "").strip()
                fetched.append({"msg_id": m.id, "title": cap or f"File #{m.id}",
                                "res": "", "dur": "", "size": ""})
        if fetched:
            storage_files.clear()
            storage_files.extend(reversed(fetched))
            _save_cache()
            print(f"[STORAGE] Refreshed {len(fetched)} files via userbot")
    except Exception as e:
        print(f"[STORAGE] Userbot fetch skipped: {e}")

async def upload_to_storage(bot: Client, path: str, track: Track) -> Optional[int]:
    if not STORAGE_CH:
        return None
    try:
        vq  = str(pick_vq(track.height)).split(".")[-1]
        cap = (f"🎬 **{track.title}**\n{'━'*22}\n"
               f"📐 Resolution : `{fres(track.width, track.height)}`\n"
               f"🎞 VC Quality  : `{vq}`\n"
               f"🔊 Audio       : `STUDIO · AAC 320k · Stereo · 48kHz`\n"
               f"⏱ Duration    : `{fdur(track.duration)}`\n"
               f"📦 File Size   : `{fsz(track.size)}`\n"
               f"📅 Saved At    : `{now_utc()}`\n{'━'*22}\n🔁 Replay: `/replay MSGID`")
        msg = await bot.send_document(STORAGE_CH, path, caption=cap,
                                      force_document=True,
                                      file_name=f"{track.title[:50]}.mp4")
        try:
            await bot.edit_message_caption(STORAGE_CH, msg.id,
                                           caption=cap.replace("MSGID", str(msg.id)))
        except: pass
        try:
            await bot.pin_chat_message(STORAGE_CH, msg.id, disable_notification=True)
        except: pass
        storage_files.append({"msg_id": msg.id, "title": track.title,
                               "res": fres(track.width, track.height),
                               "dur": fdur(track.duration), "size": fsz(track.size)})
        if len(storage_files) > 100:
            storage_files.pop(0)
        _save_cache()
        print(f"[STORAGE] ✓ uploaded msg_id={msg.id}")
        return msg.id
    except Exception as e:
        print(f"[STORAGE ERR] {e}")
        return None

async def dl_from_storage(bot: Client, msg_id: int, save_path: str) -> bool:
    try:
        msg = await bot.get_messages(STORAGE_CH, msg_id)
        result = await msg.download(file_name=save_path)
        if not result:
            return False
        # Pyrogram may save to a slightly different path — move it if needed
        result = str(result)
        if result != save_path and os.path.exists(result):
            os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
            os.rename(result, save_path)
        return os.path.exists(save_path)
    except Exception as e:
        print(f"[STORAGE DL ERR] {e}"); return False

async def refresh_storage_files() -> int:
    if not STORAGE_CH or not accounts:
        return 0
    ub = accounts[0].get("userbot")
    if not ub:
        return len(storage_files)
    try:
        fetched = []
        async for m in ub.get_chat_history(STORAGE_CH, limit=50):
            if m.document or m.video:
                cap = (m.caption or "").split("\n")[0]
                cap = cap.replace("🎬 ", "").replace("**", "").strip()
                fetched.append({"msg_id": m.id, "title": cap or f"File #{m.id}",
                                "res": "", "dur": "", "size": ""})
        if fetched:
            storage_files.clear()
            storage_files.extend(reversed(fetched))
            _save_cache()
        print(f"[STORAGE] Refreshed: {len(fetched)} files via userbot")
        return len(fetched)
    except Exception as e:
        print(f"[STORAGE REFRESH ERR] {e}")
        return -1


# ── Download ──────────────────────────────────────────────────────
async def dl_video(session, bot: Client, message: dict,
                   save_path: str, status_cb=None) -> bool:
    vid = message.get("video") or message.get("document")
    if not vid: return False
    fsize = int(vid.get("file_size") or 0)

    if fsize > 20 * 1024 * 1024:
        print(f"[DL] MTProto ({fsz(fsize)})...")
        try:
            last_pct = [0]
            async def progress(cur, tot):
                pct = int(cur * 100 / tot)
                if pct - last_pct[0] >= 20 and status_cb:
                    last_pct[0] = pct
                    await status_cb(f"⬇️ Downloading... `{pct}%`\n📦 {fsz(cur)} / {fsz(tot)}")
            msg  = await bot.get_messages(message["chat"]["id"], message["message_id"])
            path = await msg.download(file_name=save_path, progress=progress)
            return path is not None
        except Exception as e:
            print(f"[DL ERR] {e}"); return False
    else:
        print(f"[DL] Bot API ({fsz(fsize)})...")
        fp = await tg_get_file(session, vid["file_id"])
        return await tg_dl_file(session, fp, save_path) if fp else False

async def dl_from_url(url: str, save_path: str) -> tuple[bool, str]:
    """Download video from any URL using yt-dlp, return (success, log)."""
    global _url_task_status
    log_lines = []

    def log(msg: str):
        log_lines.append(msg)
        if _url_task_status:
            _url_task_status["log"] = "\n".join(log_lines)
        print(f"[YTDLP] {msg}")

    log(f"⬇️ Downloading: {url}")
    try:
        cmd = [
            "yt-dlp",
            "-f", "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
            "--merge-output-format", "mp4",
            "--no-playlist",
            "--no-warnings",
            "-o", save_path, url
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
        out = stdout.decode() + stderr.decode()
        for line in out.split("\n"):
            line = line.strip()
            if line and ("[download]" in line or "Merger" in line or "Destination" in line):
                log(line[:120])

        if proc.returncode != 0:
            log(f"❌ yt-dlp error (code {proc.returncode})")
            log(out[-400:])
            return False, "\n".join(log_lines)

        if not os.path.exists(save_path) or os.path.getsize(save_path) == 0:
            log("❌ File nahi bana")
            return False, "\n".join(log_lines)

        log(f"✅ Download complete: {fsz(os.path.getsize(save_path))}")
        return True, "\n".join(log_lines)
    except asyncio.TimeoutError:
        log("❌ Timeout (10 minutes se zyada)")
        return False, "\n".join(log_lines)
    except Exception as e:
        log(f"❌ {e}")
        return False, "\n".join(log_lines)


# ── Playback ──────────────────────────────────────────────────────
def gid_for_account(acc_idx: int) -> int:
    g = GROUP_IDS[acc_idx] if acc_idx < len(GROUP_IDS) else GROUP_CHAT_ID
    return g or GROUP_CHAT_ID

async def play_track(session, track: Track, gid: int):
    state = gs(gid)
    acct  = accounts[state.account_idx] if accounts else None
    if not acct: return

    if track.channel_msg_id and track.channel_msg_id in playing_msg_ids:
        if state.status_chat:
            await send(session, state.status_chat,
                f"⚠️ *'{track.title[:30]}' already playing on another account!*")
        await play_next(session, gid)
        return

    if not os.path.exists(track.path):
        if track.channel_msg_id:
            ok = await dl_from_storage(acct["bot"], track.channel_msg_id, track.path)
            if not ok:
                if state.status_chat:
                    await send(session, state.status_chat, "❌ Storage se re-download fail.")
                await play_next(session, gid); return
        else:
            if state.status_chat:
                await send(session, state.status_chat, "❌ File nahi mili.")
            await play_next(session, gid); return

    vq = pick_vq(track.height)
    for attempt in range(1, 4):
        try:
            stream = tc.MediaStream(
                media_path=track.path,
                audio_parameters=tc.AudioQuality.STUDIO,
                video_parameters=vq,
                audio_flags=tc.MediaStream.Flags.REQUIRED,
                video_flags=tc.MediaStream.Flags.AUTO_DETECT,
            )
            await acct["call"].play(gid, stream)
            state.now_playing = track
            state.paused = False
            if track.channel_msg_id:
                playing_msg_ids.add(track.channel_msg_id)
            rnd_badge = " 🎲" if random_mode.get(state.account_idx) else ""
            print(f"[VC] ▶ '{track.title}' [{fres(track.width,track.height)}]{rnd_badge}")

            if state.status_chat and state.status_msg:
                await edit(session, state.status_chat, state.status_msg,
                    f"✅ *Now Playing*{rnd_badge}\n\n"
                    f"🎬 *{track.title}*\n"
                    f"📐 {fres(track.width, track.height)} → 1920×1080 (VC)\n"
                    f"🔊 STUDIO · AAC 320k · Stereo\n"
                    f"⏱ {fdur(track.duration)}  📦 {fsz(track.size)}\n"
                    f"👤 Account #{state.account_idx+1}\n"
                    f"📋 Queue: {len(state.queue)} pending",
                    buttons=btns_playing(gid))
            break
        except Exception as e:
            err_str = str(e)
            print(f"[VC ERR] attempt={attempt}: {err_str}")
            if attempt < 3:
                await asyncio.sleep(2)
            else:
                # Detect common causes and give targeted help
                hint = ""
                el = err_str.lower()
                if "groupcall" in el or "not found" in el or "not started" in el:
                    hint = "🔴 *Cause:* Group VC start nahi hai.\n👉 Group mein Video Chat shuru karo."
                elif "admin" in el or "forbidden" in el or "rights" in el:
                    hint = "🔴 *Cause:* Userbot group admin nahi hai.\n👉 Userbot ko admin banao (VC manage permission chahiye)."
                elif "flood" in el:
                    hint = "🔴 *Cause:* Telegram rate limit.\n👉 Thodi der baad retry karo."
                elif "already" in el and "participant" in el:
                    hint = "ℹ️ Userbot already VC mein hai — stream issue.\n👉 Retry try karo."
                elif "connection" in el or "timeout" in el:
                    hint = "🔴 *Cause:* Network timeout.\n👉 Retry karo."
                else:
                    hint = "• Group Video Chat on hai?\n• Userbot group admin hai?\n• VC manage permission di?"

                state.last_failed_track = track
                if state.status_chat and state.status_msg:
                    await edit(session, state.status_chat, state.status_msg,
                        f"❌ *VC Error (3 attempts):*\n`{err_str[:200]}`\n\n{hint}",
                        buttons=[[{"text": "🔄 Retry VC", "callback_data": f"vc_retry:{gid}"},
                                   {"text": "⏭ Skip",     "callback_data": f"skip:{gid}"}]])

async def play_next(session, gid: int):
    state = gs(gid)
    if state.now_playing:
        if state.now_playing.channel_msg_id:
            playing_msg_ids.discard(state.now_playing.channel_msg_id)
            try: os.remove(state.now_playing.path)
            except: pass
    state.now_playing = None

    if random_mode.get(state.account_idx) and STORAGE_CH and storage_files:
        await play_random_track(session, gid)
        return

    if state.queue:
        nxt = state.queue.popleft()
        await play_track(session, nxt, gid)
    else:
        acct = accounts[state.account_idx] if accounts else None
        if acct:
            try: await acct["call"].leave_call(gid)
            except: pass
        if state.status_chat:
            await send(session, state.status_chat,
                "✅ *Queue khatam! VC se nikal gaya.*\n📹 Naya video bhejo.",
                buttons=btns_main())

async def play_random_track(session, gid: int):
    state = gs(gid)
    if not storage_files:
        if state.status_chat:
            await send(session, state.status_chat, "⚠️ Storage channel mein koi file nahi hai.")
        return
    available = [f for f in storage_files if f["msg_id"] not in playing_msg_ids]
    if not available:
        available = storage_files
    chosen = random.choice(available)
    msg_id = chosen["msg_id"]
    title  = chosen["title"]
    sp     = f"downloads/random_{msg_id}.mp4"
    if state.status_chat:
        await send(session, state.status_chat,
            f"🎲 *Random pick!*\n🎬 {title}\n⬇️ Downloading...")
    bot_obj = accounts[0]["bot"]
    ok = await dl_from_storage(bot_obj, msg_id, sp)
    if not ok:
        await play_next(session, gid)
        return
    p = probe(sp)
    track = Track(path=sp, title=title, duration=p["dur"],
                  width=p["w"], height=p["h"],
                  size=os.path.getsize(sp), channel_msg_id=msg_id)
    state.status_msg = None
    await play_track(session, track, gid)

async def cmd_play_on_account(session, chat_id: int, acc_idx: int, msg_id_or_title: str):
    if acc_idx < 0 or acc_idx >= len(accounts):
        await send(session, chat_id,
            f"❌ Account #{acc_idx+1} nahi hai. {len(accounts)} account(s) available.")
        return
    gid   = gid_for_account(acc_idx)
    state = gs(gid)
    state.account_idx = acc_idx
    if not STORAGE_CH:
        await send(session, chat_id, "⚠️ Storage channel set nahi hai.")
        return
    if not msg_id_or_title.lstrip("-").isdigit():
        await send(session, chat_id,
            "❌ *Usage:* `/play <acc#> <msg_id>`\nExample: `/play 1 123456`")
        return
    mid     = int(msg_id_or_title)
    bot_obj = accounts[0]["bot"]
    status  = await send(session, chat_id,
        f"📥 *Account #{acc_idx+1} ke liye fetch kar raha hoon...*\n`msg_id={mid}` → Group `{gid}`")
    sid = status.get("result", {}).get("message_id")
    if mid in playing_msg_ids:
        await edit(session, chat_id, sid, "⚠️ *Ye video already kisi aur account pe chal raha hai!*")
        return
    try:
        ch_msg = await bot_obj.get_messages(STORAGE_CH, mid)
        if not (ch_msg.document or ch_msg.video):
            await edit(session, chat_id, sid, "❌ Us msg_id pe video nahi mili.")
            return
        cap   = ch_msg.caption or ""
        title = cap.split("\n")[0].replace("🎬 ", "").replace("**", "").strip() or f"file_{mid}"
        sp    = f"downloads/play_acc{acc_idx}_{mid}.mp4"
        await edit(session, chat_id, sid, f"⬇️ *Downloading (Account #{acc_idx+1})...*\n🎬 {title}")
        ok = await dl_from_storage(bot_obj, mid, sp)
        if not ok:
            await edit(session, chat_id, sid, "❌ Download fail hua."); return
        p = probe(sp)
        track = Track(path=sp, title=title, duration=p["dur"],
                      width=p["w"], height=p["h"],
                      size=os.path.getsize(sp), channel_msg_id=mid)
        state.status_chat = chat_id
        state.status_msg  = sid
        if state.now_playing:
            state.queue.append(track)
            await edit(session, chat_id, sid,
                f"📋 *Queue mein add (Account #{acc_idx+1})*\n🎬 {title}\nPosition #{len(state.queue)}")
        else:
            await play_track(session, track, gid)
    except Exception as e:
        if sid: await edit(session, chat_id, sid, f"❌ `{e}`")


# ── Files helpers ─────────────────────────────────────────────────
async def show_files(session, chat_id, msg_id=None):
    if not STORAGE_CH:
        text = "⚠️ Storage channel set nahi hai.\n.env mein STORAGE_CHANNEL_ID dalo."
    elif not storage_files:
        text = "📁 *Storage — Empty*\n\n_(Koi file nahi mili. Pehle ek video bhejo!)_"
    else:
        lines = [f"📁 *Storage — {len(storage_files)} Files:*\n"]
        for f in reversed(storage_files[-20:]):
            res = f" `{f['res']}`" if f.get("res") else ""
            dur = f" ⏱{f['dur']}" if f.get("dur") else ""
            lines.append(f"• `/replay {f['msg_id']}` — *{f['title'][:35]}*{res}{dur}")
        text = "\n".join(lines)
    btns = [[{"text": "🔄 Refresh", "callback_data": "files_refresh"},
             {"text": "◀️ Back",    "callback_data": "back_main"}]]
    if msg_id:
        await edit(session, chat_id, msg_id, text, buttons=btns)
    else:
        await send(session, chat_id, text, buttons=btns)


# ── Callback handler ──────────────────────────────────────────────
# ── Process Management Helpers ───────────────────────────────────
def _proc_name(filename: str) -> str:
    """Derive a process key from a filename (strip ext for .py/.js)."""
    base, ext = os.path.splitext(filename)
    return base if ext in (".py", ".js") else filename

def _find_entry_point(folder: str) -> Optional[str]:
    """Find the main script inside an extracted zip folder."""
    for candidate in ["main.py", "app.py", "bot.py", "run.py", "index.py",
                       "start.py", "server.py",
                       "index.js", "main.js", "app.js", "bot.js"]:
        p = os.path.join(folder, candidate)
        if os.path.exists(p):
            return p
    # one level deep
    for sub in os.listdir(folder):
        sp = os.path.join(folder, sub)
        if os.path.isdir(sp):
            for candidate in ["main.py", "app.py", "bot.py", "index.py",
                               "index.js", "main.js"]:
                p = os.path.join(sp, candidate)
                if os.path.exists(p):
                    return p
    return None

def _proc_status_icon(name: str) -> str:
    p = _procs.get(name)
    if not p:
        return "⭕ Stopped"
    s = p.get("status", "stopped")
    return {"running": "🟢 Running", "stopped": "⭕ Stopped", "crashed": "🔴 Crashed"}.get(s, s)

def btns_process(name: str) -> list:
    p = _procs.get(name, {})
    running = p.get("status") == "running"
    return [
        [
            {"text": "⏹ Stop" if running else "▶️ Start",
             "callback_data": f"proc:{'stop' if running else 'start'}:{name}"},
            {"text": "🔄 Restart", "callback_data": f"proc:restart:{name}"},
        ],
        [
            {"text": "📋 Logs",   "callback_data": f"proc:logs:{name}"},
            {"text": "🗑 Delete", "callback_data": f"proc:del:{name}"},
        ],
    ]

async def _stream_proc_output(name: str, stream):
    """Background task: read proc stdout/stderr into log deque."""
    log = _procs[name]["log"]
    try:
        async for raw in stream:
            line = raw.decode(errors="replace").rstrip()
            if line:
                log.append(line)
    except Exception:
        pass

async def proc_start(name: str) -> tuple[bool, str]:
    """Start a hosted script by name. Returns (ok, message)."""
    # Find path
    path = None
    for candidate in [
        os.path.join(HOSTED_DIR, name + ".py"),
        os.path.join(HOSTED_DIR, name + ".js"),
        os.path.join(HOSTED_DIR, name),          # folder (zip extracted)
        os.path.join(HOSTED_DIR, name + ".py").replace(".py.py", ".py"),
    ]:
        if os.path.exists(candidate):
            path = candidate
            break
    if not path:
        return False, f"❌ `{name}` — file ya folder nahi mila HOSTED_DIR mein."

    # Stop existing if running
    await proc_stop(name, silent=True)

    # Determine command
    if os.path.isdir(path):
        entry = _find_entry_point(path)
        if not entry:
            return False, (f"❌ `{name}/` mein koi entry point nahi mila.\n"
                           f"Chahiye: main.py / app.py / bot.py / index.js etc.")
        cmd = ["python3", entry] if entry.endswith(".py") else ["node", entry]
        cwd = path
    elif path.endswith(".py"):
        cmd = ["python3", path]
        cwd = os.path.dirname(path)
    elif path.endswith(".js"):
        cmd = ["node", path]
        cwd = os.path.dirname(path)
    else:
        return False, f"❌ `{name}` — run nahi kar sakta (sirf .py/.js supported)."

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        log_deque: deque = deque(maxlen=200)
        task = asyncio.create_task(
            _stream_proc_output(name, proc.stdout))
        _procs[name] = {
            "proc": proc, "log": log_deque, "task": task,
            "started": time.time(), "path": path, "status": "running",
            "cmd": " ".join(cmd),
        }

        # Watch for early crash in background
        async def _watch():
            rc = await proc.wait()
            if name in _procs and _procs[name].get("proc") is proc:
                _procs[name]["status"] = "crashed" if rc != 0 else "stopped"
                _procs[name]["log"].append(f"[PROC] Exited with code {rc}")
        asyncio.create_task(_watch())

        return True, f"✅ *Started:* `{name}`\nCmd: `{' '.join(cmd)}`"
    except Exception as e:
        return False, f"❌ Start fail: `{e}`"

async def proc_stop(name: str, silent: bool = False) -> tuple[bool, str]:
    """Stop a running process."""
    p = _procs.get(name)
    if not p or p.get("status") != "running":
        return False, "⭕ Already stopped." if not silent else ""
    proc = p["proc"]
    try:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            proc.kill()
        p["status"] = "stopped"
        p["log"].append("[PROC] Terminated by user")
        return True, f"⏹ *Stopped:* `{name}`"
    except Exception as e:
        return False, f"❌ Stop fail: `{e}`"

def proc_logs_text(name: str, lines: int = 30) -> str:
    """Return last N lines of process output."""
    p = _procs.get(name)
    if not p:
        return "⭕ Process nahi chal raha ya kabhi start nahi hua."
    log = list(p["log"])
    if not log:
        return "📋 Log khali hai (abhi koi output nahi)."
    tail = log[-lines:]
    status = _proc_status_icon(name)
    uptime = int(time.time() - p["started"]) if p.get("started") else 0
    header = f"📋 *{name}* — {status} | ⏱ {fdur(uptime)}\n\n"
    return header + "```\n" + "\n".join(tail) + "\n```"

# ── File Hosting Helpers ──────────────────────────────────────────
async def run_cmd(cmd: list, cwd: str = None) -> tuple:
    """Run command async, return (returncode, output_str)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT)
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=180)
        return proc.returncode, (out or b"").decode(errors="replace")
    except asyncio.TimeoutError:
        return -1, "⏱ Timeout (180s)"
    except Exception as e:
        return -1, str(e)

async def auto_install_deps(folder: str) -> str:
    """Detect requirements.txt / package.json and install deps. Returns log."""
    logs = []
    req  = os.path.join(folder, "requirements.txt")
    pkg  = os.path.join(folder, "package.json")
    if os.path.exists(req):
        logs.append("📦 *requirements.txt mila* — pip install shuru...")
        rc, out = await run_cmd(["pip", "install", "-r", req, "-q"])
        snippet = out.strip()[-600:] if out.strip() else "(no output)"
        logs.append(f"{'✅ pip done' if rc == 0 else '⚠️ pip errors'}\n`{snippet}`")
    if os.path.exists(pkg):
        logs.append("📦 *package.json mila* — npm install shuru...")
        rc, out = await run_cmd(["npm", "install", "--prefix", folder])
        snippet = out.strip()[-600:] if out.strip() else "(no output)"
        logs.append(f"{'✅ npm done' if rc == 0 else '⚠️ npm errors'}\n`{snippet}`")
    return "\n".join(logs) if logs else "ℹ️ Koi requirements.txt / package.json nahi mila."

async def detect_py_imports_and_install(py_path: str) -> str:
    """Scan .py imports and pip-install any non-stdlib packages found."""
    STDLIB = {
        "os","sys","re","json","time","math","random","subprocess","asyncio",
        "datetime","pathlib","shutil","traceback","typing","collections","io",
        "functools","itertools","string","hashlib","base64","urllib","http",
        "socket","threading","logging","copy","enum","abc","dataclasses",
        "contextlib","weakref","queue","signal","glob","fnmatch","struct",
        "array","csv","xml","html","email","smtplib","sqlite3","gzip","zipfile",
        "tarfile","tempfile","platform","builtins","inspect","importlib","gc",
        "ctypes","multiprocessing","concurrent","unittest","pprint","textwrap",
        "decimal","fractions","statistics","cmath","heapq","bisect","uuid",
        "secrets","hmac","zlib","bz2","lzma","pickle","shelve","dbm","token",
        "tokenize","ast","dis","code","codeop","atexit","warnings","__future__",
    }
    try:
        with open(py_path, encoding="utf-8", errors="replace") as f:
            src = f.read()
    except Exception as e:
        return f"❌ File read error: {e}"

    pkgs: set[str] = set()
    for line in src.splitlines():
        line = line.strip()
        if line.startswith("import "):
            for part in line[7:].split(","):
                name = part.strip().split(".")[0].split(" as ")[0].strip()
                if name and name not in STDLIB:
                    pkgs.add(name)
        elif line.startswith("from ") and " import " in line:
            name = line[5:].split()[0].split(".")[0]
            if name and name not in STDLIB:
                pkgs.add(name)

    if not pkgs:
        return "ℹ️ Koi external import nahi mila — install ki zarurat nahi."
    logs = [f"📦 *Imports detected:* `{', '.join(sorted(pkgs))}`\npip install shuru..."]
    rc, out = await run_cmd(["pip", "install", *sorted(pkgs), "-q"])
    snippet = out.strip()[-600:] if out.strip() else "(no output)"
    logs.append(f"{'✅ Done' if rc == 0 else '⚠️ Kuch errors'}\n`{snippet}`")
    return "\n".join(logs)

def hosted_file_url(filename: str) -> str:
    """Return the public download URL for a hosted file."""
    domains = os.getenv("REPLIT_DOMAINS", "")
    domain  = domains.split(",")[0].strip() if domains else "localhost"
    return f"https://{domain}/hosted/{filename}"

async def handle_cb(session, cb: dict):
    cb_id   = cb["id"]
    data    = cb.get("data", "")
    uid     = cb.get("from", {}).get("id", 0)
    chat_id = cb["message"]["chat"]["id"]
    msg_id  = cb["message"]["message_id"]

    if uid != ADMIN_USER_ID:
        await answer_cb(session, cb_id, "⛔ Sirf admin!", alert=True)
        return

    parts  = data.split(":")
    action = parts[0]
    gid    = int(parts[1]) if len(parts) > 1 and parts[1].lstrip("-").isdigit() else GROUP_CHAT_ID
    state  = gs(gid)
    acct   = accounts[state.account_idx] if accounts else None

    if action == "pause":
        if acct and state.now_playing and not state.paused:
            try:
                await acct["call"].pause(gid)
                state.paused = True
                await answer_cb(session, cb_id, "⏸ Paused")
                await edit(session, chat_id, msg_id,
                    f"⏸ *Paused*\n\n🎬 {state.now_playing.title}\n"
                    f"📐 {fres(state.now_playing.width, state.now_playing.height)}\n"
                    f"📋 Queue: {len(state.queue)} pending",
                    buttons=btns_paused(gid))
            except Exception as e:
                await answer_cb(session, cb_id, f"❌ {e}", alert=True)
        else:
            await answer_cb(session, cb_id, "Kuch play nahi ho raha")

    elif action == "resume":
        if acct and state.now_playing and state.paused:
            try:
                await acct["call"].resume(gid)
                state.paused = False
                await answer_cb(session, cb_id, "▶️ Resumed")
                await edit(session, chat_id, msg_id,
                    f"✅ *Now Playing*\n\n🎬 {state.now_playing.title}\n"
                    f"📐 {fres(state.now_playing.width, state.now_playing.height)}\n"
                    f"📋 Queue: {len(state.queue)} pending",
                    buttons=btns_playing(gid))
            except Exception as e:
                await answer_cb(session, cb_id, f"❌ {e}", alert=True)
        else:
            await answer_cb(session, cb_id, "Kuch play nahi ho raha")

    elif action == "stop":
        if acct:
            try: await acct["call"].leave_call(gid)
            except: pass
        if state.now_playing and state.now_playing.channel_msg_id:
            playing_msg_ids.discard(state.now_playing.channel_msg_id)
        random_mode[state.account_idx] = False
        state.queue.clear(); state.now_playing = None
        await answer_cb(session, cb_id, "⏹ Stopped")
        await edit(session, chat_id, msg_id, "⏹ *Stopped. Queue clear.*\n📹 Naya video bhejo.",
                   buttons=btns_main())

    elif action == "skip":
        if acct and state.now_playing:
            try: await acct["call"].leave_call(gid)
            except: pass
            await answer_cb(session, cb_id, "⏭ Skipping...")
            await play_next(session, gid)
        else:
            await answer_cb(session, cb_id, "Kuch nahi chal raha")

    elif action == "queue":
        await answer_cb(session, cb_id)
        q = list(state.queue)
        if not q:
            txt  = "📋 *Queue khali hai.*"
            btns = [[{"text": "◀️ Back", "callback_data": "back_main"}]]
        else:
            lines = [f"📋 *Queue ({len(q)} videos):*\n"]
            for i, t in enumerate(q):
                lines.append(f"`{i+1}.` 🎬 *{t.title[:35]}*\n"
                             f"     📐 {fres(t.width,t.height)} ⏱ {fdur(t.duration)}")
            txt  = "\n".join(lines[:25])
            btns = [[{"text": f"🗑 Remove #{i+1}", "callback_data": f"rmq:{gid}:{i}"}]
                    for i in range(min(len(q), 6))]
            btns.append([{"text": "🗑 Clear All", "callback_data": f"clearq:{gid}"},
                         {"text": "◀️ Back",      "callback_data": "back_main"}])
        await edit(session, chat_id, msg_id, txt, buttons=btns)

    elif action == "status":
        await answer_cb(session, cb_id)
        st  = state
        rnd = " 🎲 Random Mode" if random_mode.get(st.account_idx) else ""
        if st.now_playing:
            txt = (f"{'⏸ Paused' if st.paused else '▶️ Playing'}{rnd}\n\n"
                   f"🎬 *{st.now_playing.title}*\n"
                   f"📐 {fres(st.now_playing.width, st.now_playing.height)} → 1920×1080\n"
                   f"⏱ {fdur(st.now_playing.duration)}  📦 {fsz(st.now_playing.size)}\n"
                   f"👤 Account #{st.account_idx+1}\n"
                   f"📋 Queue: {len(st.queue)} pending")
            b = btns_paused(gid) if st.paused else btns_playing(gid)
        else:
            txt = f"😴 *Kuch play nahi ho raha.*{rnd}"
            b   = btns_main()
        await edit(session, chat_id, msg_id, txt, buttons=b)

    elif action == "rmq":
        idx = int(parts[2]) if len(parts) > 2 else -1
        q   = list(state.queue)
        if 0 <= idx < len(q):
            removed = q.pop(idx)
            state.queue = deque(q)
            await answer_cb(session, cb_id, f"🗑 {removed.title[:30]}")
        else:
            await answer_cb(session, cb_id, "Invalid")

    elif action == "clearq":
        state.queue.clear()
        await answer_cb(session, cb_id, "🗑 Queue cleared")
        await edit(session, chat_id, msg_id, "🗑 *Queue clear ho gaya.*", buttons=btns_main())

    elif action == "settings":
        await answer_cb(session, cb_id)
        acct_info = "\n".join(
            f"{'✅' if gid_for_account(i) == gid and state.account_idx == i else '  '} "
            f"Acc#{i+1}: @{a['me'].username or a['me'].id} → Group {gid_for_account(i)}"
            for i, a in enumerate(accounts))
        await edit(session, chat_id, msg_id,
            f"⚙️ *Settings*\n\n"
            f"*Accounts & Groups:*\n{acct_info}\n\n"
            f"*Storage Channel:* `{'✅ ' + str(STORAGE_CH) if STORAGE_CH else '❌ Not set'}`\n"
            f"*Files cached:* {len(storage_files)}\n\n"
            f"🌐 *Web Dashboard:* Settings sab web pe mil jayega!",
            buttons=btns_settings())

    elif action == "acct":
        idx = int(parts[1])
        if 0 <= idx < len(accounts):
            state.account_idx = idx
            me = accounts[idx]["me"]
            await answer_cb(session, cb_id, f"✅ Account #{idx+1}: @{me.username or me.id}")
        else:
            await answer_cb(session, cb_id, "Invalid account")

    elif action == "back_main":
        await answer_cb(session, cb_id)
        await edit(session, chat_id, msg_id, "🎬 *VC Video Bot*\n\nMenu:", buttons=btns_main())

    elif action == "files":
        await answer_cb(session, cb_id)
        await show_files(session, chat_id, msg_id)

    elif action == "files_refresh":
        await answer_cb(session, cb_id, "🔄 Refreshing...")
        n = await refresh_storage_files()
        if n >= 0:
            await show_files(session, chat_id, msg_id)
        else:
            await edit(session, chat_id, msg_id, "❌ Refresh failed.",
                       buttons=[[{"text": "◀️ Back", "callback_data": "back_main"}]])

    elif action == "random_start":
        acc_idx = state.account_idx
        if random_mode.get(acc_idx):
            random_mode[acc_idx] = False
            await answer_cb(session, cb_id, "🎲 Random mode OFF")
            await edit(session, chat_id, msg_id,
                "🎲 *Random Mode OFF*\nManual queue pe wapas.", buttons=btns_main())
        else:
            if not storage_files:
                await answer_cb(session, cb_id, "⚠️ Storage mein koi file nahi!", alert=True)
                return
            random_mode[acc_idx] = True
            await answer_cb(session, cb_id, "🎲 Random mode ON!")
            if not state.now_playing:
                await edit(session, chat_id, msg_id,
                    "🎲 *Random Mode ON!*\n\nStorage se random video pick ho raha hai...",
                    buttons=btns_main())
                await play_random_track(session, gid)
            else:
                await edit(session, chat_id, msg_id,
                    f"🎲 *Random Mode ON!*\nCurrent ke baad random chalta rahega.\n"
                    f"▶️ Ab chal raha: *{state.now_playing.title}*",
                    buttons=btns_playing(gid))
    elif action == "vc_retry":
        state2 = gs(gid)
        trk = state2.last_failed_track
        if not trk:
            await answer_cb(session, cb_id, "⚠️ Retry karne ke liye track nahi mila.", alert=True)
            return
        await answer_cb(session, cb_id, "🔄 Retrying VC...")
        state2.last_failed_track = None
        state2.status_msg = msg_id
        await play_track(session, trk, gid)

    elif action == "proc":
        # proc:sub:name  e.g. proc:start:myscript
        sub  = parts[1] if len(parts) > 1 else ""
        name = parts[2] if len(parts) > 2 else ""
        if not name:
            await answer_cb(session, cb_id, "❌ Name missing", alert=True)
            return

        if sub == "start":
            await answer_cb(session, cb_id, f"▶️ Starting {name}...")
            ok, msg2 = await proc_start(name)
            await edit(session, chat_id, msg_id,
                f"{msg2}\n\n{_proc_status_icon(name)}",
                buttons=btns_process(name))

        elif sub == "stop":
            await answer_cb(session, cb_id, f"⏹ Stopping {name}...")
            ok, msg2 = await proc_stop(name)
            await edit(session, chat_id, msg_id,
                f"{msg2}\n\n{_proc_status_icon(name)}",
                buttons=btns_process(name))

        elif sub == "restart":
            await answer_cb(session, cb_id, f"🔄 Restarting {name}...")
            await proc_stop(name, silent=True)
            await asyncio.sleep(1)
            ok, msg2 = await proc_start(name)
            await edit(session, chat_id, msg_id,
                f"🔄 *Restarted:* `{name}`\n{msg2}\n\n{_proc_status_icon(name)}",
                buttons=btns_process(name))

        elif sub == "logs":
            await answer_cb(session, cb_id, "📋 Logs fetch ho rahi hain...")
            log_text = proc_logs_text(name, lines=40)
            await edit(session, chat_id, msg_id,
                log_text[:3800],
                buttons=btns_process(name))

        elif sub == "del":
            await answer_cb(session, cb_id, f"🗑 Deleting {name}...")
            await proc_stop(name, silent=True)
            _procs.pop(name, None)
            # Delete from hosted_files
            deleted = False
            for candidate in [
                os.path.join(HOSTED_DIR, name + ".py"),
                os.path.join(HOSTED_DIR, name + ".js"),
                os.path.join(HOSTED_DIR, name),
            ]:
                if os.path.exists(candidate):
                    try:
                        if os.path.isdir(candidate):
                            import shutil as _sh2
                            _sh2.rmtree(candidate)
                        else:
                            os.remove(candidate)
                        deleted = True
                    except Exception as de:
                        await edit(session, chat_id, msg_id, f"❌ Delete fail: `{de}`")
                        return
                    break
            if deleted:
                await edit(session, chat_id, msg_id, f"✅ *Deleted:* `{name}`\n/hosted — list dekho")
            else:
                await edit(session, chat_id, msg_id, f"⚠️ File nahi mili: `{name}`")
        else:
            await answer_cb(session, cb_id)

    else:
        await answer_cb(session, cb_id)


# ── Message handler ───────────────────────────────────────────────
async def handle_msg(session, bot: Client, message: dict):
    try:
        chat_id   = message["chat"]["id"]
        uid       = message.get("from", {}).get("id", 0)
        text      = (message.get("text") or "").strip()
        chat_type = message["chat"]["type"]
        state     = gs()

        print(f"[MSG] uid={uid} type={chat_type} text='{text[:40]}'")

        if chat_type != "private":
            return
        if uid != ADMIN_USER_ID:
            await send(session, chat_id, f"⛔ *Access Denied*\n`Your ID: {uid}`")
            return

        if text.startswith("/start"):
            acct_lines = "\n".join(
                f"  {'✅' if i==state.account_idx else '•'} Acc#{i+1}: "
                f"@{a['me'].username or a['me'].id} → Group {gid_for_account(i)}"
                for i, a in enumerate(accounts))
            await send(session, chat_id,
                f"🎬 *VC Video Bot — Advanced*\n\n"
                f"📹 Video bhejo → auto queue + VC play\n\n"
                f"*Commands:*\n"
                f"▶️ /status — ab kya chal raha hai\n"
                f"📋 /queue — queue list + remove\n"
                f"⏭ /skip — current skip\n"
                f"⏹ /stop — sab band\n"
                f"⏸ /pause · ▶️ /resume\n"
                f"📁 /files — storage channel\n"
                f"🔁 /replay <id> — stored video play\n"
                f"🎮 /play <acc#> <id> — specific account pe play\n"
                f"🔗 /url <link> — YouTube/direct URL se play\n"
                f"🎲 /random — random mode on/off\n"
                f"👤 /account — userbot switch\n"
                f"🏓 /ping — bot alive check\n\n"
                f"*🗂 File Hosting:*\n"
                f"📤 .py / .js / .zip bhejo → auto host + pkg install\n"
                f"📋 /hosted — hosted files list + URLs\n"
                f"🗑 /delhosted <naam> — file delete karo\n\n"
                f"*Accounts & Groups:*\n{acct_lines}\n\n"
                f"*Storage:* {'`✅ ' + str(STORAGE_CH) + '`' if STORAGE_CH else '`⚠️ Local only`'}\n"
                f"*Files:* {len(storage_files)} cached",
                buttons=btns_main())
            return

        if text.startswith("/ping"):
            import time as _t
            t0 = _t.time()
            m  = await send(session, chat_id, "🏓 Pong!")
            ms = int((_t.time() - t0) * 1000)
            mid2 = m.get("result", {}).get("message_id")
            if mid2:
                await edit(session, chat_id, mid2, f"🏓 Pong! `{ms}ms`")
            return

        if text.startswith("/status"):
            st  = state
            rnd = " 🎲" if random_mode.get(st.account_idx) else ""
            if st.now_playing:
                txt = (f"{'⏸ Paused' if st.paused else '▶️ Playing'}{rnd}\n\n"
                       f"🎬 *{st.now_playing.title}*\n"
                       f"📐 {fres(st.now_playing.width, st.now_playing.height)} → 1920×1080\n"
                       f"⏱ {fdur(st.now_playing.duration)} | 📦 {fsz(st.now_playing.size)}\n"
                       f"👤 Account #{st.account_idx+1}\n"
                       f"📋 Queue: {len(st.queue)} pending")
                b = btns_paused(GROUP_CHAT_ID) if st.paused else btns_playing(GROUP_CHAT_ID)
            else:
                txt = "😴 *Kuch play nahi ho raha.*\n📹 Video bhejo!"
                b   = btns_main()
            await send(session, chat_id, txt, buttons=b)
            return

        if text.startswith("/queue"):
            q = list(state.queue)
            if not q:
                await send(session, chat_id, "📋 *Queue khali hai.*", buttons=btns_main())
            else:
                lines = [f"📋 *Queue ({len(q)} videos):*\n"]
                for i, t in enumerate(q):
                    lines.append(f"`{i+1}.` 🎬 *{t.title[:35]}*\n"
                                 f"     📐 {fres(t.width,t.height)} ⏱ {fdur(t.duration)}")
                btns2 = [[{"text": f"🗑 Remove #{i+1}",
                           "callback_data": f"rmq:{GROUP_CHAT_ID}:{i}"}]
                         for i in range(min(len(q), 6))]
                btns2.append([{"text": "🗑 Clear All",
                               "callback_data": f"clearq:{GROUP_CHAT_ID}"}])
                await send(session, chat_id, "\n".join(lines[:25]), buttons=btns2)
            return

        if text.startswith("/skip"):
            acct = accounts[state.account_idx] if accounts else None
            if acct and state.now_playing:
                try: await acct["call"].leave_call(GROUP_CHAT_ID)
                except: pass
                await send(session, chat_id, "⏭ *Skipping...*")
                await play_next(session, GROUP_CHAT_ID)
            else:
                await send(session, chat_id, "Kuch play nahi ho raha.")
            return

        if text.startswith("/stop"):
            acct = accounts[state.account_idx] if accounts else None
            if acct:
                try: await acct["call"].leave_call(GROUP_CHAT_ID)
                except: pass
            if state.now_playing and state.now_playing.channel_msg_id:
                playing_msg_ids.discard(state.now_playing.channel_msg_id)
            random_mode[state.account_idx] = False
            state.queue.clear(); state.now_playing = None
            await send(session, chat_id, "⏹ *Stopped. Queue clear.*", buttons=btns_main())
            return

        if text.startswith("/pause"):
            acct = accounts[state.account_idx] if accounts else None
            if acct and state.now_playing and not state.paused:
                try:
                    await acct["call"].pause(GROUP_CHAT_ID)
                    state.paused = True
                    await send(session, chat_id,
                        f"⏸ *Paused*\n🎬 {state.now_playing.title}",
                        buttons=btns_paused(GROUP_CHAT_ID))
                except Exception as e:
                    await send(session, chat_id, f"❌ `{e}`")
            else:
                await send(session, chat_id, "Kuch play nahi ho raha.")
            return

        if text.startswith("/resume"):
            acct = accounts[state.account_idx] if accounts else None
            if acct and state.now_playing and state.paused:
                try:
                    await acct["call"].resume(GROUP_CHAT_ID)
                    state.paused = False
                    await send(session, chat_id,
                        f"▶️ *Resumed*\n🎬 {state.now_playing.title}",
                        buttons=btns_playing(GROUP_CHAT_ID))
                except Exception as e:
                    await send(session, chat_id, f"❌ `{e}`")
            else:
                await send(session, chat_id, "Kuch play nahi ho raha.")
            return

        if text.startswith("/account"):
            if len(accounts) <= 1:
                await send(session, chat_id, "⚠️ Ek hi account hai.\n.env mein STRING_SESSION_2/3 dalo.")
                return
            btns3 = [[{"text": f"{'✅' if i==state.account_idx else '👤'} "
                               f"Acc#{i+1}: @{a['me'].username or a['me'].id} → {gid_for_account(i)}",
                       "callback_data": f"acct:{i}"}]
                     for i, a in enumerate(accounts)]
            await send(session, chat_id,
                f"👤 *Account Select Karo*\n(Current: #{state.account_idx+1})",
                buttons=btns3)
            return

        if text.startswith("/files"):
            await show_files(session, chat_id)
            return

        if text.startswith("/random"):
            acc_idx = state.account_idx
            if random_mode.get(acc_idx):
                random_mode[acc_idx] = False
                await send(session, chat_id, "🎲 *Random Mode OFF*", buttons=btns_main())
            else:
                if not storage_files:
                    await send(session, chat_id,
                        "⚠️ Storage channel mein koi file nahi hai.\nPehle ek video upload karo.")
                    return
                random_mode[acc_idx] = True
                if not state.now_playing:
                    s2 = await send(session, chat_id, "🎲 *Random Mode ON!* Random video pick ho raha hai...")
                    sid = s2.get("result", {}).get("message_id")
                    state.status_chat = chat_id
                    state.status_msg  = sid
                    await play_random_track(session, GROUP_CHAT_ID)
                else:
                    await send(session, chat_id,
                        "🎲 *Random Mode ON!*\nCurrent ke baad random chalta rahega.",
                        buttons=btns_playing(GROUP_CHAT_ID))
            return

        # ── /url <link> — download from URL and play ─────────────
        if text.startswith("/url"):
            rest = text[4:].strip()
            if not rest:
                await send(session, chat_id,
                    "🔗 *Usage:* `/url <video_url>`\n"
                    "Example: `/url https://youtube.com/watch?v=xxx`\n\n"
                    "Supported: YouTube, direct MP4 links, aur bahut saare sites!")
                return
            url_parts = rest.split(None, 1)
            url2  = url_parts[0]
            title2 = url_parts[1] if len(url_parts) > 1 else ""
            s3    = await send(session, chat_id,
                f"🔗 *URL se download shuru ho raha hai...*\n`{url2[:80]}`")
            sid3 = s3.get("result", {}).get("message_id")
            asyncio.create_task(
                _url_download_and_play(session, chat_id, sid3, url2, title2, 0, GROUP_CHAT_ID))
            return

        # ── /play <acc> <msg_id> ─────────────────────────────────
        play_match = None
        if text.lower().startswith("/play"):
            rest = text[5:].strip()
            toks = rest.split()
            if len(toks) >= 2 and toks[0].isdigit():
                play_match = (int(toks[0]) - 1, toks[1])
        if play_match:
            await cmd_play_on_account(session, chat_id, play_match[0], play_match[1])
            return

        # ── /play1, /play2, /play3 shortcuts ────────────────────
        for acc_n in range(1, 4):
            pfx = f"/play{acc_n} "
            if text.lower().startswith(pfx):
                await cmd_play_on_account(session, chat_id, acc_n - 1, text[len(pfx):].strip())
                return

        # ── /replay <msg_id> ────────────────────────────────────
        if text.startswith("/replay"):
            if not STORAGE_CH:
                await send(session, chat_id, "⚠️ Storage channel set nahi hai.")
                return
            pts = text.split()
            if len(pts) < 2 or not pts[1].lstrip("-").isdigit():
                await send(session, chat_id, "❌ *Usage:* `/replay <msg_id>`\n/files se msg_id dekho")
                return
            mid_r   = int(pts[1])
            bot_obj = accounts[0]["bot"] if accounts else None
            s4      = await send(session, chat_id,
                f"📥 *Storage se fetch kar raha hoon...*\n`msg_id={mid_r}`")
            sid4 = s4.get("result", {}).get("message_id")
            if mid_r in playing_msg_ids:
                await edit(session, chat_id, sid4, "⚠️ *Ye video already chal raha hai dusre account pe!*")
                return
            try:
                ch_msg = await bot_obj.get_messages(STORAGE_CH, mid_r)
                if not (ch_msg.document or ch_msg.video):
                    await edit(session, chat_id, sid4, "❌ Us ID pe video nahi mili.")
                    return
                cap2  = ch_msg.caption or ""
                title3 = cap2.split("\n")[0].replace("🎬 ", "").replace("**", "").strip() or f"replay_{mid_r}"
                sp2   = f"downloads/replay_{mid_r}.mp4"
                await edit(session, chat_id, sid4, f"⬇️ *Downloading from storage...*\n🎬 {title3}")
                ok3 = await dl_from_storage(bot_obj, mid_r, sp2)
                if not ok3 or not os.path.exists(sp2):
                    await edit(session, chat_id, sid4,
                        f"❌ *Download fail hua.*\n"
                        f"`msg_id={mid_r}` — Userbot storage channel ka member hai?\n"
                        f"Userbot ko channel mein add karo phir retry karo.")
                    return
                p2 = probe(sp2)
                fsize2 = os.path.getsize(sp2)
                track2 = Track(path=sp2, title=title3, duration=p2["dur"],
                               width=p2["w"], height=p2["h"],
                               size=fsize2, channel_msg_id=mid_r)
                state.status_chat = chat_id
                state.status_msg  = sid4
                if state.now_playing:
                    state.queue.append(track2)
                    await edit(session, chat_id, sid4,
                        f"📋 *Queue mein add hua!*\n🎬 {title3}\nPosition #{len(state.queue)}")
                else:
                    await play_track(session, track2, GROUP_CHAT_ID)
            except Exception as e:
                if sid4: await edit(session, chat_id, sid4, f"❌ `{e}`")
            return

        # ── /hosted — list all hosted files with process buttons ────
        if text.startswith("/hosted"):
            try:
                all_names = sorted(os.listdir(HOSTED_DIR))
            except Exception as e:
                await send(session, chat_id, f"❌ Error: `{e}`")
                return
            if not all_names:
                await send(session, chat_id,
                    "🗂 *Hosted Files — Khali*\n\n"
                    "📤 `.py` `.js` ya `.zip` file bhejo — main host kar dunga!")
                return
            # Send one message per file (with its own process buttons)
            for fname in all_names:
                full     = os.path.join(HOSTED_DIR, fname)
                pk       = _proc_name(fname)
                icon     = _proc_status_icon(pk)
                if os.path.isdir(full):
                    fcount = sum(len(fs) for _, _, fs in os.walk(full))
                    desc   = f"📁 *{fname}/* — {fcount} file(s)"
                else:
                    desc   = f"📄 *{fname}* ({fsz(os.path.getsize(full))})"
                url = hosted_file_url(fname)
                await send(session, chat_id,
                    f"{desc}\n{icon}\n🔗 `{url}`",
                    buttons=btns_process(pk))
            return

        # ── /delhosted <name> — delete a hosted file/folder ──────
        if text.startswith("/delhosted"):
            parts = text.split(None, 1)
            if len(parts) < 2:
                await send(session, chat_id,
                    "❌ *Usage:* `/delhosted <file_ya_folder_naam>`\n"
                    "/hosted — list dekho")
                return
            target = parts[1].strip().lstrip("/").rstrip("/")
            full   = os.path.join(HOSTED_DIR, target)
            if not os.path.exists(full):
                await send(session, chat_id, f"❌ `{target}` — nahi mila.")
                return
            try:
                if os.path.isdir(full):
                    import shutil as _sh
                    _sh.rmtree(full)
                else:
                    os.remove(full)
                await send(session, chat_id, f"✅ *Deleted:* `{target}`")
            except Exception as e:
                await send(session, chat_id, f"❌ Delete fail: `{e}`")
            return

        # ── .py / .js / .zip file received → host it ─────────────
        doc_raw  = message.get("document", {})
        doc_name = doc_raw.get("file_name", "")
        doc_ext  = os.path.splitext(doc_name)[1].lower() if doc_name else ""
        doc_mime = doc_raw.get("mime_type", "")
        is_host_doc = bool(doc_raw) and doc_ext in (".py", ".js", ".zip") and "video" not in doc_mime

        if is_host_doc:
            fid   = doc_raw.get("file_id", "")
            fsize = doc_raw.get("file_size", 0)
            safe_name = "".join(c for c in doc_name if c.isalnum() or c in "._-") or f"file{doc_ext}"

            sm = await send(session, chat_id,
                f"📥 *Receiving:* `{safe_name}` ({fsz(fsize)})\n"
                f"⏳ Download ho raha hai...")
            smid = sm.get("result", {}).get("message_id")

            async def upd(t):
                if smid: await edit(session, chat_id, smid, t)

            # Download from Telegram
            fp = await tg_get_file(session, fid)
            if not fp:
                await upd("❌ File path nahi mila Telegram se.")
                return
            dest = os.path.join(HOSTED_DIR, safe_name)
            ok = await tg_dl_file(session, fp, dest)
            if not ok:
                await upd("❌ Download fail hua.")
                return

            await upd(f"✅ *Downloaded:* `{safe_name}`\n🔄 Processing...")

            install_log = ""
            final_dest  = dest

            if doc_ext == ".zip":
                import zipfile as _zf
                folder_name = os.path.splitext(safe_name)[0]
                extract_dir = os.path.join(HOSTED_DIR, folder_name)
                os.makedirs(extract_dir, exist_ok=True)
                try:
                    with _zf.ZipFile(dest, "r") as z:
                        z.extractall(extract_dir)
                    os.remove(dest)
                    final_dest = extract_dir
                    await upd(f"📦 *Extracted:* `{folder_name}/`\n🔍 Dependencies check ho rahi hain...")
                    install_log = await auto_install_deps(extract_dir)
                    # also check one level deep (in case zip had a subfolder)
                    if not install_log or "nahi mila" in install_log:
                        for sub in os.listdir(extract_dir):
                            sp = os.path.join(extract_dir, sub)
                            if os.path.isdir(sp):
                                extra = await auto_install_deps(sp)
                                if "nahi mila" not in extra:
                                    install_log = extra
                                    break
                except Exception as e:
                    install_log = f"❌ Zip extract error: {e}"

            elif doc_ext == ".py":
                await upd(f"🐍 *Python file:* `{safe_name}`\n🔍 Imports scan ho rahe hain...")
                install_log = await detect_py_imports_and_install(dest)

            elif doc_ext == ".js":
                install_log = "ℹ️ JS file host hua. Node pe run karne ke liye package.json wali zip bhejo."

            display_name = folder_name if doc_ext == ".zip" else safe_name
            proc_key     = _proc_name(display_name)
            url = hosted_file_url(display_name)
            if smid:
                await edit(session, chat_id, smid,
                    f"✅ *Hosted!*\n\n"
                    f"📄 `{display_name}`\n"
                    f"🔗 `{url}`\n\n"
                    f"{install_log}\n\n"
                    f"*Process Controls:*",
                    buttons=btns_process(proc_key))
            return

        # ── Video file received ──────────────────────────────────
        is_video   = "video" in message
        is_vid_doc = ("document" in message and
                      "video" in message["document"].get("mime_type", ""))

        if is_video or is_vid_doc:
            meta   = message.get("video") or message.get("document", {})
            fsize  = int(meta.get("file_size") or 0)
            orig_w = int(meta.get("width") or 0)
            orig_h = int(meta.get("height") or 0)
            orig_d = float(meta.get("duration") or 0)
            fname  = meta.get("file_name", f"video_{message['message_id']}.mp4")
            title4 = os.path.splitext(fname)[0][:50]

            qpos   = len(state.queue) + (1 if state.now_playing else 0)
            q_note = "\n🚀 *Turant play hoga!*" if qpos == 0 else f"\n📋 *Queue position: #{qpos+1}*"

            s5  = await send(session, chat_id,
                f"⬇️ *Downloading...*\n\n🎬 *{title4}*\n"
                f"📐 {fres(orig_w,orig_h)}  ⏱ {fdur(orig_d)}\n"
                f"📦 {fsz(fsize) if fsize else '?'}{q_note}")
            sid5 = s5.get("result", {}).get("message_id")
            raw5 = f"downloads/{message['message_id']}_raw.mp4"
            fin5 = f"downloads/{message['message_id']}.mp4"

            async def upd_status5(txt5):
                if sid5: await edit(session, chat_id, sid5, txt5)

            ok5 = await dl_video(session, bot, message, raw5, upd_status5)
            if not ok5:
                await edit(session, chat_id, sid5, "❌ *Download fail hua.*\nDobara bhejo.")
                return

            await edit(session, chat_id, sid5,
                f"✅ *Downloaded!*\n🔄 *Converting (H.264 + AAC stereo · 1080p)...*\n🎬 {title4}")

            p5  = probe(raw5)
            h5  = p5["h"] or orig_h
            w5  = p5["w"] or orig_w
            dur5 = p5["dur"] or orig_d
            ok6 = await convert(raw5, fin5)
            if not ok6:
                fin5 = raw5
            elif raw5 != fin5:
                try: os.remove(raw5)
                except: pass

            out_w5, out_h5 = (1920, 1080) if ok6 else (w5, h5)
            fsize5 = os.path.getsize(fin5)
            track5 = Track(path=fin5, title=title4, duration=dur5,
                           width=out_w5, height=out_h5, size=fsize5)

            if STORAGE_CH:
                await edit(session, chat_id, sid5,
                    f"✅ *Converted!*\n☁️ *Uploading to storage channel...*\n🎬 {title4}")
                bot_obj5 = accounts[0]["bot"]
                ch_id5   = await upload_to_storage(bot_obj5, fin5, track5)
                track5.channel_msg_id = ch_id5
                if ch_id5:
                    try: os.remove(fin5)
                    except: pass

            state.status_chat = chat_id
            state.status_msg  = sid5

            if state.now_playing:
                state.queue.append(track5)
                await edit(session, chat_id, sid5,
                    f"📋 *Queue mein add hua!*\n\n🎬 *{title4}*\n"
                    f"📐 → 1920×1080 (FHD)  ⏱ {fdur(dur5)}\n"
                    f"📦 {fsz(fsize5)}\n"
                    f"📋 Position #{len(state.queue)} in queue",
                    buttons=[[{"text": "📋 Queue Dekho",
                               "callback_data": f"queue:{GROUP_CHAT_ID}"}]])
            else:
                await edit(session, chat_id, sid5,
                    f"🎬 *VC pe play kar raha hoon...*\n\n🎬 *{title4}*\n"
                    f"📐 → 1920×1080 (FHD)  🔊 STUDIO Stereo")
                await play_track(session, track5, GROUP_CHAT_ID)
            return

        if text and not text.startswith("/"):
            await send(session, chat_id,
                "📹 *Video bhejo* — main VC pe play karunga!\n/start — help",
                buttons=btns_main())

    except Exception:
        print(f"[MSG ERR] {traceback.format_exc()}")


# ── URL download + play (shared by Telegram /url and web dashboard) ──
async def _url_download_and_play(session, chat_id: Optional[int], msg_id: Optional[int],
                                  url: str, title: str, acc_idx: int, gid: int):
    global _url_task_status
    _url_task_status = {"log": "Shuru ho raha hai...", "done": False, "error": None}
    state = gs(gid)
    state.account_idx = acc_idx

    def _log(line: str):
        _url_task_status["log"] = (_url_task_status.get("log", "") + "\n" + line).strip()

    try:
        _log(f"🔗 URL: {url[:100]}")
        sp   = f"downloads/url_{int(time.time())}.mp4"
        ok, log_text = await dl_from_url(url, sp)
        _url_task_status["log"] = log_text

        if not ok:
            _url_task_status.update(done=True, error="Download fail")
            if chat_id and msg_id:
                await edit(session, chat_id, msg_id, f"❌ *URL Download fail hua.*\nyt-dlp error — URL check karo.")
            return

        _log("🔄 Converting (H.264 + AAC stereo · 1080p)...")
        fin = sp.replace(".mp4", "_out.mp4")
        ok2 = await convert(sp, fin)
        if not ok2:
            fin = sp
        elif sp != fin:
            try: os.remove(sp)
            except: pass

        p    = probe(fin)
        ow   = p["w"] if p["w"] else 1920
        oh   = p["h"] if p["h"] else 1080
        dur  = p["dur"]
        sz   = os.path.getsize(fin)
        ttl  = title or (url.split("/")[-1].split("?")[0][:50] or "URL Video")

        _log(f"✅ Convert done: {fres(ow, oh)} · {fdur(dur)} · {fsz(sz)}")
        track = Track(path=fin, title=ttl, duration=dur, width=ow, height=oh, size=sz)

        if STORAGE_CH:
            _log("☁️ Storage channel pe upload ho raha hai...")
            if accounts:
                ch_id6 = await upload_to_storage(accounts[0]["bot"], fin, track)
                track.channel_msg_id = ch_id6
                if ch_id6:
                    try: os.remove(fin)
                    except: pass
            _log(f"✅ Uploaded: msg_id={track.channel_msg_id}")

        if chat_id and msg_id:
            state.status_chat = chat_id
            state.status_msg  = msg_id

        if state.now_playing:
            state.queue.append(track)
            _log(f"📋 Queue mein add: Position #{len(state.queue)}")
            if chat_id and msg_id:
                await edit(session, chat_id, msg_id,
                    f"📋 *Queue mein add hua!*\n\n🎬 *{ttl}*\n"
                    f"📐 {fres(ow, oh)}  ⏱ {fdur(dur)}\n"
                    f"📋 Position #{len(state.queue)} in queue")
        else:
            _log("▶️ VC pe play ho raha hai...")
            if chat_id and msg_id:
                await edit(session, chat_id, msg_id,
                    f"🎬 *VC pe play kar raha hoon...*\n\n🎬 *{ttl}*\n"
                    f"📐 {fres(ow, oh)} → 1920×1080 (FHD)  🔊 STUDIO Stereo")
            await play_track(session, track, gid)

        _url_task_status.update(done=True, error=None)

    except Exception as e:
        err = traceback.format_exc()
        print(f"[URL TASK ERR] {err}")
        _url_task_status.update(done=True, error=str(e))
        if chat_id and msg_id:
            await edit(session, chat_id, msg_id, f"❌ *Error:* `{e}`")


# ── Web Dashboard HTML ────────────────────────────────────────────
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VPlay VC Bot — Dashboard</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0d1117;--surf:#161b22;--surf2:#1c2128;--border:#30363d;--text:#e6edf3;--muted:#8b949e;--accent:#58a6ff;--green:#3fb950;--red:#f85149;--yellow:#d29922;--purple:#bc8cff}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;font-size:14px;line-height:1.5;min-height:100vh}
header{background:var(--surf);border-bottom:1px solid var(--border);padding:12px 20px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.logo{font-size:16px;font-weight:700;display:flex;align-items:center;gap:8px}
.hdr-right{display:flex;align-items:center;gap:12px}
.status-badge{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--muted)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green);animation:blink 2s ease-in-out infinite}
.dot.off{background:var(--red);box-shadow:0 0 6px var(--red)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.4}}
main{max-width:880px;margin:0 auto;padding:20px 16px;display:flex;flex-direction:column;gap:14px}
.card{background:var(--surf);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.card-hdr{padding:10px 16px;border-bottom:1px solid var(--border);font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);display:flex;align-items:center;justify-content:space-between}
.np-idle{padding:24px;text-align:center;color:var(--muted)}
.np-idle .ico{font-size:36px;margin-bottom:6px}
.np-active{padding:14px 16px}
.track-title{font-size:15px;font-weight:600;margin-bottom:6px;line-height:1.3}
.track-meta{display:flex;flex-wrap:wrap;gap:10px;font-size:12px;color:var(--muted);margin-bottom:10px}
.badge{display:inline-flex;align-items:center;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500}
.badge.play{background:rgba(63,185,80,.15);color:var(--green);border:1px solid rgba(63,185,80,.3)}
.badge.pau{background:rgba(210,153,34,.15);color:var(--yellow);border:1px solid rgba(210,153,34,.3)}
.badge.rnd{background:rgba(188,140,255,.15);color:var(--purple);border:1px solid rgba(188,140,255,.3)}
.controls{display:flex;gap:8px;flex-wrap:wrap;padding:10px 16px;background:var(--surf2);border-top:1px solid var(--border)}
.btn{padding:7px 13px;border-radius:6px;border:1px solid var(--border);background:var(--surf);color:var(--text);cursor:pointer;font-size:13px;display:inline-flex;align-items:center;gap:5px;transition:all .15s;font-family:inherit}
.btn:hover{background:var(--surf2);border-color:var(--accent)}
.btn:active{transform:scale(.97)}
.btn.pri{background:var(--accent);color:#000;border-color:var(--accent);font-weight:600}
.btn.pri:hover{opacity:.9}
.btn.dng{color:var(--red);border-color:rgba(248,81,73,.4)}
.btn.dng:hover{background:rgba(248,81,73,.1)}
.btn.suc{color:var(--green);border-color:rgba(63,185,80,.4)}
.btn.suc:hover{background:rgba(63,185,80,.1)}
.btn.pur{color:var(--purple);border-color:rgba(188,140,255,.4)}
.btn.pur:hover{background:rgba(188,140,255,.1)}
.btn.pur.on{background:rgba(188,140,255,.2)}
.btn:disabled{opacity:.35;cursor:not-allowed}
.tabs{display:flex;border-bottom:1px solid var(--border);background:var(--surf);overflow-x:auto}
.tab{padding:10px 16px;cursor:pointer;font-size:13px;color:var(--muted);border-bottom:2px solid transparent;white-space:nowrap;background:none;border-top:none;border-left:none;border-right:none;font-family:inherit;transition:all .15s}
.tab:hover{color:var(--text)}
.tab.act{color:var(--accent);border-bottom-color:var(--accent)}
.qbadge{display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:9px;background:var(--accent);color:#000;font-size:10px;font-weight:700;margin-left:4px}
.panel{display:none;padding:14px}
.panel.act{display:block}
.qi{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:6px;background:var(--surf2);margin-bottom:7px}
.qn{color:var(--muted);font-size:12px;min-width:18px;text-align:center}
.qinfo{flex:1;min-width:0}
.qt{font-size:13px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.qm{font-size:11px;color:var(--muted);margin-top:2px}
.fi{display:flex;align-items:center;gap:10px;padding:8px 12px;border-radius:6px;border:1px solid var(--border);margin-bottom:6px;transition:border-color .15s}
.fi:hover{border-color:var(--accent)}
.finfo{flex:1;min-width:0}
.ftitle{font-size:13px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.fmeta{font-size:11px;color:var(--muted);margin-top:2px}
.fright{display:flex;gap:6px;align-items:center;flex-shrink:0}
input,select,textarea{background:var(--surf2);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:8px 11px;font-size:13px;font-family:inherit;outline:none;transition:border-color .15s;width:100%}
input:focus,select:focus,textarea:focus{border-color:var(--accent)}
input::placeholder{color:var(--muted)}
label{font-size:12px;color:var(--muted);display:block;margin-bottom:4px}
.row{display:flex;gap:8px;margin-bottom:10px}
.col{flex:1;min-width:0}
.sgrp{margin-bottom:18px}
.sgrp h3{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid var(--border)}
.srow{margin-bottom:10px}
.hint{font-size:11px;color:var(--muted);margin-top:3px}
.acc-row{display:flex;align-items:center;gap:10px;padding:8px 12px;border:1px solid var(--border);border-radius:6px;cursor:pointer;margin-bottom:6px;transition:all .15s;background:var(--surf2)}
.acc-row:hover,.acc-row.sel{border-color:var(--accent);background:rgba(88,166,255,.06)}
.acc-row.sel::after{content:'✓';margin-left:auto;color:var(--accent);font-weight:700}
.progbar{width:100%;height:4px;background:var(--border);border-radius:2px;overflow:hidden;margin-top:8px}
.progfill{height:100%;background:var(--accent);border-radius:2px;transition:width .3s;width:0%}
.progfill.ind{width:40%;animation:ind 1.5s ease-in-out infinite}
@keyframes ind{0%{transform:translateX(-200%)}100%{transform:translateX(350%)}}
.alog{font-size:11px;color:var(--muted);font-family:'Courier New',monospace;background:var(--surf2);border:1px solid var(--border);border-radius:6px;padding:10px;margin-top:8px;min-height:60px;max-height:130px;overflow-y:auto;white-space:pre-wrap;word-break:break-all}
.empty{text-align:center;padding:28px 16px;color:var(--muted)}
.empty .eico{font-size:28px;margin-bottom:6px}
.fb{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.toast{position:fixed;bottom:20px;right:20px;padding:9px 15px;background:var(--surf);border:1px solid var(--border);border-radius:8px;font-size:13px;z-index:999;transition:all .3s;opacity:0;transform:translateY(8px);pointer-events:none}
.toast.show{opacity:1;transform:translateY(0)}
.toast.ok{border-color:var(--green);color:var(--green)}
.toast.err{border-color:var(--red);color:var(--red)}
.spin{animation:spin 1s linear infinite;display:inline-block}
@keyframes spin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}
.text-m{color:var(--muted)}
.text-s{font-size:12px}
.gap2{gap:8px}
@media(max-width:580px){.controls{padding:8px 12px;gap:6px}.btn{padding:6px 10px;font-size:12px}.track-title{font-size:14px}}
</style>
</head>
<body>
<div class="toast" id="toast"></div>
<header>
  <div class="logo">📹 VPlay VC Bot</div>
  <div class="hdr-right">
    <div class="status-badge">
      <div class="dot" id="sdot"></div>
      <span id="stxt">Connecting...</span>
    </div>
    <span class="text-s text-m" id="uptime"></span>
  </div>
</header>
<main>

  <!-- Now Playing -->
  <div class="card">
    <div class="card-hdr">
      <span>🎬 Now Playing</span>
      <span id="np-badge"></span>
    </div>
    <div id="np-body">
      <div class="np-idle"><div class="ico">😴</div><div>Kuch nahi chal raha</div><div class="text-s text-m" style="margin-top:4px">Neeche URL daalo ya storage se play karo</div></div>
    </div>
    <div class="controls">
      <button class="btn suc" id="btn-pause" onclick="ctrl('pause')" disabled>⏸ Pause</button>
      <button class="btn suc" id="btn-resume" onclick="ctrl('resume')" style="display:none">▶️ Resume</button>
      <button class="btn" id="btn-skip" onclick="ctrl('skip')" disabled>⏭ Skip</button>
      <button class="btn dng" id="btn-stop" onclick="ctrl('stop')" disabled>⏹ Stop</button>
      <button class="btn pur" id="btn-rnd" onclick="toggleRnd()">🎲 Random</button>
    </div>
  </div>

  <!-- Tabs -->
  <div class="card">
    <div class="tabs">
      <button class="tab act" onclick="tab('queue',this)">📋 Queue <span class="qbadge" id="qbadge" style="display:none">0</span></button>
      <button class="tab" onclick="tab('files',this)">📁 Files</button>
      <button class="tab" onclick="tab('url',this)">🔗 Play URL / VC Link</button>
      <button class="tab" onclick="tab('settings',this)">⚙️ Settings</button>
    </div>

    <!-- Queue -->
    <div class="panel act" id="panel-queue">
      <div class="fb">
        <span class="text-s text-m" id="qinfo">—</span>
        <button class="btn dng" style="padding:4px 9px;font-size:12px" onclick="clearQ()">🗑 Clear All</button>
      </div>
      <div id="qlist"></div>
    </div>

    <!-- Files -->
    <div class="panel" id="panel-files">
      <div class="fb">
        <span class="text-s text-m" id="finfo">—</span>
        <button class="btn" style="padding:4px 9px;font-size:12px" onclick="refFiles(event)">🔄 Refresh</button>
      </div>
      <div id="flist"></div>
    </div>

    <!-- URL Play -->
    <div class="panel" id="panel-url">
      <div class="srow">
        <label>🔗 Video URL (YouTube, direct .mp4, aur bahut saare sites)</label>
        <input id="url-in" type="text" placeholder="https://youtube.com/watch?v=... ya direct video link" />
      </div>
      <div class="row">
        <div class="col">
          <label>Title (optional, blank = auto)</label>
          <input id="url-title" type="text" placeholder="Custom title" />
        </div>
        <div style="width:130px">
          <label>Account</label>
          <select id="url-acc"><option value="0">Account #1</option></select>
        </div>
      </div>
      <button class="btn pri" id="btn-url" onclick="playUrl()" style="width:100%;margin-bottom:8px">▶️ Download & Play in VC</button>
      <div id="url-prog" style="display:none">
        <div class="progbar"><div class="progfill ind" id="url-pbar"></div></div>
        <div class="alog" id="url-log">Starting...</div>
      </div>
    </div>

    <!-- Settings -->
    <div class="panel" id="panel-settings">
      <div class="sgrp">
        <h3>Bot Info</h3>
        <div class="text-s text-m" id="sbot">Loading...</div>
      </div>
      <div class="sgrp">
        <h3>Storage Channel</h3>
        <div class="srow">
          <label>Storage Channel ID</label>
          <input id="s-sch" placeholder="-100xxxxxxxxxx (0 = disabled)" />
          <div class="hint">Private channel mein bot ko admin banao (Post + Pin permission)</div>
        </div>
      </div>
      <div class="sgrp">
        <h3>Group Chat IDs (per account)</h3>
        <div class="srow"><label>Account #1 Group</label><input id="s-g1" placeholder="-100xxxxxxxxxx" /></div>
        <div class="srow"><label>Account #2 Group</label><input id="s-g2" placeholder="-100xxxxxxxxxx (optional)" /></div>
        <div class="srow"><label>Account #3 Group</label><input id="s-g3" placeholder="-100xxxxxxxxxx (optional)" /></div>
      </div>
      <div class="sgrp">
        <h3>Active Account</h3>
        <div id="saccs">Loading...</div>
      </div>
      <div class="sgrp">
        <h3>➕ Account Add Karo</h3>
        <div class="srow">
          <label>Pyrogram String Session</label>
          <textarea id="acc-sess" rows="3" placeholder="BQAxxxxxx... (generate karo: t.me/StringSessionBot ya pyrogramsession.com)"
            style="width:100%;background:#1e1e2e;color:#cdd6f4;border:1px solid #313244;border-radius:6px;padding:8px;font-size:12px;font-family:monospace;resize:vertical"></textarea>
          <div class="hint">Pyrogram v2 string session paste karo. <a href="https://t.me/StringSessionBot" target="_blank" style="color:#89b4fa">@StringSessionBot</a> se generate kar sakte ho.</div>
        </div>
        <div class="srow">
          <label>Group Chat ID (optional)</label>
          <input id="acc-gid" placeholder="-100xxxxxxxxxx (khali choddo = default group)" />
          <div class="hint">Agar ye account alag group mein VC join kare toh ID daalo, warna khali choddo.</div>
        </div>
        <button class="btn pri" onclick="addAccount()" style="width:100%" id="acc-add-btn">➕ Account Add Karo</button>
        <div class="text-s text-m" id="acc-add-status" style="margin-top:6px"></div>
      </div>
      <button class="btn pri" onclick="saveSettings()" style="width:100%">💾 Save Settings (Session Only)</button>
      <div class="text-s text-m" style="margin-top:8px">⚠️ Permanent changes ke liye env vars update karo aur bot restart karo.</div>
    </div>
  </div>
</main>

<script>
const B='/bot/api';
let ST=null,_urlPoll=null,_curTab='queue';

function toast(m,t='ok'){
  const e=document.getElementById('toast');
  e.textContent=m;e.className='toast show '+t;
  clearTimeout(e._t);e._t=setTimeout(()=>e.className='toast',2500);
}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function fdur(s){s=Math.round(s||0);const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sc=s%60;return h?`${h}h${m}m${sc}s`:(m?`${m}m${sc}s`:`${sc}s`)}
function fsz(b){b=parseInt(b||0);if(b>=(1<<30))return(b/(1<<30)).toFixed(1)+' GB';if(b>=(1<<20))return(b/(1<<20)).toFixed(1)+' MB';return(b/(1<<10)).toFixed(1)+' KB'}
function fup(s){const h=Math.floor(s/3600),m=Math.floor((s%3600)/60);return h?`${h}h ${m}m`:`${m}m`}

async function ref(){
  try{
    const r=await fetch(`${B}/status`);
    if(!r.ok)throw 0;
    const d=await r.json();ST=d;render(d);
  }catch{
    document.getElementById('sdot').className='dot off';
    document.getElementById('stxt').textContent='Offline';
  }
}

function render(d){
  document.getElementById('sdot').className='dot';
  document.getElementById('stxt').textContent=`@${d.bot_username||'VPlayBot'} · ${d.accounts} acc`;
  document.getElementById('uptime').textContent=`⏱ ${fup(d.uptime)}`;

  const np=d.now_playing,rnd=d.random_mode;
  const badge=document.getElementById('np-badge');
  badge.innerHTML=(np?(np.paused?'<span class="badge pau">⏸ Paused</span>':'<span class="badge play">▶️ Playing</span>'):'')
    +(rnd?' <span class="badge rnd">🎲 Random</span>':'');

  document.getElementById('np-body').innerHTML=np?`
    <div class="np-active">
      <div class="track-title">🎬 ${esc(np.title)}</div>
      <div class="track-meta">
        <span>📐 ${esc(np.res||'?')}</span><span>⏱ ${fdur(np.duration)}</span>
        <span>📦 ${fsz(np.size)}</span><span>👤 Acc#${np.account+1}</span>
        ${d.queue_count?`<span>📋 ${d.queue_count} queued</span>`:''}
      </div>
    </div>`:`<div class="np-idle"><div class="ico">${rnd?'🎲':'😴'}</div>
    <div>${rnd?'Random mode — next pick soon...':'Kuch nahi chal raha'}</div>
    <div class="text-s text-m" style="margin-top:4px">Neeche URL daalo ya storage se play karo</div></div>`;

  const pl=!!np,pau=np?.paused;
  const bp=document.getElementById('btn-pause'),br=document.getElementById('btn-resume');
  bp.disabled=!pl||pau;bp.style.display=(!pl||pau)?'none':'';
  br.disabled=!pau;br.style.display=pau?'':'none';
  document.getElementById('btn-skip').disabled=!pl;
  document.getElementById('btn-stop').disabled=!pl;
  const rb=document.getElementById('btn-rnd');
  rb.className='btn pur'+(rnd?' on':'');rb.textContent=rnd?'🎲 Random ON':'🎲 Random';

  const qc=d.queue_count||0,qb=document.getElementById('qbadge');
  qb.style.display=qc?'':'none';qb.textContent=qc;

  if(_curTab==='queue')renderQ(d.queue||[]);
  renderAccs(d);

  if(d.url_task&&_urlPoll){
    document.getElementById('url-log').textContent=d.url_task.log||'';
    document.getElementById('url-log').scrollTop=9999;
    if(d.url_task.done){clearInterval(_urlPoll);_urlPoll=null;document.getElementById('url-prog').style.display='none';}
  }
}

function renderQ(q){
  document.getElementById('qinfo').textContent=q.length?`${q.length} video(s) queued`:'Queue khali hai';
  document.getElementById('qlist').innerHTML=q.length?q.map((t,i)=>`
    <div class="qi">
      <div class="qn">${i+1}</div>
      <div class="qinfo"><div class="qt">${esc(t.title)}</div>
      <div class="qm">📐 ${t.res||'?'} · ⏱ ${fdur(t.duration)} · 📦 ${fsz(t.size)}</div></div>
      <button class="btn dng" style="padding:4px 7px;font-size:11px" onclick="rmQ(${i})">🗑</button>
    </div>`).join(''):'<div class="empty"><div class="eico">📋</div><div>Queue khali hai</div></div>';
}

function renderAccs(d){
  if(!d.accounts_info)return;
  document.getElementById('saccs').innerHTML=d.accounts_info.map((a,i)=>`
    <div class="acc-row ${a.is_current?'sel':''}" style="display:flex;align-items:center;gap:8px;cursor:default">
      <span style="flex:0 0 auto;cursor:pointer" onclick="swAcc(${i})">👤 Acc#${i+1}</span>
      <span class="text-m" style="flex:1;cursor:pointer" onclick="swAcc(${i})">@${esc(a.username||String(a.id))}</span>
      <span class="text-s text-m" style="flex:1">→ Group ${a.group}</span>
      <span class="text-s" style="color:${a.is_extra?'#a6e3a1':'#6c7086'}">${a.is_extra?'[+added]':'[env]'}</span>
      ${a.is_extra?`<button class="btn dng" style="padding:3px 8px;font-size:11px" onclick="removeAccount(${i})">🗑</button>`:''}
    </div>`).join('');
  document.getElementById('sbot').textContent=
    `Bot: @${d.bot_username||'?'} | Admin: ${d.admin_id} | Accs: ${d.accounts} | Files: ${d.storage_files_count} | Storage: ${d.storage_ch?'✅ '+d.storage_ch:'❌ off'}`;
  const sel=document.getElementById('url-acc');
  if(sel.options.length!==d.accounts){
    sel.innerHTML=Array.from({length:d.accounts},(_,i)=>`<option value="${i}">Account #${i+1}</option>`).join('');
  }
}

async function ctrl(a){
  const r=await fetch(`${B}/control`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:a})});
  const d=await r.json();toast(d.msg,d.ok?'ok':'err');ref();
}
async function toggleRnd(){
  const r=await fetch(`${B}/random`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({enable:!ST?.random_mode})});
  const d=await r.json();toast(d.msg,d.ok?'ok':'err');ref();
}
async function rmQ(i){
  const r=await fetch(`${B}/queue/${i}`,{method:'DELETE'});
  const d=await r.json();toast(d.msg,d.ok?'ok':'err');ref();
}
async function clearQ(){
  const r=await fetch(`${B}/queue/clear`,{method:'POST'});
  const d=await r.json();toast(d.msg,d.ok?'ok':'err');ref();
}
async function swAcc(i){
  const r=await fetch(`${B}/account`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({acc_idx:i})});
  const d=await r.json();toast(d.msg,d.ok?'ok':'err');ref();
}
async function addAccount(){
  const sess=(document.getElementById('acc-sess').value||'').trim();
  const gid=(document.getElementById('acc-gid').value||'').trim();
  if(!sess){toast('❌ Session string paste karo','err');return;}
  const btn=document.getElementById('acc-add-btn');
  const st=document.getElementById('acc-add-status');
  btn.disabled=true;btn.textContent='⏳ Connecting...';
  st.textContent='Userbot se connect ho raha hai...';
  try{
    const r=await fetch(`${B}/account/add`,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({session_string:sess,group_id:gid?parseInt(gid):0})});
    const d=await r.json();
    toast(d.msg,d.ok?'ok':'err');
    st.textContent=d.msg;
    if(d.ok){document.getElementById('acc-sess').value='';document.getElementById('acc-gid').value='';}
    ref();
  }catch(e){toast('❌ Network error','err');st.textContent='Network error: '+e;}
  finally{btn.disabled=false;btn.textContent='➕ Account Add Karo';}
}
async function removeAccount(i){
  if(!confirm(`Account #${i+1} remove karein? Ye sirf runtime se hatega.`))return;
  const r=await fetch(`${B}/account/remove`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({acc_idx:i})});
  const d=await r.json();toast(d.msg,d.ok?'ok':'err');ref();
}

async function loadFiles(){
  document.getElementById('finfo').textContent='Loading...';
  const r=await fetch(`${B}/files`);const d=await r.json();renderFiles(d.files||[]);
}
function renderFiles(files){
  const accs=ST?.accounts||1;
  const ao=Array.from({length:accs},(_,i)=>`<option value="${i}">Acc#${i+1}</option>`).join('');
  document.getElementById('finfo').textContent=files.length?`${files.length} file(s)`:'Koi file nahi';
  document.getElementById('flist').innerHTML=files.length?[...files].reverse().slice(0,30).map(f=>`
    <div class="fi">
      <div style="font-size:20px">🎬</div>
      <div class="finfo"><div class="ftitle">${esc(f.title)}</div>
      <div class="fmeta">${f.res?f.res+' · ':''}${f.dur?'⏱ '+f.dur+' · ':''}msg#${f.msg_id}</div></div>
      <div class="fright">
        <select id="fa-${f.msg_id}" style="width:75px;padding:3px 6px;font-size:11px">${ao}</select>
        <button class="btn pri" style="padding:4px 9px;font-size:11px;white-space:nowrap" onclick="playFile(${f.msg_id})">▶ Play</button>
      </div>
    </div>`).join(''):'<div class="empty"><div class="eico">📁</div><div>Koi file nahi mili</div></div>';
}
async function refFiles(e){
  const btn=e.target;btn.disabled=true;btn.innerHTML='<span class="spin">⟳</span>';
  const r=await fetch(`${B}/files/refresh`,{method:'POST'});const d=await r.json();
  toast(d.msg,d.ok?'ok':'err');await loadFiles();btn.disabled=false;btn.innerHTML='🔄 Refresh';
}
async function playFile(mid){
  const sel=document.getElementById('fa-'+mid);
  const acc=sel?parseInt(sel.value):0;
  const r=await fetch(`${B}/play/file`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({msg_id:mid,acc_idx:acc})});
  const d=await r.json();toast(d.msg,d.ok?'ok':'err');if(d.ok)tab('queue',document.querySelector('.tab'));ref();
}
async function playUrl(){
  const url=document.getElementById('url-in').value.trim();
  const title=document.getElementById('url-title').value.trim();
  const acc=parseInt(document.getElementById('url-acc').value);
  if(!url){toast('URL daalo pehle!','err');return;}
  const btn=document.getElementById('btn-url');btn.disabled=true;btn.textContent='⏳ Processing...';
  document.getElementById('url-prog').style.display='';
  document.getElementById('url-log').textContent='Shuru ho raha hai...';
  const r=await fetch(`${B}/play/url`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url,title:title||undefined,acc_idx:acc})});
  const d=await r.json();
  if(d.ok){toast('✅ Download shuru!','ok');document.getElementById('url-in').value='';
    _urlPoll=setInterval(ref,1500);}
  else{toast('❌ '+d.msg,'err');document.getElementById('url-prog').style.display='none';}
  btn.disabled=false;btn.textContent='▶️ Download & Play in VC';
}
async function loadSettings(){
  const r=await fetch(`${B}/settings`);const d=await r.json();
  document.getElementById('s-sch').value=d.storage_ch||'';
  document.getElementById('s-g1').value=d.group_ids?.[0]||'';
  document.getElementById('s-g2').value=d.group_ids?.[1]||'';
  document.getElementById('s-g3').value=d.group_ids?.[2]||'';
}
async function saveSettings(){
  const body={storage_ch:document.getElementById('s-sch').value.trim(),
    group_ids:[document.getElementById('s-g1').value.trim(),document.getElementById('s-g2').value.trim(),document.getElementById('s-g3').value.trim()]};
  const r=await fetch(`${B}/settings`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const d=await r.json();toast(d.msg,d.ok?'ok':'err');
}
function tab(name,btn){
  _curTab=name;
  document.querySelectorAll('.tab').forEach(b=>b.classList.remove('act'));
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('act'));
  document.getElementById('panel-'+name).classList.add('act');
  if(btn)btn.classList.add('act');
  else{const names=['queue','files','url','settings'];const i=names.indexOf(name);document.querySelectorAll('.tab')[i]?.classList.add('act');}
  if(name==='files')loadFiles();
  if(name==='settings')loadSettings();
}
ref();setInterval(ref,2000);
</script>
</body>
</html>"""


# ── Web API handlers ──────────────────────────────────────────────
def _j(data: dict, status: int = 200) -> web.Response:
    return web.Response(
        text=json.dumps(data, ensure_ascii=False),
        content_type="application/json",
        status=status,
        headers={"Access-Control-Allow-Origin": "*"}
    )

async def web_dashboard(req: web.Request) -> web.Response:
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")

async def web_status(req: web.Request) -> web.Response:
    state = gs()
    np    = state.now_playing
    np_data = None
    if np:
        np_data = {
            "title": np.title, "duration": np.duration,
            "width": np.width, "height": np.height, "size": np.size,
            "res": fres(np.width, np.height), "paused": state.paused,
            "account": state.account_idx
        }
    queue_list = [{"title": t.title, "duration": t.duration, "width": t.width,
                   "height": t.height, "size": t.size, "res": fres(t.width, t.height)}
                  for t in list(state.queue)]
    acc_info = [{"username": getattr(a["me"], "username", None),
                 "id": a["me"].id, "group": gid_for_account(i),
                 "is_current": state.account_idx == i,
                 "is_extra": a.get("is_extra", False)}
                for i, a in enumerate(accounts)]
    return _j({
        "ok": True,
        "bot_username": _bot_username,
        "admin_id": ADMIN_USER_ID,
        "accounts": len(accounts),
        "accounts_info": acc_info,
        "storage_ch": STORAGE_CH,
        "storage_files_count": len(storage_files),
        "now_playing": np_data,
        "paused": state.paused,
        "queue": queue_list,
        "queue_count": len(state.queue),
        "random_mode": random_mode.get(state.account_idx, False),
        "uptime": int(time.time() - _start_time),
        "url_task": _url_task_status,
    })

async def web_control(req: web.Request) -> web.Response:
    data  = await req.json()
    action = data.get("action", "")
    state = gs()
    acct  = accounts[state.account_idx] if accounts else None
    gid   = GROUP_CHAT_ID

    if action == "pause":
        if acct and state.now_playing and not state.paused:
            try:
                await acct["call"].pause(gid)
                state.paused = True
                return _j({"ok": True, "msg": "⏸ Paused"})
            except Exception as e:
                return _j({"ok": False, "msg": str(e)})
        return _j({"ok": False, "msg": "Kuch play nahi ho raha"})

    elif action == "resume":
        if acct and state.now_playing and state.paused:
            try:
                await acct["call"].resume(gid)
                state.paused = False
                return _j({"ok": True, "msg": "▶️ Resumed"})
            except Exception as e:
                return _j({"ok": False, "msg": str(e)})
        return _j({"ok": False, "msg": "Kuch play nahi ho raha"})

    elif action == "skip":
        if acct and state.now_playing:
            try: await acct["call"].leave_call(gid)
            except: pass
            asyncio.create_task(play_next(_http_session, gid))
            return _j({"ok": True, "msg": "⏭ Skipping..."})
        return _j({"ok": False, "msg": "Kuch nahi chal raha"})

    elif action == "stop":
        if acct:
            try: await acct["call"].leave_call(gid)
            except: pass
        if state.now_playing and state.now_playing.channel_msg_id:
            playing_msg_ids.discard(state.now_playing.channel_msg_id)
        random_mode[state.account_idx] = False
        state.queue.clear(); state.now_playing = None
        return _j({"ok": True, "msg": "⏹ Stopped"})

    return _j({"ok": False, "msg": "Unknown action"})

async def web_random(req: web.Request) -> web.Response:
    data    = await req.json()
    enable  = data.get("enable", False)
    state   = gs()
    acc_idx = state.account_idx
    gid     = GROUP_CHAT_ID
    random_mode[acc_idx] = enable
    if enable:
        if not storage_files:
            random_mode[acc_idx] = False
            return _j({"ok": False, "msg": "⚠️ Storage mein koi file nahi!"})
        if not state.now_playing:
            asyncio.create_task(play_random_track(_http_session, gid))
        return _j({"ok": True, "msg": "🎲 Random Mode ON!"})
    return _j({"ok": True, "msg": "🎲 Random Mode OFF"})

async def web_queue_remove(req: web.Request) -> web.Response:
    idx = int(req.match_info.get("idx", -1))
    state = gs()
    q = list(state.queue)
    if 0 <= idx < len(q):
        removed = q.pop(idx)
        state.queue = deque(q)
        return _j({"ok": True, "msg": f"🗑 Removed: {removed.title[:30]}"})
    return _j({"ok": False, "msg": "Invalid index"})

async def web_queue_clear(req: web.Request) -> web.Response:
    gs().queue.clear()
    return _j({"ok": True, "msg": "🗑 Queue cleared"})

async def web_files(req: web.Request) -> web.Response:
    return _j({"ok": True, "files": storage_files})

async def web_files_refresh(req: web.Request) -> web.Response:
    n = await refresh_storage_files()
    if n >= 0:
        return _j({"ok": True, "msg": f"✅ {n} files refreshed", "count": n})
    return _j({"ok": False, "msg": "❌ Refresh fail — userbot storage channel mein hai?"})

async def web_play_url(req: web.Request) -> web.Response:
    data    = await req.json()
    url     = (data.get("url") or "").strip()
    title   = (data.get("title") or "").strip()
    acc_idx = int(data.get("acc_idx", 0))
    if not url:
        return _j({"ok": False, "msg": "URL required"})
    if acc_idx < 0 or acc_idx >= max(len(accounts), 1):
        acc_idx = 0
    gid = gid_for_account(acc_idx) if accounts else GROUP_CHAT_ID
    asyncio.create_task(
        _url_download_and_play(_http_session, None, None, url, title, acc_idx, gid))
    return _j({"ok": True, "msg": "✅ Download shuru ho gaya! Status dekho..."})

async def web_play_file(req: web.Request) -> web.Response:
    global STORAGE_CH
    data    = await req.json()
    msg_id  = int(data.get("msg_id", 0))
    acc_idx = int(data.get("acc_idx", 0))
    if not STORAGE_CH:
        return _j({"ok": False, "msg": "⚠️ Storage channel set nahi hai"})
    if not accounts:
        return _j({"ok": False, "msg": "⚠️ Koi account connected nahi"})
    if acc_idx < 0 or acc_idx >= len(accounts):
        acc_idx = 0
    gid   = gid_for_account(acc_idx)
    state = gs(gid)
    state.account_idx = acc_idx
    if msg_id in playing_msg_ids:
        return _j({"ok": False, "msg": "⚠️ Ye video already chal raha hai dusre account pe!"})
    bot_obj = accounts[0]["bot"]
    asyncio.create_task(_replay_and_play(bot_obj, msg_id, acc_idx, gid))
    return _j({"ok": True, "msg": f"⬇️ Downloading msg#{msg_id}..."})

async def _replay_and_play(bot_obj, msg_id: int, acc_idx: int, gid: int):
    state = gs(gid)
    try:
        ch_msg = await bot_obj.get_messages(STORAGE_CH, msg_id)
        if not (ch_msg.document or ch_msg.video):
            return
        cap   = ch_msg.caption or ""
        title = cap.split("\n")[0].replace("🎬 ", "").replace("**", "").strip() or f"file_{msg_id}"
        sp    = f"downloads/web_replay_{msg_id}.mp4"
        ok    = await dl_from_storage(bot_obj, msg_id, sp)
        if not ok or not os.path.exists(sp):
            print(f"[WEB REPLAY] Download fail or file missing: {sp}")
            return
        p = probe(sp)
        track = Track(path=sp, title=title, duration=p["dur"],
                      width=p["w"], height=p["h"],
                      size=os.path.getsize(sp), channel_msg_id=msg_id)
        if state.now_playing:
            state.queue.append(track)
        else:
            await play_track(_http_session, track, gid)
    except Exception as e:
        print(f"[WEB REPLAY ERR] {e}")

async def web_account(req: web.Request) -> web.Response:
    data    = await req.json()
    acc_idx = int(data.get("acc_idx", 0))
    state   = gs()
    if 0 <= acc_idx < len(accounts):
        state.account_idx = acc_idx
        me = accounts[acc_idx]["me"]
        return _j({"ok": True, "msg": f"✅ Account #{acc_idx+1}: @{me.username or me.id}"})
    return _j({"ok": False, "msg": "Invalid account"})

async def web_settings_get(req: web.Request) -> web.Response:
    return _j({
        "ok": True,
        "storage_ch": STORAGE_CH,
        "group_ids": GROUP_IDS,
        "admin_id": ADMIN_USER_ID,
        "accounts_count": len(accounts),
    })

async def web_settings_post(req: web.Request) -> web.Response:
    global STORAGE_CH, GROUP_IDS, GROUP_CHAT_ID
    data = await req.json()
    msgs = []
    if "storage_ch" in data and data["storage_ch"] is not None:
        try:
            STORAGE_CH = int(str(data["storage_ch"]).replace(" ", "") or "0")
            msgs.append(f"Storage: {STORAGE_CH or 'disabled'}")
        except: pass
    if "group_ids" in data and isinstance(data["group_ids"], list):
        for i, gid_str in enumerate(data["group_ids"][:3]):
            try:
                val = int(str(gid_str).replace(" ", "") or "0")
                if i < len(GROUP_IDS):
                    GROUP_IDS[i] = val
            except: pass
        GROUP_CHAT_ID = GROUP_IDS[0]
        msgs.append(f"Groups: {GROUP_IDS}")
    return _j({"ok": True, "msg": "✅ Saved: " + (", ".join(msgs) or "no changes")})

async def web_account_add(req: web.Request) -> web.Response:
    """Dynamically add a new userbot account at runtime."""
    global _extra_sessions
    data           = await req.json()
    session_string = (data.get("session_string") or "").strip()
    group_id       = int(str(data.get("group_id") or "0").strip() or "0")
    if not session_string:
        return _j({"ok": False, "msg": "❌ session_string required"})
    if len(session_string) < 50:
        return _j({"ok": False, "msg": "❌ Invalid session string (too short)"})
    idx = len(accounts)
    try:
        from pyrogram import Client as _C
        from pytgcalls import PyTgCalls as _P
        ub   = _C(f"vc_userbot_{idx}", api_id=API_ID, api_hash=API_HASH,
                  session_string=session_string)
        call = _P(ub)
        await ub.start()
        me_ub = await ub.get_me()
        await call.start()

        i2   = idx
        gid4 = group_id or GROUP_CHAT_ID
        @call.on_update()
        async def _handler(_, update):
            if isinstance(update, tc.StreamEnded):
                print(f"[VC] Stream ended — account #{i2+1} | group {gid4}")
                await play_next(_http_session, gid4)

        while len(GROUP_IDS) <= idx:
            GROUP_IDS.append(GROUP_CHAT_ID)
        if group_id:
            GROUP_IDS[idx] = group_id

        accounts.append({"bot": accounts[0]["bot"] if accounts else None,
                         "userbot": ub, "call": call, "me": me_ub,
                         "session_string": session_string, "is_extra": True})
        gs(gid4).account_idx = idx

        # Persist to accounts_config.json
        _extra_sessions.append({"session_string": session_string, "group_id": group_id})
        _save_extra_sessions(_extra_sessions)

        uname = me_ub.username or str(me_ub.id)
        print(f"[ACCOUNTS] Added Account #{idx+1}: @{uname} → Group {gid4}")
        return _j({"ok": True,
                   "msg": f"✅ Account #{idx+1} add ho gaya! @{uname} → Group {gid4}",
                   "username": uname, "id": me_ub.id})
    except Exception as e:
        print(f"[ACCOUNTS] Add error: {e}")
        return _j({"ok": False, "msg": f"❌ Error: {e}"})

async def web_account_remove(req: web.Request) -> web.Response:
    """Remove an extra (dynamically added) account."""
    global _extra_sessions
    data    = await req.json()
    acc_idx = int(data.get("acc_idx", -1))
    if acc_idx < 0 or acc_idx >= len(accounts):
        return _j({"ok": False, "msg": "❌ Invalid account index"})
    if not accounts[acc_idx].get("is_extra"):
        return _j({"ok": False, "msg": "❌ Env-var accounts cannot be removed from dashboard. Remove them from Secrets."})
    try:
        a = accounts.pop(acc_idx)
        try: await a["call"].stop()
        except: pass
        try: await a["userbot"].stop()
        except: pass
        # Remove from GROUP_IDS
        if acc_idx < len(GROUP_IDS):
            GROUP_IDS.pop(acc_idx)
        # Remove from persisted extra sessions (by session string match)
        sess_str = a.get("session_string", "")
        _extra_sessions = [e for e in _extra_sessions
                           if e.get("session_string") != sess_str]
        _save_extra_sessions(_extra_sessions)
        return _j({"ok": True, "msg": f"✅ Account #{acc_idx+1} remove ho gaya."})
    except Exception as e:
        return _j({"ok": False, "msg": f"❌ Error: {e}"})

async def web_hosted_file(req: web.Request) -> web.Response:
    """Serve files from the hosted_files directory."""
    path_info = req.match_info.get("path", "")
    # Security: block path traversal
    safe = os.path.normpath(path_info).lstrip("/").lstrip("\\")
    if ".." in safe or safe.startswith("/"):
        raise web.HTTPForbidden()
    full = os.path.join(HOSTED_DIR, safe)
    if not os.path.exists(full):
        raise web.HTTPNotFound(text=f"File not found: {safe}")
    if os.path.isdir(full):
        # Return directory listing as HTML
        entries = sorted(os.listdir(full))
        rows = ""
        for e in entries:
            ep = os.path.join(full, e)
            sz = f"{fsz(os.path.getsize(ep))}" if os.path.isfile(ep) else "dir"
            href = f"/hosted/{safe}/{e}".replace("//", "/")
            rows += f'<tr><td><a href="{href}">{e}</a></td><td style="color:#888">{sz}</td></tr>'
        html = (f"<!DOCTYPE html><html><head><title>/{safe}</title>"
                f"<style>body{{font-family:monospace;background:#0d1117;color:#c9d1d9;padding:2rem}}"
                f"a{{color:#58a6ff}}table{{border-collapse:collapse;width:100%}}"
                f"td{{padding:6px 12px;border-bottom:1px solid #21262d}}</style></head>"
                f"<body><h2>📁 /{safe}</h2><table>{rows}</table>"
                f"<p style='color:#555;font-size:12px'>VPlay Hosted Files</p></body></html>")
        return web.Response(text=html, content_type="text/html")
    # Serve file with correct content type
    import mimetypes
    ctype, _ = mimetypes.guess_type(full)
    ctype = ctype or "application/octet-stream"
    return web.FileResponse(full, headers={"Content-Type": ctype})

async def web_hosted_index(req: web.Request) -> web.Response:
    """Directory index for /hosted root."""
    entries = sorted(os.listdir(HOSTED_DIR)) if os.path.exists(HOSTED_DIR) else []
    rows = ""
    for e in entries:
        ep = os.path.join(HOSTED_DIR, e)
        sz = fsz(os.path.getsize(ep)) if os.path.isfile(ep) else "dir"
        rows += f'<tr><td><a href="/hosted/{e}">{e}</a></td><td style="color:#888">{sz}</td></tr>'
    body = f"<table>{rows}</table>" if rows else '<p style="color:#888">Koi file nahi hai abhi.</p>'
    html = (f"<!DOCTYPE html><html><head><title>Hosted Files</title>"
            f"<style>body{{font-family:monospace;background:#0d1117;color:#c9d1d9;padding:2rem}}"
            f"a{{color:#58a6ff}}table{{border-collapse:collapse;width:100%}}"
            f"td{{padding:6px 12px;border-bottom:1px solid #21262d}}</style></head>"
            f"<body><h2>🗂 Hosted Files</h2>{body}"
            f"<p style='color:#555;font-size:12px'>Upload via Telegram bot: send .py .js or .zip</p>"
            f"</body></html>")
    return web.Response(text=html, content_type="text/html")

async def web_options(req: web.Request) -> web.Response:
    return web.Response(headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    })

@web.middleware
async def cors_middleware(req: web.Request, handler) -> web.Response:
    if req.method == "OPTIONS":
        return web.Response(headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        })
    try:
        resp = await handler(req)
    except web.HTTPException as exc:
        resp = exc
    resp.headers.setdefault("Access-Control-Allow-Origin", "*")
    return resp


# ── Poll loop ─────────────────────────────────────────────────────
async def poll_loop(bot: Client):
    print("[POLL] HTTP polling shuru...")
    offset = None

    async with _http_session.get(f"{API_BASE}/getUpdates",
                                  params={"offset": -1, "limit": 1}) as r:
        d = await r.json()
        if d.get("result"):
            offset = d["result"][-1]["update_id"] + 1
            print(f"[POLL] Fresh start (skip → {offset-1})")

    while True:
        try:
            params = {"timeout": 30, "limit": 10}
            if offset is not None:
                params["offset"] = offset
            async with _http_session.get(f"{API_BASE}/getUpdates",
                                          params=params) as resp:
                data = await resp.json()
            if not data.get("ok"):
                print(f"[POLL WARN] {data}")
                await asyncio.sleep(3)
                continue
            for upd in data.get("result", []):
                if "message" in upd:
                    await handle_msg(_http_session, bot, upd["message"])
                elif "callback_query" in upd:
                    await handle_cb(_http_session, upd["callback_query"])
                offset = upd["update_id"] + 1
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[POLL ERR] {e}")
            await asyncio.sleep(3)


# ── Main ──────────────────────────────────────────────────────────
async def main():
    global _http_session, _bot_username

    # Shared HTTP session
    _http_session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=65))

    # ── Web server ──
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get("/",                    web_dashboard)
    app.router.add_get("/bot/api/status",      web_status)
    app.router.add_post("/bot/api/control",    web_control)
    app.router.add_post("/bot/api/random",     web_random)
    app.router.add_delete("/bot/api/queue/{idx}", web_queue_remove)
    app.router.add_post("/bot/api/queue/clear",   web_queue_clear)
    app.router.add_get("/bot/api/files",          web_files)
    app.router.add_post("/bot/api/files/refresh", web_files_refresh)
    app.router.add_post("/bot/api/play/url",      web_play_url)
    app.router.add_post("/bot/api/play/file",     web_play_file)
    app.router.add_post("/bot/api/account",        web_account)
    app.router.add_post("/bot/api/account/add",   web_account_add)
    app.router.add_post("/bot/api/account/remove",web_account_remove)
    app.router.add_get("/bot/api/settings",       web_settings_get)
    app.router.add_post("/bot/api/settings",      web_settings_post)
    app.router.add_get("/hosted",                 web_hosted_index)
    app.router.add_get("/hosted/",                web_hosted_index)
    app.router.add_get("/hosted/{path:.+}",       web_hosted_file)
    app.router.add_route("OPTIONS", "/{path_info:.*}", web_options)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    print(f"[WEB] Dashboard live → http://0.0.0.0:{PORT}/")

    # ── Telegram bot ──
    print("[BOOT] Telegram bot starting...")
    bot = Client("vc_bot", api_id=API_ID, api_hash=API_HASH,
                 bot_token=BOT_TOKEN, no_updates=True)
    await bot.start()
    me_bot = await bot.get_me()
    _bot_username = me_bot.username or str(me_bot.id)
    print(f"[OK] Bot: @{_bot_username}")

    async def _start_userbot(idx: int, sess: str, group_id: int = 0):
        """Start a single userbot and register its VC handler."""
        ub   = Client(f"vc_userbot_{idx}", api_id=API_ID, api_hash=API_HASH,
                      session_string=sess)
        call = PyTgCalls(ub)

        def make_handler(i2, call_ref):
            gid4 = GROUP_IDS[i2] if i2 < len(GROUP_IDS) else GROUP_CHAT_ID
            @call_ref.on_update()
            async def _handler(_, update):
                if isinstance(update, tc.StreamEnded):
                    print(f"[VC] Stream ended — account #{i2+1} | group {gid4}")
                    await play_next(_http_session, gid4)

        await ub.start()
        me_ub = await ub.get_me()
        await call.start()
        make_handler(idx, call)
        accounts.append({"bot": bot, "userbot": ub, "call": call, "me": me_ub,
                         "session_string": sess, "is_extra": False})
        # Ensure GROUP_IDS has an entry for this index
        while len(GROUP_IDS) <= idx:
            GROUP_IDS.append(GROUP_CHAT_ID)
        if group_id:
            GROUP_IDS[idx] = group_id
        g = gid_for_account(idx)
        gs(g).account_idx = idx
        print(f"[OK] Account #{idx+1}: @{me_ub.username or '?'} ({me_ub.id}) → Group {g}")

    for i, sess in enumerate(SESSIONS):
        await _start_userbot(i, sess)

    # Load extra sessions saved via dashboard
    for extra in _extra_sessions:
        idx  = len(accounts)
        sess = extra.get("session_string", "")
        gid  = int(extra.get("group_id", 0) or 0)
        if not sess:
            continue
        try:
            await _start_userbot(idx, sess, gid)
            accounts[-1]["is_extra"] = True
        except Exception as e:
            print(f"[ACCOUNTS CFG] Failed to start extra account #{idx+1}: {e}")

    print(f"[OK] {len(accounts)} account(s) | Storage: {STORAGE_CH or 'disabled'}")
    await fetch_storage_files_startup(bot)
    print(f"[READY] ✅ Bot + Dashboard ready! http://localhost:{PORT}/")

    try:
        await poll_loop(bot)
    finally:
        await _http_session.close()
        for a in accounts:
            try: await a["call"].stop()
            except: pass
            try: await a["userbot"].stop()
            except: pass
        try: await bot.stop()
        except: pass
        await runner.cleanup()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
