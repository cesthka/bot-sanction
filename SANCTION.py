"""
╔══════════════════════════════════════════════════════════════════════════╗
║                    SANCTION — Bot de modération                          ║
║  warn / mute / vmute / kick / ban / timeout avec casier, auto-escala-    ║
║  tion, notes staff, appels, anti-raid, modstats.                         ║
╚══════════════════════════════════════════════════════════════════════════╝
"""
import discord
from discord.ext import commands, tasks
import os
import sys
import sqlite3
import json
import random
import string
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ========================= CONFIG =========================
BOT_TOKEN = os.environ.get("TOKEN")
if not BOT_TOKEN:
    print("[ERREUR CRITIQUE] La variable d'environnement TOKEN n'est pas définie.")
    sys.exit(1)

PARIS_TZ = ZoneInfo("Europe/Paris")
DEFAULT_BUYER_IDS = [1312375517927706630, 1312375955737542676]
DEFAULT_PREFIX = "-"
DB_PATH = "sanction.db"

# Limites par rang (mute max en secondes selon rang de l'auteur)
MUTE_LIMITS = {
    1: 60 * 60,            # Helper : max 1h
    2: 60 * 60 * 24,       # Modo : max 24h
    3: 60 * 60 * 24 * 28,  # Sys : max 28 jours (limite Discord)
    4: 60 * 60 * 24 * 28,  # Buyer : idem
}

PURGE_LIMITS = {
    1: 20,   # Helper : max 20 messages
    2: 100,  # Modo : max 100
    3: 500,  # Sys : max 500
    4: 500,
}

# Auto-escalation par défaut (seuil_warns → action, durée_secondes)
# Action : "mute", "kick", "ban"
DEFAULT_ESCALATION = [
    {"warns": 3,  "action": "mute", "duration": 3600,      "reason": "Escalation auto (3 warns)"},
    {"warns": 5,  "action": "mute", "duration": 21600,     "reason": "Escalation auto (5 warns)"},
    {"warns": 7,  "action": "mute", "duration": 86400,     "reason": "Escalation auto (7 warns)"},
    {"warns": 10, "action": "kick", "duration": 0,         "reason": "Escalation auto (10 warns)"},
    {"warns": 15, "action": "ban",  "duration": 0,         "reason": "Escalation auto (15 warns)"},
]

# Anti-raid par défaut
DEFAULT_ANTIRAID = {
    "enabled": False,
    "joins_threshold": 5,      # X comptes
    "window_seconds": 10,      # en Y secondes
    "action": "timeout",       # "timeout" ou "kick"
    "timeout_duration": 3600,  # 1h par défaut
}

RAISON_MIN_LENGTH = 5

# Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)
log = logging.getLogger("sanction")

# Caches
_prefix_cache = {"value": None}

# Tracking temporel pour anti-raid (mémoire, reset au restart)
# {guild_id: [timestamps des join récents]}
_recent_joins = {}


# ========================= DATABASE =========================

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    # Config
    c.execute("""CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY, value TEXT
    )""")

    # Rangs (Buyer 4 > Sys 3 > Modo 2 > Helper 1 > Aucun 0)
    c.execute("""CREATE TABLE IF NOT EXISTS ranks (
        user_id TEXT PRIMARY KEY, rank INTEGER NOT NULL
    )""")

    # Ban du bot
    c.execute("""CREATE TABLE IF NOT EXISTS bot_bans (
        user_id TEXT PRIMARY KEY,
        banned_by TEXT,
        banned_at TEXT
    )""")

    # Salon de log
    c.execute("""CREATE TABLE IF NOT EXISTS log_channels (
        guild_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL
    )""")

    # Salons autorisés (Sys+ bypass)
    c.execute("""CREATE TABLE IF NOT EXISTS allowed_channels (
        guild_id TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        added_by TEXT,
        added_at TEXT,
        PRIMARY KEY (guild_id, channel_id)
    )""")

    # Sanctions (table centrale)
    c.execute("""CREATE TABLE IF NOT EXISTS sanctions (
        id TEXT PRIMARY KEY,
        guild_id TEXT NOT NULL,
        target_id TEXT NOT NULL,
        moderator_id TEXT NOT NULL,
        type TEXT NOT NULL,
        reason TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT,
        active INTEGER DEFAULT 1,
        revoked_by TEXT,
        revoked_at TEXT,
        revoke_reason TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sanctions_target ON sanctions(target_id, guild_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sanctions_expires ON sanctions(active, expires_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sanctions_mod ON sanctions(moderator_id, guild_id)")

    # Notes staff privées
    c.execute("""CREATE TABLE IF NOT EXISTS staff_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id TEXT NOT NULL,
        target_id TEXT NOT NULL,
        author_id TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_notes_target ON staff_notes(target_id, guild_id)")

    # Appels de sanction
    c.execute("""CREATE TABLE IF NOT EXISTS appeals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sanction_id TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        motif TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        handled_by TEXT,
        handled_at TEXT,
        decision_reason TEXT,
        created_at TEXT NOT NULL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_appeals_status ON appeals(status, guild_id)")

    # Locked channels (pour savoir lesquels on a lock pour unlock propre)
    c.execute("""CREATE TABLE IF NOT EXISTS locked_channels (
        channel_id TEXT PRIMARY KEY,
        guild_id TEXT NOT NULL,
        locked_by TEXT,
        locked_at TEXT,
        reason TEXT
    )""")

    # Default config
    c.execute("INSERT OR IGNORE INTO config VALUES ('prefix', ?)", (DEFAULT_PREFIX,))
    c.execute("INSERT OR IGNORE INTO config VALUES ('buyer_ids', ?)",
              (json.dumps([str(i) for i in DEFAULT_BUYER_IDS]),))
    c.execute("INSERT OR IGNORE INTO config VALUES ('escalation', ?)",
              (json.dumps(DEFAULT_ESCALATION),))
    c.execute("INSERT OR IGNORE INTO config VALUES ('antiraid', ?)",
              (json.dumps(DEFAULT_ANTIRAID),))

    conn.commit()
    conn.close()


# ---- Config ----

def get_config(key):
    conn = get_db()
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else None


def set_config(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO config VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()
    if key == "prefix":
        _prefix_cache["value"] = str(value)


def get_prefix_cached():
    if _prefix_cache["value"] is None:
        _prefix_cache["value"] = get_config("prefix") or DEFAULT_PREFIX
    return _prefix_cache["value"]


def get_escalation():
    raw = get_config("escalation")
    if not raw:
        return list(DEFAULT_ESCALATION)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return list(DEFAULT_ESCALATION)


def set_escalation(rules):
    set_config("escalation", json.dumps(rules))


def get_antiraid():
    raw = get_config("antiraid")
    if not raw:
        return dict(DEFAULT_ANTIRAID)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return dict(DEFAULT_ANTIRAID)


def set_antiraid(cfg):
    set_config("antiraid", json.dumps(cfg))


# ---- Rangs ----

def get_rank_db(user_id):
    buyer_ids_raw = get_config("buyer_ids")
    if buyer_ids_raw:
        buyer_ids = json.loads(buyer_ids_raw)
        if str(user_id) in buyer_ids:
            return 4
    conn = get_db()
    row = conn.execute("SELECT rank FROM ranks WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return row["rank"] if row else 0


def set_rank_db(user_id, rank):
    conn = get_db()
    if rank == 0:
        conn.execute("DELETE FROM ranks WHERE user_id = ?", (str(user_id),))
    else:
        conn.execute("INSERT OR REPLACE INTO ranks VALUES (?, ?)", (str(user_id), rank))
    conn.commit()
    conn.close()


def get_ranks_by_level(level):
    conn = get_db()
    rows = conn.execute("SELECT user_id FROM ranks WHERE rank = ?", (level,)).fetchall()
    conn.close()
    return [r["user_id"] for r in rows]


def has_min_rank(user_id, minimum):
    return get_rank_db(user_id) >= minimum


def rank_name(level):
    return {4: "Buyer", 3: "Sys", 2: "Modérateur", 1: "Helper", 0: "Aucun"}[level]


# ---- Bot ban ----

def is_bot_banned(user_id):
    conn = get_db()
    row = conn.execute("SELECT 1 FROM bot_bans WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return row is not None


def add_bot_ban(user_id, banned_by):
    conn = get_db()
    now = datetime.now(PARIS_TZ).strftime("%d/%m/%Y %Hh%M")
    conn.execute("INSERT OR REPLACE INTO bot_bans VALUES (?, ?, ?)",
                 (str(user_id), str(banned_by), now))
    conn.commit()
    conn.close()


def remove_bot_ban(user_id):
    conn = get_db()
    conn.execute("DELETE FROM bot_bans WHERE user_id = ?", (str(user_id),))
    conn.commit()
    conn.close()


# ---- Log channels ----

def get_log_channel(guild_id):
    conn = get_db()
    row = conn.execute("SELECT channel_id FROM log_channels WHERE guild_id = ?",
                       (str(guild_id),)).fetchone()
    conn.close()
    return row["channel_id"] if row else None


def set_log_channel(guild_id, channel_id):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO log_channels VALUES (?, ?)",
                 (str(guild_id), str(channel_id)))
    conn.commit()
    conn.close()


# ---- Allowed channels ----

def add_allowed_channel(guild_id, channel_id, added_by):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO allowed_channels (guild_id, channel_id, added_by, added_at) VALUES (?, ?, ?, ?)",
        (str(guild_id), str(channel_id), str(added_by), now)
    )
    conn.commit()
    conn.close()


def remove_allowed_channel(guild_id, channel_id):
    conn = get_db()
    cur = conn.execute(
        "DELETE FROM allowed_channels WHERE guild_id = ? AND channel_id = ?",
        (str(guild_id), str(channel_id))
    )
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted > 0


def get_allowed_channels(guild_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT channel_id FROM allowed_channels WHERE guild_id = ?",
        (str(guild_id),)
    ).fetchall()
    conn.close()
    return [r["channel_id"] for r in rows]


def is_channel_allowed(guild_id, channel_id):
    conn = get_db()
    row = conn.execute(
        "SELECT 1 FROM allowed_channels WHERE guild_id = ? AND channel_id = ? LIMIT 1",
        (str(guild_id), str(channel_id))
    ).fetchone()
    conn.close()
    return row is not None


# ---- Sanctions (CRUD) ----

def generate_sanction_id():
    """Génère un ID court et lisible : 4 caractères alphanumériques majuscules."""
    chars = string.ascii_uppercase + string.digits
    # On évite O/0 et I/1 pour pas confondre
    chars = chars.replace("O", "").replace("0", "").replace("I", "").replace("1", "")
    for _ in range(20):
        sid = "".join(random.choices(chars, k=4))
        # Vérifie l'unicité
        conn = get_db()
        exists = conn.execute("SELECT 1 FROM sanctions WHERE id = ?", (sid,)).fetchone()
        conn.close()
        if not exists:
            return sid
    # Fallback improbable
    return "".join(random.choices(chars, k=6))


def create_sanction(guild_id, target_id, moderator_id, stype, reason, duration_seconds=None):
    """
    Crée une sanction dans la DB. Retourne l'ID de la sanction.
    duration_seconds : None pour permanent, sinon durée en secondes.
    """
    sid = generate_sanction_id()
    now = datetime.now(PARIS_TZ)
    expires_at = None
    if duration_seconds and duration_seconds > 0:
        expires_at = (now + timedelta(seconds=duration_seconds)).isoformat()

    conn = get_db()
    conn.execute("""INSERT INTO sanctions
        (id, guild_id, target_id, moderator_id, type, reason, created_at, expires_at, active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)""",
        (sid, str(guild_id), str(target_id), str(moderator_id), stype, reason,
         now.isoformat(), expires_at))
    conn.commit()
    conn.close()
    return sid


def get_sanction(sanction_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM sanctions WHERE id = ?",
                       (sanction_id.upper(),)).fetchone()
    conn.close()
    return dict(row) if row else None


def revoke_sanction(sanction_id, revoked_by, revoke_reason=None):
    """Marque une sanction comme inactive. Retourne True si OK."""
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    cur = conn.execute("""UPDATE sanctions
        SET active = 0, revoked_by = ?, revoked_at = ?, revoke_reason = ?
        WHERE id = ? AND active = 1""",
        (str(revoked_by), now, revoke_reason, sanction_id.upper()))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def get_user_sanctions(guild_id, target_id, active_only=False):
    conn = get_db()
    if active_only:
        rows = conn.execute("""SELECT * FROM sanctions
            WHERE guild_id = ? AND target_id = ? AND active = 1
            ORDER BY created_at DESC""",
            (str(guild_id), str(target_id))).fetchall()
    else:
        rows = conn.execute("""SELECT * FROM sanctions
            WHERE guild_id = ? AND target_id = ?
            ORDER BY created_at DESC""",
            (str(guild_id), str(target_id))).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def count_active_warns(guild_id, target_id):
    conn = get_db()
    row = conn.execute("""SELECT COUNT(*) as c FROM sanctions
        WHERE guild_id = ? AND target_id = ? AND type = 'warn' AND active = 1""",
        (str(guild_id), str(target_id))).fetchone()
    conn.close()
    return row["c"] if row else 0


def get_expiring_sanctions():
    """Retourne les sanctions actives qui ont expiré (à débloquer auto)."""
    conn = get_db()
    now_iso = datetime.now(PARIS_TZ).isoformat()
    rows = conn.execute("""SELECT * FROM sanctions
        WHERE active = 1 AND expires_at IS NOT NULL AND expires_at <= ?""",
        (now_iso,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def clear_user_warns(guild_id, target_id, cleared_by):
    """Marque tous les warns actifs d'un user comme révoqués."""
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    cur = conn.execute("""UPDATE sanctions
        SET active = 0, revoked_by = ?, revoked_at = ?, revoke_reason = 'Clear warns'
        WHERE guild_id = ? AND target_id = ? AND type = 'warn' AND active = 1""",
        (str(cleared_by), now, str(guild_id), str(target_id)))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected


def reset_user_casier(guild_id, target_id):
    """Supprime complètement toutes les sanctions d'un user (action drastique)."""
    conn = get_db()
    cur = conn.execute("""DELETE FROM sanctions WHERE guild_id = ? AND target_id = ?""",
        (str(guild_id), str(target_id)))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected


def get_mod_stats(guild_id, moderator_id, days=None):
    """Stats d'un modérateur. Si days=None, tout l'historique."""
    conn = get_db()
    if days:
        cutoff = (datetime.now(PARIS_TZ) - timedelta(days=days)).isoformat()
        rows = conn.execute("""SELECT type, COUNT(*) as c FROM sanctions
            WHERE guild_id = ? AND moderator_id = ? AND created_at >= ?
            GROUP BY type""",
            (str(guild_id), str(moderator_id), cutoff)).fetchall()
    else:
        rows = conn.execute("""SELECT type, COUNT(*) as c FROM sanctions
            WHERE guild_id = ? AND moderator_id = ?
            GROUP BY type""",
            (str(guild_id), str(moderator_id))).fetchall()
    conn.close()
    return {r["type"]: r["c"] for r in rows}


# ---- Staff notes ----

def add_staff_note(guild_id, target_id, author_id, content):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    cur = conn.execute("""INSERT INTO staff_notes
        (guild_id, target_id, author_id, content, created_at) VALUES (?, ?, ?, ?, ?)""",
        (str(guild_id), str(target_id), str(author_id), content, now))
    note_id = cur.lastrowid
    conn.commit()
    conn.close()
    return note_id


def get_staff_notes(guild_id, target_id):
    conn = get_db()
    rows = conn.execute("""SELECT * FROM staff_notes
        WHERE guild_id = ? AND target_id = ? ORDER BY created_at DESC""",
        (str(guild_id), str(target_id))).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_staff_note(note_id):
    conn = get_db()
    cur = conn.execute("DELETE FROM staff_notes WHERE id = ?", (note_id,))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


# ---- Appeals ----

def create_appeal(sanction_id, guild_id, user_id, motif):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    cur = conn.execute("""INSERT INTO appeals
        (sanction_id, guild_id, user_id, motif, status, created_at)
        VALUES (?, ?, ?, ?, 'pending', ?)""",
        (sanction_id.upper(), str(guild_id), str(user_id), motif, now))
    appeal_id = cur.lastrowid
    conn.commit()
    conn.close()
    return appeal_id


def get_appeal(appeal_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM appeals WHERE id = ?", (appeal_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_pending_appeals(guild_id):
    conn = get_db()
    rows = conn.execute("""SELECT * FROM appeals
        WHERE guild_id = ? AND status = 'pending' ORDER BY created_at ASC""",
        (str(guild_id),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def handle_appeal(appeal_id, status, handler_id, reason):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    cur = conn.execute("""UPDATE appeals
        SET status = ?, handled_by = ?, handled_at = ?, decision_reason = ?
        WHERE id = ? AND status = 'pending'""",
        (status, str(handler_id), now, reason, appeal_id))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def user_has_pending_appeal(user_id, sanction_id):
    conn = get_db()
    row = conn.execute("""SELECT 1 FROM appeals
        WHERE user_id = ? AND sanction_id = ? AND status = 'pending' LIMIT 1""",
        (str(user_id), sanction_id.upper())).fetchone()
    conn.close()
    return row is not None


# ---- Locked channels ----

def add_locked_channel(channel_id, guild_id, locked_by, reason):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute("""INSERT OR REPLACE INTO locked_channels
        (channel_id, guild_id, locked_by, locked_at, reason) VALUES (?, ?, ?, ?, ?)""",
        (str(channel_id), str(guild_id), str(locked_by), now, reason))
    conn.commit()
    conn.close()


def remove_locked_channel(channel_id):
    conn = get_db()
    conn.execute("DELETE FROM locked_channels WHERE channel_id = ?", (str(channel_id),))
    conn.commit()
    conn.close()


def is_locked(channel_id):
    conn = get_db()
    row = conn.execute("SELECT 1 FROM locked_channels WHERE channel_id = ?",
                       (str(channel_id),)).fetchone()
    conn.close()
    return row is not None


# ========================= HELPERS =========================

def embed_color():
    return 0x2b2d31


def success_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=0x43b581)
    em.set_footer(text="Sanction")
    return em


def error_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=0xf04747)
    em.set_footer(text="Sanction")
    return em


def info_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=embed_color())
    em.set_footer(text="Sanction")
    return em


def get_french_time():
    now = datetime.now(PARIS_TZ)
    JOURS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin",
               "juillet", "août", "septembre", "octobre", "novembre", "décembre"]
    return f"{JOURS_FR[now.weekday()]} {now.day} {MOIS_FR[now.month - 1]} {now.year} — {now.strftime('%Hh%M')}"


def format_datetime(iso_str):
    """Formate une date ISO en format court lisible."""
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%d/%m/%Y %Hh%M")
    except (ValueError, TypeError):
        return iso_str or "?"


def parse_duration(duration_str):
    """
    Parse une durée textuelle en secondes.
    Formats acceptés : 30s, 15m, 2h, 7j, 2d, 30min
    Retourne (seconds, None) si OK, (None, error_msg) sinon.
    "perm" ou "permanent" → (None, None) explicite pour indiquer permanent.
    """
    if not duration_str:
        return None, "Durée manquante."
    s = duration_str.strip().lower()
    if s in ("perm", "permanent", "∞", "inf"):
        return 0, None  # 0 = permanent

    # Extraire nombre + unité
    import re
    match = re.match(r"^(\d+)\s*(s|sec|m|min|mn|h|j|d|jour|jours|day|days|hour|hours|minute|minutes)?$", s)
    if not match:
        return None, "Format invalide. Utilise `30s`, `15m`, `2h`, `7j` ou `perm`."
    value = int(match.group(1))
    unit = match.group(2) or "m"  # défaut : minutes

    multipliers = {
        "s": 1, "sec": 1,
        "m": 60, "min": 60, "mn": 60, "minute": 60, "minutes": 60,
        "h": 3600, "hour": 3600, "hours": 3600,
        "j": 86400, "d": 86400, "jour": 86400, "jours": 86400, "day": 86400, "days": 86400,
    }
    seconds = value * multipliers.get(unit, 60)
    if seconds <= 0:
        return None, "La durée doit être positive."
    return seconds, None


def format_duration(seconds):
    """Formate une durée en secondes en texte lisible."""
    if seconds == 0 or seconds is None:
        return "permanent"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}min"
    if seconds < 86400:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h{f'{m:02d}' if m else ''}"
    d = seconds // 86400
    h = (seconds % 86400) // 3600
    return f"{d}j{h}h" if h else f"{d}j"


def validate_reason(reason):
    """Vérifie qu'une raison est présente et >= RAISON_MIN_LENGTH caractères après strip."""
    if not reason:
        return False, f"Raison obligatoire (minimum {RAISON_MIN_LENGTH} caractères)."
    if len(reason.strip()) < RAISON_MIN_LENGTH:
        return False, f"Raison trop courte (minimum {RAISON_MIN_LENGTH} caractères)."
    return True, reason.strip()


# ========================= RESOLVE USER =========================

async def resolve_user_or_id(ctx, user_input):
    """Résolution qui marche même si la personne n'est plus sur le serveur."""
    if not user_input:
        return None, None
    raw = user_input.strip()
    cleaned = raw.strip("<@!>")
    user_id = None
    try:
        user_id = int(cleaned)
    except ValueError:
        try:
            m = await commands.MemberConverter().convert(ctx, raw)
            return m, m.id
        except commands.CommandError:
            pass
        try:
            u = await commands.UserConverter().convert(ctx, raw)
            return u, u.id
        except commands.CommandError:
            return None, None

    if ctx.guild:
        member = ctx.guild.get_member(user_id)
        if member:
            return member, user_id
    try:
        user = await bot.fetch_user(user_id)
        return user, user_id
    except discord.NotFound:
        return None, user_id
    except discord.HTTPException as e:
        log.warning(f"resolve_user_or_id: fetch_user({user_id}) a échoué : {e}")
        return None, user_id


def format_user_display(display_obj, user_id):
    if display_obj is not None:
        return f"{display_obj.mention} (`{display_obj.id}`)"
    return f"<@{user_id}> (`{user_id}`) *(hors serveur)*"


# ========================= CHECKS HIÉRARCHIQUES =========================

def can_sanction_target(author_id, target_id):
    """
    Un rang ne peut jamais sanctionner un rang égal ou supérieur.
    Retourne (True, None) si OK, (False, error_msg) sinon.
    """
    author_rank = get_rank_db(author_id)
    target_rank = get_rank_db(target_id)
    if target_rank >= author_rank and author_rank < 4:
        return False, (
            f"Tu ne peux pas sanctionner quelqu'un de rang **{rank_name(target_rank)}** "
            f"(ton rang : **{rank_name(author_rank)}**)."
        )
    # Buyer peut tout sauf les autres Buyers
    if author_rank == 4 and target_rank == 4 and str(author_id) != str(target_id):
        # On autorise quand même un buyer à agir sur un autre, c'est exceptionnel
        return True, None
    return True, None


# ========================= BOT SETUP =========================

init_db()
intents = discord.Intents.all()


def get_prefix(bot, message):
    return get_prefix_cached()


bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)


# ========================= GLOBAL CHANNEL CHECK =========================

class ChannelNotAllowedError(commands.CheckFailure):
    pass


@bot.check
async def check_allowed_channel(ctx):
    if has_min_rank(ctx.author.id, 3):  # Sys+ bypass
        return True
    if ctx.guild is None:
        return True
    if is_channel_allowed(ctx.guild.id, ctx.channel.id):
        return True
    raise ChannelNotAllowedError("Salon non autorisé.")


# ========================= BAN CHECK =========================

async def check_bot_ban(ctx):
    """À appeler au début des commandes publiques (pas les admin/config)."""
    if is_bot_banned(ctx.author.id):
        try:
            await ctx.send(embed=error_embed(
                "⛔ Accès refusé",
                "Tu as été banni du bot Sanction."
            ))
        except discord.HTTPException:
            pass
        return True
    return False


# ========================= EVENTS =========================

@bot.event
async def on_ready():
    log.info(f"Sanction connecté : {bot.user} ({bot.user.id})")
    log.info(f"Prefix : {get_prefix_cached()}")
    await bot.change_presence(
        activity=discord.Activity(type=discord.ActivityType.watching, name="les règles")
    )
    # Démarrage des tâches de fond (définies en partie 2 et 3)
    if not expire_sanctions_loop.is_running():
        expire_sanctions_loop.start()


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandInvokeError):
        error = error.original
    if isinstance(error, ChannelNotAllowedError):
        try:
            await ctx.message.add_reaction("❌")
        except discord.HTTPException:
            pass
        return
    if isinstance(error, (commands.MemberNotFound, commands.UserNotFound)):
        await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Impossible de trouver cet utilisateur."))
    elif isinstance(error, commands.ChannelNotFound):
        await ctx.send(embed=error_embed("❌ Salon introuvable", "Impossible de trouver ce salon."))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=error_embed(
            "❌ Argument manquant",
            f"Il te manque l'argument : `{error.param.name}`."
        ))
    elif isinstance(error, commands.BadArgument):
        await ctx.send(embed=error_embed("❌ Argument invalide", str(error)))
    elif isinstance(error, commands.CommandNotFound):
        pass
    else:
        log.error(
            f"Erreur non gérée '{ctx.command}' par {ctx.author} : {error}\n"
            + "".join(traceback.format_exception(type(error), error, error.__traceback__))
        )
        try:
            await ctx.send(embed=error_embed(
                "❌ Erreur interne",
                "Une erreur inattendue. Les logs ont été générés."
            ))
        except discord.HTTPException:
            pass


# ========================= LOG =========================

async def send_log(guild, action, author, target_display=None, target_id=None,
                   desc=None, reason=None, duration=None, sanction_id=None, color=0xe74c3c):
    channel_id = get_log_channel(guild.id)
    if not channel_id:
        return
    channel = guild.get_channel(int(channel_id))
    if not channel:
        return
    em = discord.Embed(title=f"📋 {action}", color=color)
    em.add_field(name="Modérateur", value=f"{author.mention} (`{author.id}`)", inline=True)
    if target_id is not None:
        em.add_field(name="Cible", value=format_user_display(target_display, target_id), inline=True)
    if sanction_id:
        em.add_field(name="ID sanction", value=f"`#{sanction_id}`", inline=True)
    if duration is not None:
        em.add_field(name="Durée", value=format_duration(duration), inline=True)
    if reason:
        em.add_field(name="Raison", value=reason, inline=False)
    if desc:
        em.add_field(name="Détail", value=desc, inline=False)
    em.set_footer(text=get_french_time())
    try:
        await channel.send(embed=em)
    except discord.HTTPException as e:
        log.warning(f"send_log: échec d'envoi : {e}")


# ========================= COMMANDES SYSTÈME (BUYER) =========================

@bot.command(name="prefix")
async def _prefix(ctx, new_prefix: str = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut changer le prefix."))
    if not new_prefix:
        return await ctx.send(embed=info_embed("Prefix actuel", f"`{get_prefix_cached()}`"))
    set_config("prefix", new_prefix)
    await ctx.send(embed=success_embed("✅ Prefix modifié", f"Nouveau prefix : `{new_prefix}`"))


@bot.command(name="setlog")
async def _setlog(ctx, channel: discord.TextChannel = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut définir le salon de logs."))
    if not channel:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un salon."))
    set_log_channel(ctx.guild.id, channel.id)
    await ctx.send(embed=success_embed("✅ Logs configurés", f"Les logs seront envoyés dans {channel.mention}."))


# ========================= RANGS =========================

@bot.command(name="sys")
async def _sys(ctx, *, user_input: str = None):
    if user_input is None:
        if not has_min_rank(ctx.author.id, 4):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut voir la liste sys."))
        ids = get_ranks_by_level(3)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste Sys", "Aucun sys."))
        return await ctx.send(embed=info_embed(f"📋 Liste Sys ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut ajouter des sys."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) == 3:
        return await ctx.send(embed=error_embed("Déjà Sys", f"{format_user_display(display, uid)} est déjà sys."))
    set_rank_db(uid, 3)
    await ctx.send(embed=success_embed("✅ Sys ajouté", f"{format_user_display(display, uid)} est maintenant **sys**."))
    await send_log(ctx.guild, "Sys ajouté", ctx.author, display, uid, color=0x43b581)


@bot.command(name="unsys")
async def _unsys(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut retirer des sys."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) != 3:
        return await ctx.send(embed=error_embed("Pas Sys", f"{format_user_display(display, uid)} n'est pas sys."))
    set_rank_db(uid, 0)
    await ctx.send(embed=success_embed("✅ Sys retiré", f"{format_user_display(display, uid)} n'est plus sys."))
    await send_log(ctx.guild, "Sys retiré", ctx.author, display, uid, color=0xfaa61a)


@bot.command(name="mod")
async def _mod(ctx, *, user_input: str = None):
    if user_input is None:
        if not has_min_rank(ctx.author.id, 3):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
        ids = get_ranks_by_level(2)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste Modérateurs", "Aucun modérateur."))
        return await ctx.send(embed=info_embed(f"📋 Modérateurs ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) >= 3:
        return await ctx.send(embed=error_embed("❌ Erreur", f"{format_user_display(display, uid)} a un rang supérieur."))
    set_rank_db(uid, 2)
    await ctx.send(embed=success_embed("✅ Modérateur ajouté", f"{format_user_display(display, uid)} est maintenant **modérateur**."))
    await send_log(ctx.guild, "Modo ajouté", ctx.author, display, uid, color=0x43b581)


@bot.command(name="unmod")
async def _unmod(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) != 2:
        return await ctx.send(embed=error_embed("Pas Modérateur", f"{format_user_display(display, uid)} n'est pas modérateur."))
    set_rank_db(uid, 0)
    await ctx.send(embed=success_embed("✅ Modérateur retiré", f"{format_user_display(display, uid)} n'est plus modérateur."))
    await send_log(ctx.guild, "Modo retiré", ctx.author, display, uid, color=0xfaa61a)


@bot.command(name="helper")
async def _helper(ctx, *, user_input: str = None):
    if user_input is None:
        if not has_min_rank(ctx.author.id, 3):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
        ids = get_ranks_by_level(1)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste Helpers", "Aucun helper."))
        return await ctx.send(embed=info_embed(f"📋 Helpers ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) >= 2:
        return await ctx.send(embed=error_embed("❌ Erreur", f"{format_user_display(display, uid)} a un rang supérieur."))
    set_rank_db(uid, 1)
    await ctx.send(embed=success_embed("✅ Helper ajouté", f"{format_user_display(display, uid)} est maintenant **helper**."))
    await send_log(ctx.guild, "Helper ajouté", ctx.author, display, uid, color=0x43b581)


@bot.command(name="unhelper")
async def _unhelper(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) != 1:
        return await ctx.send(embed=error_embed("Pas Helper", f"{format_user_display(display, uid)} n'est pas helper."))
    set_rank_db(uid, 0)
    await ctx.send(embed=success_embed("✅ Helper retiré", f"{format_user_display(display, uid)} n'est plus helper."))
    await send_log(ctx.guild, "Helper retiré", ctx.author, display, uid, color=0xfaa61a)


# ========================= BOT BAN =========================

@bot.command(name="botban")
async def _botban(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if is_bot_banned(uid):
        return await ctx.send(embed=error_embed("Déjà banni", f"{format_user_display(display, uid)} est déjà banni du bot."))
    add_bot_ban(uid, ctx.author.id)
    await ctx.send(embed=success_embed("⛔ Banni du bot", f"{format_user_display(display, uid)} ne peut plus utiliser Sanction."))
    await send_log(ctx.guild, "Bot ban", ctx.author, display, uid, color=0xf04747)


@bot.command(name="botunban")
async def _botunban(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if not is_bot_banned(uid):
        return await ctx.send(embed=error_embed("Pas banni", f"{format_user_display(display, uid)} n'est pas banni."))
    remove_bot_ban(uid)
    await ctx.send(embed=success_embed("✅ Débanni", f"{format_user_display(display, uid)} peut à nouveau utiliser Sanction."))
    await send_log(ctx.guild, "Bot unban", ctx.author, display, uid, color=0x43b581)


# ========================= SALONS AUTORISÉS =========================

async def _resolve_channel(ctx, channel_input):
    clean = channel_input.strip("<#>")
    try:
        cid = int(clean)
        ch = ctx.guild.get_channel(cid)
        return ch, cid
    except ValueError:
        pass
    try:
        ch = await commands.TextChannelConverter().convert(ctx, channel_input)
        return ch, ch.id
    except commands.CommandError:
        return None, None


@bot.command(name="allow")
async def _allow(ctx, *, channel_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if channel_input is None:
        allowed = get_allowed_channels(ctx.guild.id)
        if not allowed:
            return await ctx.send(embed=info_embed(
                "📋 Aucun salon autorisé",
                f"Seuls les **Sys+** peuvent utiliser le bot.\n"
                f"Utilise `{get_prefix_cached()}allow #salon` pour en ajouter un."
            ))
        lines = []
        for cid in allowed:
            ch = ctx.guild.get_channel(int(cid))
            lines.append(f"• {ch.mention} (`{cid}`)" if ch else f"• *Salon inaccessible* (`{cid}`)")
        return await ctx.send(embed=info_embed(f"📋 Salons autorisés ({len(allowed)})", "\n".join(lines)))
    channel, raw_id = await _resolve_channel(ctx, channel_input)
    if not channel:
        return await ctx.send(embed=error_embed("❌ Salon introuvable", "Mention `#salon` ou ID."))
    if is_channel_allowed(ctx.guild.id, channel.id):
        return await ctx.send(embed=error_embed("Déjà autorisé", f"{channel.mention} est déjà autorisé."))
    add_allowed_channel(ctx.guild.id, channel.id, ctx.author.id)
    await ctx.send(embed=success_embed("✅ Salon autorisé", f"{channel.mention} est maintenant autorisé."))
    await send_log(ctx.guild, "Salon autorisé", ctx.author,
                   desc=f"Salon : {channel.mention} (`{channel.id}`)", color=0x43b581)


@bot.command(name="unallow")
async def _unallow(ctx, *, channel_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not channel_input:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}unallow #salon` ou ID"
        ))
    channel, raw_id = await _resolve_channel(ctx, channel_input)
    if not channel:
        if raw_id is not None:
            if remove_allowed_channel(ctx.guild.id, raw_id):
                return await ctx.send(embed=success_embed(
                    "✅ Salon retiré", f"Salon `{raw_id}` retiré (salon supprimé/inaccessible)."
                ))
            return await ctx.send(embed=error_embed("Pas dans la liste", f"Salon `{raw_id}` pas autorisé."))
        return await ctx.send(embed=error_embed("❌ Salon introuvable", "Mention ou ID."))
    if not remove_allowed_channel(ctx.guild.id, channel.id):
        return await ctx.send(embed=error_embed("Pas dans la liste", f"{channel.mention} pas autorisé."))
    await ctx.send(embed=success_embed("✅ Salon retiré", f"{channel.mention} n'est plus autorisé."))
    await send_log(ctx.guild, "Salon retiré", ctx.author,
                   desc=f"Salon : {channel.mention} (`{channel.id}`)", color=0xf04747)


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                 PARTIE 2 — SANCTIONS + AUTO-ESCALATION                    ║
# ║  warn, mute, vmute, kick, ban, timeout, unsanction, casier, clearwarns   ║
# ║  + expire_sanctions_loop qui débloque auto à l'expiration                ║
# ╚══════════════════════════════════════════════════════════════════════════╝


# ========================= DM NOTIFICATION =========================

async def notify_target_dm(target, guild, action, reason, duration=None, sanction_id=None,
                           moderator=None):
    """Tente de DM la cible pour l'informer de la sanction. Silencieux si ferme."""
    if target is None:
        return
    try:
        color_map = {
            "warn": 0xfaa61a, "mute": 0xe67e22, "vmute": 0xe67e22,
            "kick": 0xe74c3c, "ban": 0xf04747, "timeout": 0xe67e22,
        }
        em = discord.Embed(
            title=f"⚠️ Tu as reçu un {action}",
            description=f"Sur le serveur **{guild.name}**",
            color=color_map.get(action, 0xe74c3c),
        )
        if moderator:
            em.add_field(name="Par", value=moderator.mention, inline=True)
        if sanction_id:
            em.add_field(name="ID sanction", value=f"`#{sanction_id}`", inline=True)
        if duration is not None:
            em.add_field(name="Durée", value=format_duration(duration), inline=True)
        em.add_field(name="Raison", value=reason, inline=False)
        if sanction_id:
            em.add_field(
                name="Faire appel",
                value=f"Si tu juges ça injuste : `{get_prefix_cached()}appel {sanction_id} <motif>`",
                inline=False,
            )
        em.set_footer(text="Sanction ・ Meira")
        await target.send(embed=em)
    except (discord.Forbidden, discord.HTTPException):
        pass


# ========================= AUTO-ESCALATION =========================

async def check_escalation(ctx, target_display, target_id):
    """
    Appelé après un warn. Compte les warns actifs et déclenche l'action
    correspondante au palier atteint, si un palier est matché.
    """
    warns_count = count_active_warns(ctx.guild.id, target_id)
    escalation = get_escalation()
    # Trouve la règle avec le plus grand seuil <= warns_count
    matched = None
    for rule in sorted(escalation, key=lambda r: r["warns"]):
        if rule["warns"] == warns_count:
            matched = rule
            break
    if not matched:
        return

    action = matched["action"]
    duration = matched.get("duration", 0) or None
    auto_reason = matched.get("reason", f"Escalation auto ({warns_count} warns)")

    # Applique l'action en tant que "bot" (on log ctx.me comme modérateur)
    target_member = ctx.guild.get_member(target_id) if ctx.guild else None

    try:
        if action == "mute":
            if target_member:
                try:
                    until = discord.utils.utcnow() + timedelta(seconds=duration)
                    await target_member.timeout(until, reason=auto_reason)
                except (discord.Forbidden, discord.HTTPException) as e:
                    log.warning(f"Escalation mute échoué : {e}")
            sid = create_sanction(ctx.guild.id, target_id, ctx.me.id, "mute", auto_reason, duration)
            await ctx.send(embed=info_embed(
                "⚠️ Escalation automatique",
                f"{format_user_display(target_display, target_id)} atteint **{warns_count} warns** "
                f"→ **mute {format_duration(duration)}** appliqué.\nID : `#{sid}`"
            ))
            await notify_target_dm(target_member, ctx.guild, "mute", auto_reason,
                                   duration=duration, sanction_id=sid, moderator=ctx.me)
            await send_log(ctx.guild, "Escalation → Mute", ctx.me, target_display, target_id,
                           reason=auto_reason, duration=duration, sanction_id=sid, color=0xe67e22)

        elif action == "kick":
            if target_member:
                try:
                    await target_member.kick(reason=auto_reason)
                except (discord.Forbidden, discord.HTTPException) as e:
                    log.warning(f"Escalation kick échoué : {e}")
            sid = create_sanction(ctx.guild.id, target_id, ctx.me.id, "kick", auto_reason)
            await ctx.send(embed=info_embed(
                "⚠️ Escalation automatique",
                f"{format_user_display(target_display, target_id)} atteint **{warns_count} warns** "
                f"→ **kick** appliqué.\nID : `#{sid}`"
            ))
            await notify_target_dm(target_member, ctx.guild, "kick", auto_reason,
                                   sanction_id=sid, moderator=ctx.me)
            await send_log(ctx.guild, "Escalation → Kick", ctx.me, target_display, target_id,
                           reason=auto_reason, sanction_id=sid, color=0xe74c3c)

        elif action == "ban":
            try:
                await ctx.guild.ban(discord.Object(id=target_id), reason=auto_reason)
            except (discord.Forbidden, discord.HTTPException) as e:
                log.warning(f"Escalation ban échoué : {e}")
            sid = create_sanction(ctx.guild.id, target_id, ctx.me.id, "ban", auto_reason)
            await ctx.send(embed=info_embed(
                "⚠️ Escalation automatique",
                f"{format_user_display(target_display, target_id)} atteint **{warns_count} warns** "
                f"→ **ban permanent** appliqué.\nID : `#{sid}`"
            ))
            await notify_target_dm(target_member, ctx.guild, "ban", auto_reason,
                                   sanction_id=sid, moderator=ctx.me)
            await send_log(ctx.guild, "Escalation → Ban", ctx.me, target_display, target_id,
                           reason=auto_reason, sanction_id=sid, color=0xf04747)
    except Exception as e:
        log.error(f"check_escalation: erreur pour {target_id} : {e}\n{traceback.format_exc()}")


# ========================= WARN =========================

@bot.command(name="warn")
async def _warn(ctx, user_input: str = None, *, reason: str = None):
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Helper+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed(
            "Arguments manquants",
            f"Usage : `{get_prefix_cached()}warn @user <raison>`"
        ))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if uid == ctx.author.id:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te warn toi-même."))

    ok, reason_or_err = validate_reason(reason)
    if not ok:
        return await ctx.send(embed=error_embed("❌ Raison requise", reason_or_err + f"\nUsage : `{get_prefix_cached()}warn @user <raison détaillée>`"))
    reason = reason_or_err

    allowed, err = can_sanction_target(ctx.author.id, uid)
    if not allowed:
        return await ctx.send(embed=error_embed("❌ Permission refusée", err))

    sid = create_sanction(ctx.guild.id, uid, ctx.author.id, "warn", reason)
    warns_count = count_active_warns(ctx.guild.id, uid)

    await ctx.send(embed=success_embed(
        "⚠️ Warn appliqué",
        f"**Cible :** {format_user_display(display, uid)}\n"
        f"**Raison :** {reason}\n"
        f"**ID :** `#{sid}`\n"
        f"**Warns actifs :** {warns_count}"
    ))

    # DM le warn à la cible
    target_member = ctx.guild.get_member(uid)
    await notify_target_dm(target_member or display, ctx.guild, "warn", reason,
                           sanction_id=sid, moderator=ctx.author)

    await send_log(ctx.guild, "Warn", ctx.author, display, uid,
                   reason=reason, sanction_id=sid, color=0xfaa61a)

    # Check auto-escalation
    await check_escalation(ctx, display, uid)


@bot.command(name="unwarn")
async def _unwarn(ctx, sanction_id: str = None, *, reason: str = None):
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis."))
    if not sanction_id:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}unwarn <id>` (ex : `{get_prefix_cached()}unwarn A7F3`)"
        ))
    sid = sanction_id.upper().lstrip("#")
    sanc = get_sanction(sid)
    if not sanc:
        return await ctx.send(embed=error_embed("❌ Sanction introuvable", f"Aucune sanction avec l'ID `#{sid}`."))
    if sanc["type"] != "warn":
        return await ctx.send(embed=error_embed("❌ Mauvais type", f"La sanction `#{sid}` est un **{sanc['type']}**, pas un warn. Utilise `{get_prefix_cached()}unsanction`."))
    if not sanc["active"]:
        return await ctx.send(embed=error_embed("Déjà révoqué", f"Le warn `#{sid}` est déjà inactif."))

    # Un modo ne peut unwarn que ses propres warns (sauf Sys+)
    author_rank = get_rank_db(ctx.author.id)
    if author_rank < 3 and str(sanc["moderator_id"]) != str(ctx.author.id):
        return await ctx.send(embed=error_embed(
            "❌ Permission refusée",
            "Tu ne peux annuler que **tes propres** warns. Un Sys+ peut en annuler n'importe lequel."
        ))

    revoke_sanction(sid, ctx.author.id, reason or "Aucune raison")
    target_id = int(sanc["target_id"])
    display = ctx.guild.get_member(target_id) or None
    if not display:
        try:
            display = await bot.fetch_user(target_id)
        except discord.HTTPException:
            display = None

    await ctx.send(embed=success_embed(
        "✅ Warn annulé",
        f"Warn `#{sid}` retiré de {format_user_display(display, target_id)}."
    ))
    await send_log(ctx.guild, "Warn annulé", ctx.author, display, target_id,
                   sanction_id=sid, reason=reason, color=0x43b581)


@bot.command(name="clearwarns")
async def _clearwarns(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    cleared = clear_user_warns(ctx.guild.id, uid, ctx.author.id)
    if cleared == 0:
        return await ctx.send(embed=info_embed("Aucun warn", f"{format_user_display(display, uid)} n'a pas de warn actif."))

    await ctx.send(embed=success_embed(
        "🧹 Warns effacés",
        f"**{cleared}** warn(s) effacé(s) pour {format_user_display(display, uid)}."
    ))
    await send_log(ctx.guild, "Clear warns", ctx.author, display, uid,
                   desc=f"{cleared} warns effacés", color=0x43b581)


# ========================= MUTE (timeout textuel Discord) =========================

@bot.command(name="mute")
async def _mute(ctx, user_input: str = None, duration: str = None, *, reason: str = None):
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Helper+** requis."))
    if not user_input or not duration:
        return await ctx.send(embed=error_embed(
            "Arguments manquants",
            f"Usage : `{get_prefix_cached()}mute @user <durée> <raison>`\n"
            f"Ex : `{get_prefix_cached()}mute @user 30m spam`"
        ))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if uid == ctx.author.id:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te mute toi-même."))

    seconds, err = parse_duration(duration)
    if err:
        return await ctx.send(embed=error_embed("❌ Durée invalide", err))
    if seconds == 0:
        return await ctx.send(embed=error_embed("❌ Durée invalide", "Un mute doit avoir une durée."))

    # Check limite selon rang
    author_rank = get_rank_db(ctx.author.id)
    max_duration = MUTE_LIMITS.get(author_rank, 0)
    if seconds > max_duration:
        return await ctx.send(embed=error_embed(
            "❌ Durée trop longue",
            f"En tant que **{rank_name(author_rank)}**, tu peux mute jusqu'à **{format_duration(max_duration)}** max."
        ))
    # Limite absolue Discord : 28 jours
    if seconds > 60 * 60 * 24 * 28:
        return await ctx.send(embed=error_embed(
            "❌ Durée trop longue",
            "La limite Discord pour un timeout est de **28 jours**."
        ))

    ok, reason_or_err = validate_reason(reason)
    if not ok:
        return await ctx.send(embed=error_embed("❌ Raison requise", reason_or_err + f"\nUsage : `{get_prefix_cached()}mute @user <durée> <raison détaillée>`"))
    reason = reason_or_err

    allowed, err = can_sanction_target(ctx.author.id, uid)
    if not allowed:
        return await ctx.send(embed=error_embed("❌ Permission refusée", err))

    target_member = ctx.guild.get_member(uid)
    if not target_member:
        return await ctx.send(embed=error_embed(
            "❌ Membre absent",
            f"{format_user_display(display, uid)} n'est pas sur le serveur. On ne peut pas mute quelqu'un qui n'est pas là."
        ))

    try:
        until = discord.utils.utcnow() + timedelta(seconds=seconds)
        await target_member.timeout(until, reason=f"{reason} | par {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed(
            "❌ Permission manquante",
            "Je n'ai pas la permission de mute ce membre (son rôle est au-dessus du mien ?)."
        ))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", f"Impossible de mute : {e}"))

    sid = create_sanction(ctx.guild.id, uid, ctx.author.id, "mute", reason, seconds)

    await ctx.send(embed=success_embed(
        "🔇 Mute appliqué",
        f"**Cible :** {format_user_display(display, uid)}\n"
        f"**Durée :** {format_duration(seconds)}\n"
        f"**Raison :** {reason}\n"
        f"**ID :** `#{sid}`"
    ))
    await notify_target_dm(target_member, ctx.guild, "mute", reason,
                           duration=seconds, sanction_id=sid, moderator=ctx.author)
    await send_log(ctx.guild, "Mute", ctx.author, display, uid,
                   reason=reason, duration=seconds, sanction_id=sid, color=0xe67e22)


@bot.command(name="unmute")
async def _unmute(ctx, *, user_input: str = None):
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    target_member = ctx.guild.get_member(uid)
    if not target_member:
        return await ctx.send(embed=error_embed("❌ Membre absent", "Ce membre n'est pas sur le serveur."))
    if not target_member.is_timed_out():
        return await ctx.send(embed=error_embed("Pas mute", f"{format_user_display(display, uid)} n'est pas actuellement mute."))

    try:
        await target_member.timeout(None, reason=f"Unmute par {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je ne peux pas unmute ce membre."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    # Révoque la sanction mute active en DB si elle existe
    active_mutes = [s for s in get_user_sanctions(ctx.guild.id, uid, active_only=True)
                    if s["type"] == "mute"]
    revoked_sid = None
    if active_mutes:
        # On révoque la plus récente
        sanc = active_mutes[0]
        revoke_sanction(sanc["id"], ctx.author.id, "Unmute manuel")
        revoked_sid = sanc["id"]

    desc = f"{format_user_display(display, uid)} a été unmute."
    if revoked_sid:
        desc += f"\nSanction `#{revoked_sid}` révoquée."
    await ctx.send(embed=success_embed("✅ Unmute", desc))
    await send_log(ctx.guild, "Unmute", ctx.author, display, uid,
                   sanction_id=revoked_sid, color=0x43b581)


# ========================= VMUTE (server_mute vocal) =========================

@bot.command(name="vmute")
async def _vmute(ctx, user_input: str = None, duration: str = None, *, reason: str = None):
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Helper+** requis."))
    if not user_input or not duration:
        return await ctx.send(embed=error_embed(
            "Arguments manquants",
            f"Usage : `{get_prefix_cached()}vmute @user <durée> <raison>`"
        ))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if uid == ctx.author.id:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te vmute toi-même."))

    seconds, err = parse_duration(duration)
    if err:
        return await ctx.send(embed=error_embed("❌ Durée invalide", err))
    if seconds == 0:
        return await ctx.send(embed=error_embed("❌ Durée invalide", "Un vmute doit avoir une durée."))

    author_rank = get_rank_db(ctx.author.id)
    max_duration = MUTE_LIMITS.get(author_rank, 0)
    if seconds > max_duration:
        return await ctx.send(embed=error_embed(
            "❌ Durée trop longue",
            f"En tant que **{rank_name(author_rank)}**, tu peux vmute jusqu'à **{format_duration(max_duration)}** max."
        ))

    ok, reason_or_err = validate_reason(reason)
    if not ok:
        return await ctx.send(embed=error_embed("❌ Raison requise", reason_or_err))
    reason = reason_or_err

    allowed, err = can_sanction_target(ctx.author.id, uid)
    if not allowed:
        return await ctx.send(embed=error_embed("❌ Permission refusée", err))

    target_member = ctx.guild.get_member(uid)
    if not target_member:
        return await ctx.send(embed=error_embed("❌ Membre absent", "Ce membre n'est pas sur le serveur."))

    try:
        await target_member.edit(mute=True, reason=f"{reason} | par {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je ne peux pas vmute ce membre."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    sid = create_sanction(ctx.guild.id, uid, ctx.author.id, "vmute", reason, seconds)

    await ctx.send(embed=success_embed(
        "🎤 VMute appliqué",
        f"**Cible :** {format_user_display(display, uid)}\n"
        f"**Durée :** {format_duration(seconds)}\n"
        f"**Raison :** {reason}\n"
        f"**ID :** `#{sid}`"
    ))
    await notify_target_dm(target_member, ctx.guild, "vmute", reason,
                           duration=seconds, sanction_id=sid, moderator=ctx.author)
    await send_log(ctx.guild, "VMute", ctx.author, display, uid,
                   reason=reason, duration=seconds, sanction_id=sid, color=0xe67e22)


@bot.command(name="unvmute")
async def _unvmute(ctx, *, user_input: str = None):
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    target_member = ctx.guild.get_member(uid)
    if not target_member:
        return await ctx.send(embed=error_embed("❌ Membre absent", "Ce membre n'est pas sur le serveur."))

    try:
        await target_member.edit(mute=False, reason=f"Unvmute par {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je ne peux pas unvmute ce membre."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    active_vmutes = [s for s in get_user_sanctions(ctx.guild.id, uid, active_only=True)
                     if s["type"] == "vmute"]
    revoked_sid = None
    if active_vmutes:
        sanc = active_vmutes[0]
        revoke_sanction(sanc["id"], ctx.author.id, "Unvmute manuel")
        revoked_sid = sanc["id"]

    desc = f"{format_user_display(display, uid)} a été unvmute."
    if revoked_sid:
        desc += f"\nSanction `#{revoked_sid}` révoquée."
    await ctx.send(embed=success_embed("✅ Unvmute", desc))
    await send_log(ctx.guild, "Unvmute", ctx.author, display, uid,
                   sanction_id=revoked_sid, color=0x43b581)


# ========================= KICK =========================

@bot.command(name="kick")
async def _kick(ctx, user_input: str = None, *, reason: str = None):
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed(
            "Arguments manquants",
            f"Usage : `{get_prefix_cached()}kick @user <raison>`"
        ))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if uid == ctx.author.id:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te kick toi-même."))

    ok, reason_or_err = validate_reason(reason)
    if not ok:
        return await ctx.send(embed=error_embed("❌ Raison requise", reason_or_err))
    reason = reason_or_err

    allowed, err = can_sanction_target(ctx.author.id, uid)
    if not allowed:
        return await ctx.send(embed=error_embed("❌ Permission refusée", err))

    target_member = ctx.guild.get_member(uid)
    if not target_member:
        return await ctx.send(embed=error_embed(
            "❌ Membre absent",
            "Ce membre n'est pas sur le serveur. Il n'y a rien à kick."
        ))

    # DM avant kick (sinon la personne peut plus recevoir)
    await notify_target_dm(target_member, ctx.guild, "kick", reason,
                           moderator=ctx.author)

    try:
        await target_member.kick(reason=f"{reason} | par {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je ne peux pas kick ce membre."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    sid = create_sanction(ctx.guild.id, uid, ctx.author.id, "kick", reason)

    await ctx.send(embed=success_embed(
        "👢 Kick appliqué",
        f"**Cible :** {format_user_display(display, uid)}\n"
        f"**Raison :** {reason}\n"
        f"**ID :** `#{sid}`"
    ))
    await send_log(ctx.guild, "Kick", ctx.author, display, uid,
                   reason=reason, sanction_id=sid, color=0xe74c3c)


# ========================= BAN =========================

@bot.command(name="ban")
async def _ban(ctx, user_input: str = None, duration: str = None, *, reason: str = None):
    """
    Usage :
      -ban @user perm <raison>     → ban permanent
      -ban @user 7j <raison>       → ban temporaire 7 jours
    """
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis pour ban."))
    if not user_input:
        return await ctx.send(embed=error_embed(
            "Arguments manquants",
            f"Usage :\n"
            f"`{get_prefix_cached()}ban @user perm <raison>` → permanent\n"
            f"`{get_prefix_cached()}ban @user <durée> <raison>` → temporaire (ex : 7j)"
        ))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if uid == ctx.author.id:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te ban toi-même."))

    # Parse durée (si absente → permanent avec warning pour forcer explicite)
    if not duration:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Précise la durée : `perm` pour permanent, ou `7j`, `24h`, `30m` etc.\n"
            f"Usage : `{get_prefix_cached()}ban @user <durée> <raison>`"
        ))

    seconds, err = parse_duration(duration)
    if err:
        return await ctx.send(embed=error_embed("❌ Durée invalide", err))
    # 0 = permanent (retourné par parse_duration pour "perm")

    ok, reason_or_err = validate_reason(reason)
    if not ok:
        return await ctx.send(embed=error_embed("❌ Raison requise", reason_or_err))
    reason = reason_or_err

    allowed, err = can_sanction_target(ctx.author.id, uid)
    if not allowed:
        return await ctx.send(embed=error_embed("❌ Permission refusée", err))

    # DM avant ban (si membre présent)
    target_member = ctx.guild.get_member(uid)
    if target_member:
        await notify_target_dm(target_member, ctx.guild, "ban", reason,
                               duration=seconds if seconds else None,
                               moderator=ctx.author)

    try:
        await ctx.guild.ban(discord.Object(id=uid), reason=f"{reason} | par {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je ne peux pas ban ce membre."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    sid = create_sanction(ctx.guild.id, uid, ctx.author.id, "ban", reason,
                          duration_seconds=seconds if seconds > 0 else None)

    duration_display = format_duration(seconds) if seconds > 0 else "permanent"
    await ctx.send(embed=success_embed(
        "⛔ Ban appliqué",
        f"**Cible :** {format_user_display(display, uid)}\n"
        f"**Durée :** {duration_display}\n"
        f"**Raison :** {reason}\n"
        f"**ID :** `#{sid}`"
    ))
    await send_log(ctx.guild, "Ban", ctx.author, display, uid,
                   reason=reason, duration=seconds if seconds > 0 else None,
                   sanction_id=sid, color=0xf04747)


@bot.command(name="unban")
async def _unban(ctx, user_input: str = None, *, reason: str = None):
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis pour unban."))
    if not user_input:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}unban <id ou mention>`"
        ))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    try:
        await ctx.guild.unban(discord.Object(id=uid),
                              reason=f"Unban par {ctx.author}" + (f" | {reason}" if reason else ""))
    except discord.NotFound:
        return await ctx.send(embed=error_embed(
            "❌ Pas banni",
            f"{format_user_display(display, uid)} n'est pas banni sur ce serveur."
        ))
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je ne peux pas unban."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    # Révoque le ban actif en DB
    active_bans = [s for s in get_user_sanctions(ctx.guild.id, uid, active_only=True)
                   if s["type"] == "ban"]
    revoked_sid = None
    if active_bans:
        sanc = active_bans[0]
        revoke_sanction(sanc["id"], ctx.author.id, reason or "Unban manuel")
        revoked_sid = sanc["id"]

    desc = f"{format_user_display(display, uid)} a été unban."
    if revoked_sid:
        desc += f"\nSanction `#{revoked_sid}` révoquée."
    await ctx.send(embed=success_embed("✅ Unban", desc))
    await send_log(ctx.guild, "Unban", ctx.author, display, uid,
                   sanction_id=revoked_sid, reason=reason, color=0x43b581)


# ========================= UNSANCTION GÉNÉRIQUE =========================

@bot.command(name="unsanction")
async def _unsanction(ctx, sanction_id: str = None, *, reason: str = None):
    """Annule n'importe quelle sanction active (Sys+ sans restriction)."""
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis."))
    if not sanction_id:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}unsanction <id> [raison]`"
        ))

    sid = sanction_id.upper().lstrip("#")
    sanc = get_sanction(sid)
    if not sanc:
        return await ctx.send(embed=error_embed("❌ Sanction introuvable", f"Aucune sanction avec l'ID `#{sid}`."))
    if not sanc["active"]:
        return await ctx.send(embed=error_embed("Déjà révoquée", f"La sanction `#{sid}` est déjà inactive."))

    # Modo ne peut unsanction que ses propres sanctions
    author_rank = get_rank_db(ctx.author.id)
    if author_rank < 3 and str(sanc["moderator_id"]) != str(ctx.author.id):
        return await ctx.send(embed=error_embed(
            "❌ Permission refusée",
            "Tu ne peux annuler que **tes propres** sanctions. Un Sys+ peut en annuler n'importe laquelle."
        ))

    target_id = int(sanc["target_id"])
    display = ctx.guild.get_member(target_id) if ctx.guild else None
    if not display:
        try:
            display = await bot.fetch_user(target_id)
        except discord.HTTPException:
            display = None

    # Effet Discord selon le type
    stype = sanc["type"]
    action_msg = ""
    if stype == "mute" and display and isinstance(display, discord.Member):
        try:
            await display.timeout(None, reason=f"Unsanction par {ctx.author}")
            action_msg = " (unmute Discord appliqué)"
        except discord.HTTPException:
            pass
    elif stype == "vmute" and display and isinstance(display, discord.Member):
        try:
            await display.edit(mute=False, reason=f"Unsanction par {ctx.author}")
            action_msg = " (unvmute Discord appliqué)"
        except discord.HTTPException:
            pass
    elif stype == "ban":
        try:
            await ctx.guild.unban(discord.Object(id=target_id),
                                  reason=f"Unsanction par {ctx.author}")
            action_msg = " (unban Discord appliqué)"
        except (discord.NotFound, discord.HTTPException):
            pass

    revoke_sanction(sid, ctx.author.id, reason or "Aucune raison")

    await ctx.send(embed=success_embed(
        "✅ Sanction annulée",
        f"Sanction `#{sid}` ({stype}) annulée pour {format_user_display(display, target_id)}.{action_msg}"
    ))
    await send_log(ctx.guild, f"Unsanction ({stype})", ctx.author, display, target_id,
                   sanction_id=sid, reason=reason, color=0x43b581)


# ========================= CASIER / SANCTION DÉTAIL =========================

@bot.command(name="casier")
async def _casier(ctx, *, user_input: str = None):
    """Affiche le casier d'un membre. Sans argument : son propre casier."""
    if await check_bot_ban(ctx):
        return

    # Sans argument : l'auteur voit son propre casier
    if user_input is None:
        target = ctx.author
        uid = ctx.author.id
        display = ctx.author
    else:
        if not has_min_rank(ctx.author.id, 1):
            return await ctx.send(embed=error_embed(
                "❌ Permission refusée",
                f"**Helper+** requis pour voir le casier des autres.\n"
                f"Tu peux voir **le tien** avec `{get_prefix_cached()}casier` (sans argument)."
            ))
        display, uid = await resolve_user_or_id(ctx, user_input)
        if uid is None:
            return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    sanctions = get_user_sanctions(ctx.guild.id, uid, active_only=False)
    if not sanctions:
        return await ctx.send(embed=info_embed(
            "📋 Casier vide",
            f"{format_user_display(display, uid)} n'a **aucune sanction** dans son historique."
        ))

    # Compte les stats
    actives = [s for s in sanctions if s["active"]]
    total = len(sanctions)
    active_warns = sum(1 for s in actives if s["type"] == "warn")

    type_emojis = {
        "warn": "⚠️", "mute": "🔇", "vmute": "🎤",
        "kick": "👢", "ban": "⛔", "timeout": "⏰",
    }

    lines = []
    for s in sanctions[:15]:  # on affiche les 15 plus récentes
        emoji = type_emojis.get(s["type"], "📋")
        active_mark = "🟢" if s["active"] else "⚪"
        created = format_datetime(s["created_at"])
        dur = ""
        if s["expires_at"]:
            dur = f" ・ {format_duration(int((datetime.fromisoformat(s['expires_at']) - datetime.fromisoformat(s['created_at'])).total_seconds()))}"
        reason_short = s["reason"][:60] + ("…" if len(s["reason"]) > 60 else "")
        lines.append(
            f"{active_mark} `#{s['id']}` {emoji} **{s['type']}**{dur} ・ {created}\n"
            f"    ↳ {reason_short}\n"
            f"    ↳ par <@{s['moderator_id']}>"
        )

    header = (
        f"**Total :** {total} sanction(s)  ・  "
        f"**Actives :** {len(actives)}  ・  "
        f"**Warns actifs :** {active_warns}\n\n"
    )
    if len(sanctions) > 15:
        lines.append(f"\n*... et {len(sanctions) - 15} autres plus anciennes*")

    em = discord.Embed(
        title=f"📋 Casier — {display.display_name if display else f'ID {uid}'}",
        description=header + "\n".join(lines),
        color=embed_color(),
    )
    if display and hasattr(display, "display_avatar"):
        em.set_thumbnail(url=display.display_avatar.url)
    em.set_footer(text=f"Sanction ・ {get_prefix_cached()}sanction <id> pour le détail")
    await ctx.send(embed=em)


@bot.command(name="sanction")
async def _sanction(ctx, sanction_id: str = None):
    """Affiche le détail d'une sanction précise."""
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Helper+** requis."))
    if not sanction_id:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}sanction <id>`"
        ))

    sid = sanction_id.upper().lstrip("#")
    sanc = get_sanction(sid)
    if not sanc:
        return await ctx.send(embed=error_embed("❌ Sanction introuvable", f"Aucune sanction avec l'ID `#{sid}`."))

    type_emojis = {
        "warn": "⚠️", "mute": "🔇", "vmute": "🎤",
        "kick": "👢", "ban": "⛔", "timeout": "⏰",
    }
    emoji = type_emojis.get(sanc["type"], "📋")

    em = discord.Embed(
        title=f"{emoji} Sanction `#{sid}`",
        color=0x43b581 if not sanc["active"] else 0xe74c3c,
    )
    em.add_field(name="Type", value=sanc["type"], inline=True)
    em.add_field(name="Statut", value="🟢 Active" if sanc["active"] else "⚪ Révoquée", inline=True)
    em.add_field(name="Cible", value=f"<@{sanc['target_id']}> (`{sanc['target_id']}`)", inline=True)
    em.add_field(name="Par", value=f"<@{sanc['moderator_id']}>", inline=True)
    em.add_field(name="Créée le", value=format_datetime(sanc["created_at"]), inline=True)
    if sanc["expires_at"]:
        try:
            dur = int((datetime.fromisoformat(sanc["expires_at"]) -
                      datetime.fromisoformat(sanc["created_at"])).total_seconds())
            em.add_field(name="Durée", value=format_duration(dur), inline=True)
        except (ValueError, TypeError):
            pass
        em.add_field(name="Expire le", value=format_datetime(sanc["expires_at"]), inline=True)
    em.add_field(name="Raison", value=sanc["reason"], inline=False)
    if not sanc["active"]:
        em.add_field(name="Révoquée par", value=f"<@{sanc['revoked_by']}>" if sanc["revoked_by"] else "?", inline=True)
        em.add_field(name="Révoquée le", value=format_datetime(sanc["revoked_at"]), inline=True)
        if sanc.get("revoke_reason"):
            em.add_field(name="Motif révocation", value=sanc["revoke_reason"], inline=False)
    em.set_footer(text="Sanction ・ Meira")
    await ctx.send(embed=em)


@bot.command(name="resetcasier")
async def _resetcasier(ctx, *, user_input: str = None):
    """Wipe complet du casier d'un membre. Action drastique, Sys+ requis."""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    deleted = reset_user_casier(ctx.guild.id, uid)
    if deleted == 0:
        return await ctx.send(embed=info_embed("Casier vide", f"{format_user_display(display, uid)} n'avait aucune sanction."))

    await ctx.send(embed=success_embed(
        "🧹 Casier reset",
        f"**{deleted}** sanction(s) supprimée(s) du casier de {format_user_display(display, uid)}."
    ))
    await send_log(ctx.guild, "Reset casier", ctx.author, display, uid,
                   desc=f"{deleted} sanctions supprimées", color=0xf04747)


# ========================= LOOP D'EXPIRATION (vraie logique) =========================


@tasks.loop(seconds=30)
async def expire_sanctions_loop():
    """
    Vérifie toutes les 30s les sanctions expirées et débloque auto.
    - Les timeouts Discord natifs se débloquent tout seuls (on a juste à marquer inactif en DB).
    - Les bans temporaires : on appelle guild.unban().
    - Les vmutes : on appelle member.edit(mute=False).
    """
    try:
        expiring = get_expiring_sanctions()
    except sqlite3.Error as e:
        log.error(f"expire_sanctions_loop: erreur DB : {e}")
        return

    for sanc in expiring:
        sid = sanc["id"]
        guild = bot.get_guild(int(sanc["guild_id"]))
        if not guild:
            # Le bot n'est plus sur ce serveur : on marque juste inactif pour pas boucler
            revoke_sanction(sid, str(bot.user.id), "Expiration auto (guild inaccessible)")
            continue

        target_id = int(sanc["target_id"])
        stype = sanc["type"]

        try:
            if stype == "ban":
                try:
                    await guild.unban(discord.Object(id=target_id), reason="Expiration auto du ban")
                    log.info(f"Expiration auto ban #{sid} pour {target_id}")
                except discord.NotFound:
                    pass  # Déjà unban manuellement
                except discord.Forbidden:
                    log.warning(f"Permission refusée pour unban auto #{sid}")
                    continue
                except discord.HTTPException as e:
                    log.warning(f"Erreur unban auto #{sid} : {e}")
                    continue

            elif stype == "vmute":
                member = guild.get_member(target_id)
                if member:
                    try:
                        await member.edit(mute=False, reason="Expiration auto du vmute")
                        log.info(f"Expiration auto vmute #{sid}")
                    except (discord.Forbidden, discord.HTTPException):
                        pass

            # mute (timeout Discord) : Discord le gère tout seul, on marque juste inactif

            revoke_sanction(sid, str(bot.user.id), "Expiration auto")

            # Log (avec le modérateur qui avait posé la sanction)
            target_member = guild.get_member(target_id)
            if not target_member:
                try:
                    target_member = await bot.fetch_user(target_id)
                except discord.HTTPException:
                    pass
            await send_log(
                guild, f"Expiration auto ({stype})", bot.user,
                target_member, target_id,
                sanction_id=sid,
                desc=f"Sanction `#{sid}` expirée (durée atteinte)",
                color=0x95a5a6,
            )
        except Exception as e:
            log.error(f"expire_sanctions_loop: erreur sur #{sid} : {e}\n{traceback.format_exc()}")


# ========================= RE-BAN AU JOIN =========================

@bot.event
async def on_member_join(member):
    """Si un user ban actif rejoint (cas rare mais possible après restart/unban externe),
    on le re-ban. + Check anti-raid (en partie 3)."""
    # Re-ban au join si ban actif en DB
    active_sanctions = get_user_sanctions(member.guild.id, member.id, active_only=True)
    active_bans = [s for s in active_sanctions if s["type"] == "ban"]
    if active_bans:
        try:
            await member.ban(reason="Ban actif en DB (re-sécurisation)")
            log.info(f"Re-ban auto de {member} (ban actif #{active_bans[0]['id']})")
        except (discord.Forbidden, discord.HTTPException):
            pass

    # Anti-raid : tracking + déclenchement (implémenté en partie 3)
    await antiraid_check_join(member)


async def antiraid_check_join(member):
    """Placeholder — logique complète en partie 3."""
    pass


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║      PARTIE 3 — PURGE, LOCK, NOTES, APPELS, ANTI-RAID, STATS, HELP       ║
# ╚══════════════════════════════════════════════════════════════════════════╝


# ========================= PURGE =========================

@bot.command(name="purge")
async def _purge(ctx, count: int = None, *, user_input: str = None):
    """
    Supprime les N derniers messages. Optionnel : filtrer par user.
    -purge 50          → 50 derniers messages
    -purge 50 @user    → 50 derniers messages de @user
    """
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Helper+** requis."))
    if count is None or count <= 0:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}purge <nombre> [@user]`"
        ))

    author_rank = get_rank_db(ctx.author.id)
    max_count = PURGE_LIMITS.get(author_rank, 20)
    if count > max_count:
        return await ctx.send(embed=error_embed(
            "❌ Limite dépassée",
            f"En tant que **{rank_name(author_rank)}**, tu peux purger jusqu'à **{max_count}** messages max."
        ))

    # Résolution du filtre user si fourni
    target_id = None
    target_display = None
    if user_input:
        target_display, target_id = await resolve_user_or_id(ctx, user_input)
        if target_id is None:
            return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Filtrage user invalide."))

    # Supprime d'abord le message de commande pour qu'il soit pas compté
    try:
        await ctx.message.delete()
    except discord.HTTPException:
        pass

    def check(m):
        if target_id is not None:
            return m.author.id == target_id
        return True

    try:
        deleted = await ctx.channel.purge(limit=count, check=check, bulk=True)
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je n'ai pas la permission de supprimer des messages ici."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    confirm_desc = f"**{len(deleted)}** message(s) supprimé(s)"
    if target_id:
        confirm_desc += f" de {format_user_display(target_display, target_id)}"
    confirm_desc += f" dans {ctx.channel.mention}."

    # Message éphémère qui s'auto-détruit (on le supprime après 5s)
    confirm_msg = await ctx.send(embed=success_embed("🧹 Purge", confirm_desc))
    await send_log(ctx.guild, "Purge", ctx.author,
                   target_display, target_id if target_id else None,
                   desc=f"{len(deleted)} msg dans {ctx.channel.mention}",
                   color=0x95a5a6)
    await asyncio.sleep(5)
    try:
        await confirm_msg.delete()
    except discord.HTTPException:
        pass


# ========================= LOCK / UNLOCK / SLOWMODE =========================

@bot.command(name="lock")
async def _lock(ctx, channel: discord.TextChannel = None, *, reason: str = None):
    """Lock le salon courant (ou celui spécifié). Retire send_messages à @everyone."""
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis."))

    ch = channel or ctx.channel
    ok, reason_or_err = validate_reason(reason)
    if not ok:
        return await ctx.send(embed=error_embed("❌ Raison requise", reason_or_err))
    reason = reason_or_err

    if is_locked(ch.id):
        return await ctx.send(embed=error_embed("Déjà lock", f"{ch.mention} est déjà verrouillé."))

    try:
        overwrite = ch.overwrites_for(ctx.guild.default_role)
        overwrite.send_messages = False
        await ch.set_permissions(ctx.guild.default_role, overwrite=overwrite,
                                 reason=f"Lock par {ctx.author} | {reason}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je ne peux pas modifier ce salon."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    add_locked_channel(ch.id, ctx.guild.id, ctx.author.id, reason)
    await ctx.send(embed=success_embed(
        "🔒 Salon verrouillé",
        f"{ch.mention} est maintenant **verrouillé**.\n**Raison :** {reason}"
    ))
    await send_log(ctx.guild, "Lock", ctx.author,
                   desc=f"Salon : {ch.mention}", reason=reason, color=0xe67e22)


@bot.command(name="unlock")
async def _unlock(ctx, channel: discord.TextChannel = None):
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis."))

    ch = channel or ctx.channel

    try:
        overwrite = ch.overwrites_for(ctx.guild.default_role)
        overwrite.send_messages = None
        await ch.set_permissions(ctx.guild.default_role, overwrite=overwrite,
                                 reason=f"Unlock par {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je ne peux pas modifier ce salon."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    remove_locked_channel(ch.id)
    await ctx.send(embed=success_embed("🔓 Salon déverrouillé", f"{ch.mention} est à nouveau ouvert."))
    await send_log(ctx.guild, "Unlock", ctx.author,
                   desc=f"Salon : {ch.mention}", color=0x43b581)


@bot.command(name="slowmode")
async def _slowmode(ctx, duration: str = None, channel: discord.TextChannel = None):
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Helper+** requis."))
    if duration is None:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}slowmode <durée> [#salon]`\n"
            f"Ex : `{get_prefix_cached()}slowmode 10s`, `{get_prefix_cached()}slowmode 0` pour désactiver"
        ))

    ch = channel or ctx.channel

    # 0 = désactiver
    if duration in ("0", "off", "none"):
        seconds = 0
    else:
        seconds, err = parse_duration(duration)
        if err:
            return await ctx.send(embed=error_embed("❌ Durée invalide", err))

    # Limite Discord : 6h max (21600s)
    if seconds > 21600:
        return await ctx.send(embed=error_embed(
            "❌ Durée trop longue",
            "Le slowmode Discord est limité à **6 heures** max."
        ))

    try:
        await ch.edit(slowmode_delay=seconds, reason=f"Slowmode par {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je ne peux pas modifier ce salon."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    if seconds == 0:
        await ctx.send(embed=success_embed("🐢 Slowmode désactivé", f"{ch.mention} n'a plus de slowmode."))
    else:
        await ctx.send(embed=success_embed(
            "🐢 Slowmode activé",
            f"{ch.mention} : **{format_duration(seconds)}** entre chaque message."
        ))
    await send_log(ctx.guild, "Slowmode", ctx.author,
                   desc=f"{ch.mention} → {format_duration(seconds) if seconds else 'désactivé'}",
                   color=0x95a5a6)


# ========================= NOTES STAFF =========================

@bot.command(name="note")
async def _note(ctx, user_input: str = None, *, content: str = None):
    """Ajoute une note privée staff sur un membre."""
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis pour ajouter une note."))
    if not user_input or not content:
        return await ctx.send(embed=error_embed(
            "Arguments manquants",
            f"Usage : `{get_prefix_cached()}note @user <contenu>`"
        ))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    content = content.strip()
    if len(content) < 3:
        return await ctx.send(embed=error_embed("❌ Note trop courte", "La note doit faire au moins 3 caractères."))
    if len(content) > 500:
        return await ctx.send(embed=error_embed("❌ Note trop longue", "Max 500 caractères."))

    note_id = add_staff_note(ctx.guild.id, uid, ctx.author.id, content)
    await ctx.send(embed=success_embed(
        "📝 Note ajoutée",
        f"Note `#{note_id}` ajoutée sur {format_user_display(display, uid)}.\n"
        f"Visible uniquement par le staff via `{get_prefix_cached()}notes @user`."
    ))
    await send_log(ctx.guild, "Note ajoutée", ctx.author, display, uid,
                   desc=f"Note `#{note_id}`", color=0x95a5a6)


@bot.command(name="notes")
async def _notes(ctx, *, user_input: str = None):
    """Affiche les notes staff sur un membre."""
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Helper+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    notes = get_staff_notes(ctx.guild.id, uid)
    if not notes:
        return await ctx.send(embed=info_embed(
            "📝 Aucune note",
            f"Aucune note staff sur {format_user_display(display, uid)}."
        ))

    lines = []
    for n in notes[:15]:
        created = format_datetime(n["created_at"])
        content = n["content"][:200] + ("…" if len(n["content"]) > 200 else "")
        lines.append(f"`#{n['id']}` ・ {created} ・ par <@{n['author_id']}>\n> {content}")

    em = discord.Embed(
        title=f"📝 Notes staff — {display.display_name if display else f'ID {uid}'}",
        description="\n\n".join(lines),
        color=embed_color(),
    )
    if len(notes) > 15:
        em.set_footer(text=f"Sanction ・ {len(notes)} notes au total, 15 affichées")
    else:
        em.set_footer(text=f"Sanction ・ {len(notes)} note(s)")
    await ctx.send(embed=em)


@bot.command(name="delnote")
async def _delnote(ctx, note_id: int = None):
    """Supprime une note staff par son ID."""
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis."))
    if note_id is None:
        return await ctx.send(embed=error_embed("Argument manquant", f"Usage : `{get_prefix_cached()}delnote <id>`"))

    if not delete_staff_note(note_id):
        return await ctx.send(embed=error_embed("❌ Note introuvable", f"Aucune note avec l'ID `#{note_id}`."))

    await ctx.send(embed=success_embed("✅ Note supprimée", f"Note `#{note_id}` supprimée."))
    await send_log(ctx.guild, "Note supprimée", ctx.author,
                   desc=f"Note `#{note_id}`", color=0xe67e22)


# ========================= APPELS =========================

@bot.command(name="appel")
async def _appel(ctx, sanction_id: str = None, *, motif: str = None):
    """Un membre conteste une sanction reçue."""
    if await check_bot_ban(ctx):
        return
    if not sanction_id:
        return await ctx.send(embed=error_embed(
            "Arguments manquants",
            f"Usage : `{get_prefix_cached()}appel <id_sanction> <motif>`\n"
            f"Ex : `{get_prefix_cached()}appel A7F3 Je pense que le warn est injuste parce que...`"
        ))

    sid = sanction_id.upper().lstrip("#")
    sanc = get_sanction(sid)
    if not sanc:
        return await ctx.send(embed=error_embed("❌ Sanction introuvable", f"Aucune sanction avec l'ID `#{sid}`."))
    if str(sanc["target_id"]) != str(ctx.author.id):
        return await ctx.send(embed=error_embed(
            "❌ Pas ta sanction",
            "Tu ne peux faire appel que pour tes propres sanctions."
        ))
    if not sanc["active"]:
        return await ctx.send(embed=error_embed(
            "Sanction déjà résolue",
            f"La sanction `#{sid}` est déjà inactive, pas besoin de faire appel."
        ))

    if not motif or len(motif.strip()) < 10:
        return await ctx.send(embed=error_embed(
            "❌ Motif trop court",
            "Explique en détail (**minimum 10 caractères**) pourquoi tu contestes cette sanction.\n"
            "Un bon motif aide le staff à trancher équitablement."
        ))
    motif = motif.strip()

    if user_has_pending_appeal(ctx.author.id, sid):
        return await ctx.send(embed=error_embed(
            "❌ Appel en cours",
            f"Tu as déjà un appel en attente pour la sanction `#{sid}`. Patiente que le staff le traite."
        ))

    appeal_id = create_appeal(sid, ctx.guild.id, ctx.author.id, motif)

    await ctx.send(embed=success_embed(
        "📨 Appel envoyé",
        f"Ton appel pour la sanction `#{sid}` a été transmis au staff.\n"
        f"ID de ton appel : `#{appeal_id}`\n\n"
        f"Tu recevras une notification par DM quand il sera traité."
    ))
    await send_log(ctx.guild, "Nouvel appel", ctx.author, ctx.author, ctx.author.id,
                   desc=f"Appel `#{appeal_id}` sur sanction `#{sid}`",
                   reason=motif[:200] + ("…" if len(motif) > 200 else ""),
                   color=0x3498db)


@bot.command(name="appels")
async def _appels(ctx):
    """Liste des appels en attente (Modo+)."""
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis."))

    pending = get_pending_appeals(ctx.guild.id)
    if not pending:
        return await ctx.send(embed=info_embed("📨 Aucun appel", "Aucun appel en attente."))

    lines = []
    for a in pending[:10]:
        created = format_datetime(a["created_at"])
        motif_short = a["motif"][:150] + ("…" if len(a["motif"]) > 150 else "")
        lines.append(
            f"**Appel `#{a['id']}`** ・ {created}\n"
            f"   ↳ Sanction : `#{a['sanction_id']}`\n"
            f"   ↳ Par : <@{a['user_id']}>\n"
            f"   ↳ Motif : *{motif_short}*"
        )

    em = discord.Embed(
        title=f"📨 Appels en attente ({len(pending)})",
        description="\n\n".join(lines) + f"\n\nUtilise `{get_prefix_cached()}traiter <id> accept/reject <motif>` pour trancher.",
        color=0x3498db,
    )
    if len(pending) > 10:
        em.set_footer(text=f"10 affichés sur {len(pending)}")
    await ctx.send(embed=em)


@bot.command(name="traiter")
async def _traiter(ctx, appeal_id: int = None, decision: str = None, *, reason: str = None):
    """Traite un appel : accept ou reject, avec motif."""
    if await check_bot_ban(ctx):
        return
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Modérateur+** requis."))
    if appeal_id is None or decision is None:
        return await ctx.send(embed=error_embed(
            "Arguments manquants",
            f"Usage : `{get_prefix_cached()}traiter <id_appel> accept/reject <motif>`"
        ))

    decision = decision.lower()
    if decision not in ("accept", "reject", "accepter", "refuser"):
        return await ctx.send(embed=error_embed(
            "❌ Décision invalide", "Utilise `accept` ou `reject`."
        ))
    if decision in ("accepter",):
        decision = "accept"
    elif decision in ("refuser",):
        decision = "reject"

    ok, reason_or_err = validate_reason(reason)
    if not ok:
        return await ctx.send(embed=error_embed("❌ Motif requis", reason_or_err + "\n(Un motif clair aide le membre à comprendre la décision.)"))
    reason = reason_or_err

    appeal = get_appeal(appeal_id)
    if not appeal:
        return await ctx.send(embed=error_embed("❌ Appel introuvable", f"Aucun appel `#{appeal_id}`."))
    if appeal["status"] != "pending":
        return await ctx.send(embed=error_embed(
            "Déjà traité",
            f"Cet appel a déjà été traité (**{appeal['status']}**)."
        ))

    status = "accepted" if decision == "accept" else "rejected"
    if not handle_appeal(appeal_id, status, ctx.author.id, reason):
        return await ctx.send(embed=error_embed("❌ Erreur", "Impossible de traiter cet appel."))

    # Si accepté : on révoque la sanction
    if decision == "accept":
        sid = appeal["sanction_id"]
        sanc = get_sanction(sid)
        target_id = int(appeal["user_id"])
        if sanc and sanc["active"]:
            # Applique le déblocage Discord selon le type
            stype = sanc["type"]
            target_member = ctx.guild.get_member(target_id)
            try:
                if stype == "mute" and target_member:
                    try:
                        await target_member.timeout(None, reason=f"Appel accepté par {ctx.author}")
                    except discord.HTTPException:
                        pass
                elif stype == "vmute" and target_member:
                    try:
                        await target_member.edit(mute=False, reason=f"Appel accepté par {ctx.author}")
                    except discord.HTTPException:
                        pass
                elif stype == "ban":
                    try:
                        await ctx.guild.unban(discord.Object(id=target_id),
                                              reason=f"Appel accepté par {ctx.author}")
                    except (discord.NotFound, discord.HTTPException):
                        pass
            except Exception as e:
                log.warning(f"Erreur déblocage suite à appel accepté : {e}")

            revoke_sanction(sid, ctx.author.id, f"Appel #{appeal_id} accepté : {reason}")

    # Notif DM au membre
    user_id = int(appeal["user_id"])
    try:
        user_obj = await bot.fetch_user(user_id)
        color = 0x43b581 if decision == "accept" else 0xf04747
        title = "✅ Ton appel a été accepté" if decision == "accept" else "❌ Ton appel a été refusé"
        em = discord.Embed(title=title, color=color)
        em.add_field(name="Sanction concernée", value=f"`#{appeal['sanction_id']}`", inline=True)
        em.add_field(name="Serveur", value=ctx.guild.name, inline=True)
        em.add_field(name="Motif de la décision", value=reason, inline=False)
        em.set_footer(text="Sanction ・ Meira")
        await user_obj.send(embed=em)
    except (discord.Forbidden, discord.HTTPException, discord.NotFound):
        pass

    emoji = "✅" if decision == "accept" else "❌"
    action_word = "accepté" if decision == "accept" else "refusé"
    await ctx.send(embed=success_embed(
        f"{emoji} Appel {action_word}",
        f"Appel `#{appeal_id}` **{action_word}**.\n"
        f"Sanction concernée : `#{appeal['sanction_id']}`\n"
        f"**Motif :** {reason}" +
        ("\n\nLa sanction a été révoquée." if decision == "accept" else "")
    ))
    await send_log(
        ctx.guild, f"Appel {action_word}", ctx.author,
        desc=f"Appel `#{appeal_id}` sur sanction `#{appeal['sanction_id']}`",
        reason=reason,
        color=0x43b581 if decision == "accept" else 0xf04747,
    )


# ========================= ANTI-RAID =========================

async def antiraid_check_join(member):
    """Vérifie si un pic de joins déclenche l'anti-raid et agit."""
    cfg = get_antiraid()
    if not cfg.get("enabled"):
        return

    guild_id = member.guild.id
    threshold = cfg.get("joins_threshold", 5)
    window = cfg.get("window_seconds", 10)
    action = cfg.get("action", "timeout")
    timeout_dur = cfg.get("timeout_duration", 3600)

    now = datetime.now(PARIS_TZ)
    if guild_id not in _recent_joins:
        _recent_joins[guild_id] = []
    # On nettoie les joins en dehors de la fenêtre
    cutoff = now - timedelta(seconds=window)
    _recent_joins[guild_id] = [(m, t) for (m, t) in _recent_joins[guild_id] if t > cutoff]
    _recent_joins[guild_id].append((member, now))

    if len(_recent_joins[guild_id]) >= threshold:
        # RAID DÉTECTÉ → on applique l'action sur tous les membres du pic
        log.warning(f"Anti-raid déclenché sur {member.guild.name} : {len(_recent_joins[guild_id])} joins en {window}s")
        members_to_act = [m for (m, t) in _recent_joins[guild_id]]
        _recent_joins[guild_id] = []  # reset pour pas reboucler

        actioned = 0
        for m in members_to_act:
            try:
                if action == "kick":
                    await m.kick(reason="Anti-raid : pic de joins détecté")
                else:
                    # timeout par défaut
                    until = discord.utils.utcnow() + timedelta(seconds=timeout_dur)
                    await m.timeout(until, reason="Anti-raid : pic de joins détecté")
                actioned += 1
            except (discord.Forbidden, discord.HTTPException):
                pass

        # Alerte staff
        await send_log(
            member.guild, "🚨 ANTI-RAID DÉCLENCHÉ", bot.user,
            desc=(
                f"**{len(members_to_act)}** comptes ont rejoint en moins de **{window}s** "
                f"(seuil : {threshold}).\n"
                f"Action appliquée : **{action}** sur **{actioned}** comptes.\n\n"
                f"Utilise `{get_prefix_cached()}casier` sur chacun pour inspecter."
            ),
            color=0xf04747,
        )


@bot.command(name="antiraid")
async def _antiraid(ctx, action: str = None, *, value: str = None):
    """
    Gère l'anti-raid.
    -antiraid            → statut
    -antiraid on/off     → activer/désactiver
    -antiraid threshold 5 → seuil de joins
    -antiraid window 10   → fenêtre en secondes
    -antiraid action timeout|kick → action
    -antiraid duration 3600 → durée timeout en secondes
    """
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))

    cfg = get_antiraid()

    if action is None or action == "status":
        em = discord.Embed(title="🛡️ Anti-raid", color=embed_color())
        em.add_field(name="Statut", value="🟢 Activé" if cfg.get("enabled") else "⚪ Désactivé", inline=True)
        em.add_field(name="Seuil", value=f"{cfg.get('joins_threshold', 5)} joins", inline=True)
        em.add_field(name="Fenêtre", value=f"{cfg.get('window_seconds', 10)}s", inline=True)
        em.add_field(name="Action", value=cfg.get("action", "timeout"), inline=True)
        em.add_field(name="Durée timeout", value=format_duration(cfg.get("timeout_duration", 3600)), inline=True)
        em.set_footer(text="Sanction ・ Meira")
        return await ctx.send(embed=em)

    action = action.lower()
    p = get_prefix_cached()

    if action in ("on", "enable"):
        cfg["enabled"] = True
        set_antiraid(cfg)
        await ctx.send(embed=success_embed("🛡️ Anti-raid activé",
            f"Déclenchement : **{cfg['joins_threshold']} joins** en **{cfg['window_seconds']}s**."))
    elif action in ("off", "disable"):
        cfg["enabled"] = False
        set_antiraid(cfg)
        await ctx.send(embed=success_embed("⚪ Anti-raid désactivé", "Plus de détection active."))
    elif action == "threshold":
        try:
            n = int(value)
            if n < 2 or n > 50:
                raise ValueError
        except (ValueError, TypeError):
            return await ctx.send(embed=error_embed("❌ Valeur invalide", "Seuil entre 2 et 50."))
        cfg["joins_threshold"] = n
        set_antiraid(cfg)
        await ctx.send(embed=success_embed("✅ Seuil mis à jour", f"Seuil : **{n}** joins."))
    elif action == "window":
        try:
            n = int(value)
            if n < 3 or n > 600:
                raise ValueError
        except (ValueError, TypeError):
            return await ctx.send(embed=error_embed("❌ Valeur invalide", "Fenêtre entre 3 et 600 secondes."))
        cfg["window_seconds"] = n
        set_antiraid(cfg)
        await ctx.send(embed=success_embed("✅ Fenêtre mise à jour", f"Fenêtre : **{n}s**."))
    elif action == "action":
        if value not in ("timeout", "kick"):
            return await ctx.send(embed=error_embed("❌ Valeur invalide", "Actions : `timeout` ou `kick`."))
        cfg["action"] = value
        set_antiraid(cfg)
        await ctx.send(embed=success_embed("✅ Action mise à jour", f"Action : **{value}**."))
    elif action == "duration":
        seconds, err = parse_duration(value)
        if err or seconds <= 0:
            return await ctx.send(embed=error_embed("❌ Durée invalide", "Utilise par ex. `1h`, `30m`, `7j`."))
        cfg["timeout_duration"] = seconds
        set_antiraid(cfg)
        await ctx.send(embed=success_embed("✅ Durée mise à jour", f"Durée timeout : **{format_duration(seconds)}**."))
    else:
        await ctx.send(embed=info_embed(
            "🛡️ Anti-raid — Aide",
            f"```\n"
            f"{p}antiraid                → statut\n"
            f"{p}antiraid on/off         → activer/désactiver\n"
            f"{p}antiraid threshold 5    → seuil de joins\n"
            f"{p}antiraid window 10      → fenêtre en secondes\n"
            f"{p}antiraid action timeout → action (timeout/kick)\n"
            f"{p}antiraid duration 1h    → durée du timeout\n"
            f"```"
        ))


# ========================= ESCALATION CONFIG =========================

@bot.command(name="escalation")
async def _escalation(ctx, action: str = None, *args):
    """
    Gère les paliers d'auto-escalation.
    -escalation              → affiche les règles
    -escalation reset        → restaure par défaut
    -escalation clear        → vide (désactive l'escalation)
    """
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))

    rules = get_escalation()
    p = get_prefix_cached()

    if action is None or action == "status" or action == "list":
        if not rules:
            return await ctx.send(embed=info_embed(
                "⚠️ Escalation",
                "Aucune règle configurée (escalation désactivée)."
            ))
        lines = []
        for r in sorted(rules, key=lambda x: x["warns"]):
            dur = format_duration(r.get("duration", 0)) if r.get("duration") else "—"
            lines.append(
                f"**{r['warns']} warns** → **{r['action']}** "
                f"({'durée : ' + dur if r['action'] in ('mute',) else 'instant'})"
            )
        em = info_embed("⚠️ Paliers d'auto-escalation", "\n".join(lines))
        em.set_footer(text=f"Sanction ・ {p}escalation reset pour restaurer les défauts")
        return await ctx.send(embed=em)

    if action == "reset":
        set_escalation(list(DEFAULT_ESCALATION))
        await ctx.send(embed=success_embed("✅ Escalation restaurée", "Règles par défaut rétablies."))
    elif action == "clear":
        set_escalation([])
        await ctx.send(embed=success_embed("🧹 Escalation vidée", "Aucun palier actif. L'auto-escalation ne s'appliquera plus."))
    else:
        await ctx.send(embed=info_embed(
            "⚠️ Escalation — Aide",
            f"```\n"
            f"{p}escalation           → affiche les règles actuelles\n"
            f"{p}escalation reset     → restaure les défauts\n"
            f"{p}escalation clear     → désactive l'escalation\n"
            f"```\n"
            f"*Pour modifier finement les paliers, édite directement la DB ou demande à l'admin du bot.*"
        ))


# ========================= MODSTATS =========================

@bot.command(name="modstats")
async def _modstats(ctx, *, user_input: str = None):
    """Stats de modération d'un modérateur (soi-même par défaut)."""
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Helper+** requis."))

    if user_input is None:
        target = ctx.author
        uid = ctx.author.id
    else:
        if not has_min_rank(ctx.author.id, 2):
            return await ctx.send(embed=error_embed(
                "❌ Permission refusée",
                f"**Modérateur+** requis pour voir les stats des autres.\n"
                f"Tu peux voir **tes** stats avec `{get_prefix_cached()}modstats` (sans argument)."
            ))
        target, uid = await resolve_user_or_id(ctx, user_input)
        if uid is None:
            return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    total_stats = get_mod_stats(ctx.guild.id, uid, days=None)
    week_stats = get_mod_stats(ctx.guild.id, uid, days=7)
    month_stats = get_mod_stats(ctx.guild.id, uid, days=30)

    def fmt(stats):
        if not stats:
            return "*Aucune action*"
        parts = []
        order = ["warn", "mute", "vmute", "kick", "ban", "timeout"]
        emojis = {"warn": "⚠️", "mute": "🔇", "vmute": "🎤", "kick": "👢", "ban": "⛔", "timeout": "⏰"}
        for t in order:
            if t in stats and stats[t] > 0:
                parts.append(f"{emojis.get(t, '📋')} **{stats[t]}** {t}")
        return " ・ ".join(parts) if parts else "*Aucune action*"

    em = discord.Embed(
        title=f"📊 Stats modération — {target.display_name if target else f'ID {uid}'}",
        color=embed_color(),
    )
    if target and hasattr(target, "display_avatar"):
        em.set_thumbnail(url=target.display_avatar.url)
    em.add_field(name="📅 7 derniers jours", value=fmt(week_stats), inline=False)
    em.add_field(name="📅 30 derniers jours", value=fmt(month_stats), inline=False)
    em.add_field(name="📊 Total", value=fmt(total_stats), inline=False)
    total_count = sum(total_stats.values())
    em.add_field(name="Total actions", value=f"**{total_count}**", inline=True)
    em.set_footer(text="Sanction ・ Meira")
    await ctx.send(embed=em)


# ========================= HELP DYNAMIQUE =========================

HELP_CATEGORIES = {
    "perso": {
        "emoji": "👤",
        "label": "Perso",
        "title": "👤  Perso",
        "items": [
            ("casier",                    "Voir ton propre casier",           0),
            ("appel <id> <motif>",        "Contester une sanction reçue",     0),
        ],
    },
    "sanctions_light": {
        "emoji": "⚠️",
        "label": "Sanctions (Helper+)",
        "title": "⚠️  Sanctions — Helper+",
        "items": [
            ("warn @user <raison>",       "Avertir (déclenche escalation)",   1),
            ("mute @user <durée> <r>",    "Mute textuel (max 1h Helper)",     1),
            ("vmute @user <durée> <r>",   "Mute vocal (max 1h Helper)",       1),
            ("casier @user",              "Voir le casier d'un membre",       1),
            ("sanction <id>",             "Détail d'une sanction",            1),
            ("notes @user",               "Lire les notes staff",             1),
            ("modstats",                  "Tes propres stats de modération",  1),
        ],
    },
    "sanctions_mid": {
        "emoji": "🔨",
        "label": "Sanctions (Modo+)",
        "title": "🔨  Sanctions — Modérateur+",
        "items": [
            ("kick @user <raison>",       "Expulser du serveur",              2),
            ("mute @user <durée> <r>",    "Mute textuel (max 24h Modo)",      2),
            ("vmute @user <durée> <r>",   "Mute vocal (max 24h Modo)",        2),
            ("unmute @user",              "Retirer un mute",                  2),
            ("unvmute @user",             "Retirer un vmute",                 2),
            ("unwarn <id> [raison]",      "Annuler un warn (le tien)",        2),
            ("unsanction <id> [raison]",  "Annuler une sanction (la tienne)", 2),
            ("modstats @mod",             "Stats d'un autre modo",            2),
        ],
    },
    "sanctions_hard": {
        "emoji": "⛔",
        "label": "Sanctions (Sys+)",
        "title": "⛔  Sanctions — Sys+",
        "items": [
            ("ban @user perm <raison>",      "Ban permanent",                   3),
            ("ban @user <durée> <raison>",   "Ban temporaire (déban auto)",     3),
            ("unban <id/mention> [raison]",  "Débannir",                        3),
            ("mute @user <durée> <r>",       "Mute textuel (max 28j Sys)",      3),
            ("unsanction <id> (n'importe)",  "Annuler toute sanction",          3),
            ("clearwarns @user",             "Effacer tous les warns",          3),
            ("resetcasier @user",            "Wipe complet du casier",          3),
        ],
    },
    "utilitaires": {
        "emoji": "🛠️",
        "label": "Utilitaires",
        "title": "🛠️  Utilitaires",
        "items": [
            ("purge <n>",          "Supprimer n messages (max 20 Helper, 100 Modo, 500 Sys)",  1),
            ("purge <n> @user",    "Messages ciblés (Modo+)",                 2),
            ("slowmode <durée>",   "Slowmode du salon (0 pour off)",          1),
            ("lock [#salon] <r>",  "Verrouiller un salon",                    2),
            ("unlock [#salon]",    "Déverrouiller",                           2),
        ],
    },
    "notes_appels": {
        "emoji": "📝",
        "label": "Notes & Appels",
        "title": "📝  Notes & Appels",
        "items": [
            ("note @user <contenu>",   "Ajouter une note staff privée",       2),
            ("notes @user",            "Lire les notes d'un membre",          1),
            ("delnote <id>",           "Supprimer une note",                  2),
            ("appels",                 "Lister les appels en attente",        2),
            ("traiter <id> accept/reject <motif>", "Trancher un appel",       2),
        ],
    },
    "perms": {
        "emoji": "👥",
        "label": "Permissions",
        "title": "👥  Permissions",
        "items": [
            ("helper @u / unhelper @u",  "Gérer les Helpers",          3),
            ("mod @u / unmod @u",        "Gérer les Modérateurs",      3),
            ("sys @u / unsys @u",        "Gérer les Sys",              4),
            ("botban @u / botunban @u",  "Ban/unban du bot",           3),
        ],
    },
    "system": {
        "emoji": "⚙️",
        "label": "Système",
        "title": "⚙️  Système",
        "items": [
            ("allow #salon / unallow #salon", "Gérer les salons autorisés", 3),
            ("allow",                         "Lister les salons autorisés", 3),
            ("escalation",                    "Config auto-escalation",     3),
            ("antiraid",                      "Config anti-raid",           3),
            ("setlog #salon",                 "Salon de logs",              4),
            ("prefix [nouveau]",              "Changer le prefix",          4),
        ],
    },
    "hierarchy": {
        "emoji": "📋",
        "label": "Hiérarchie",
        "title": "📋  Hiérarchie",
        "min_rank": 0,  # Visible pour tout le monde (même rang 0)
        "items": [],
    },
}


def help_accessible_items(key, rank):
    cat = HELP_CATEGORIES.get(key, {})
    return [(s, d) for (s, d, mr) in cat.get("items", []) if rank >= mr]


def help_category_visible(key, rank):
    cat = HELP_CATEGORIES.get(key, {})
    if "min_rank" in cat:
        return rank >= cat["min_rank"]
    return len(help_accessible_items(key, rank)) > 0


def build_help_category_embed(key, rank):
    p = get_prefix_cached()
    cat = HELP_CATEGORIES[key]
    em = discord.Embed(title=cat["title"], color=embed_color())
    items = help_accessible_items(key, rank)
    if not items:
        em.description = "*Aucune commande accessible à ton rang.*"
    else:
        max_syntax = max(len(f"{p}{syntax}") for syntax, _ in items)
        lines = [
            f"{p}{syntax}".ljust(max_syntax + 2) + f"→ {desc}"
            for syntax, desc in items
        ]
        em.description = "```\n" + "\n".join(lines) + "\n```"
    em.set_footer(text="Sanction ・ Meira")
    return em


def build_help_hierarchy_embed(rank):
    em = discord.Embed(title="📋  Hiérarchie", color=embed_color())
    lines = ["```\nBuyer > Sys > Modérateur > Helper > Aucun\n```\n"]
    levels = [
        (4, "👑 **Buyer**",       "`-prefix`, `-setlog`, `-sys`/`-unsys`. Accès total."),
        (3, "🔧 **Sys**",         "Ban, unban, mute max 28j, `-mod`/`-unmod`, `-helper`/`-unhelper`, antiraid, escalation, `-allow`/`-unallow`, `-botban`, `-resetcasier`"),
        (2, "⚠️ **Modérateur**",   "Kick, mute max 24h, unmute, unsanction propres, purge max 100, lock, notes, appels, traiter"),
        (1, "✨ **Helper**",      "Warn, mute max 1h, vmute max 1h, purge max 20, slowmode, casier, sanction, lire notes"),
        (0, "👤 **Aucun**",       "Son propre `-casier` et faire `-appel`"),
    ]
    for lvl, name, desc in levels:
        marker = " ← **toi**" if lvl == rank else ""
        lines.append(f"> {name} — {desc}{marker}")
    lines.append("")
    lines.append("ℹ️ Un rang ne peut **jamais** sanctionner un rang égal ou supérieur.")
    em.description = "\n".join(lines)
    em.set_footer(text="Sanction ・ Meira")
    return em


def build_help_home_embed(rank):
    p = get_prefix_cached()
    em = discord.Embed(color=embed_color())
    em.set_author(name="Sanction ─ Panel d'aide")

    rank_label = rank_name(rank)
    intro = (
        f"```\n🕐  {get_french_time()}\n```\n"
        f"Bienvenue sur **Sanction**, le bot de modération de Meira.\n\n"
        f"**Prefix :** `{p}` ・ **Ton rang :** {rank_label}\n\n"
    )

    category_descriptions = {
        "perso":           "Ton casier & faire appel d'une sanction",
        "sanctions_light": "Warn, mute, vmute",
        "sanctions_mid":   "Kick, unmute, purge, lock",
        "sanctions_hard":  "Ban, clearwarns, resetcasier",
        "utilitaires":     "Purge, lock, slowmode",
        "notes_appels":    "Notes staff privées & appels",
        "perms":           "Attribuer les rangs",
        "system":          "Configuration du bot",
        "hierarchy":       "Qui peut faire quoi",
    }
    visible = []
    for key, lbl in category_descriptions.items():
        if help_category_visible(key, rank):
            cat = HELP_CATEGORIES[key]
            visible.append(f"> {cat['emoji']} **{cat['label']}** — {lbl}")

    em.description = intro + ("\n".join(visible) if visible else "*Aucune catégorie disponible.*")
    em.set_footer(text="Sanction ・ Meira")
    return em


def build_help_embed_for(key, rank):
    if key == "home":
        return build_help_home_embed(rank)
    if key == "hierarchy":
        return build_help_hierarchy_embed(rank)
    return build_help_category_embed(key, rank)


class HelpDropdown(discord.ui.Select):
    def __init__(self, user_rank):
        self.user_rank = user_rank
        options = [discord.SelectOption(label="Accueil", emoji="🏠", value="home")]
        for key, cat in HELP_CATEGORIES.items():
            if help_category_visible(key, user_rank):
                options.append(discord.SelectOption(
                    label=cat["label"], emoji=cat["emoji"], value=key
                ))
        super().__init__(
            placeholder="📂 Choisis une catégorie...",
            min_values=1, max_values=1, options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        key = self.values[0]
        if key != "home" and not help_category_visible(key, self.user_rank):
            return await interaction.response.send_message(
                "Tu n'as pas accès à cette catégorie.", ephemeral=True
            )
        await interaction.response.edit_message(
            embed=build_help_embed_for(key, self.user_rank), view=self.view
        )


class HelpView(discord.ui.View):
    def __init__(self, author_id, user_rank):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.user_rank = user_rank
        self.add_item(HelpDropdown(user_rank))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"Ce menu n'est pas à toi. Fais `{get_prefix_cached()}help` pour voir le tien.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


@bot.command(name="help")
async def _help(ctx):
    if await check_bot_ban(ctx):
        return
    rank = get_rank_db(ctx.author.id)
    view = HelpView(ctx.author.id, rank)
    await ctx.send(embed=build_help_home_embed(rank), view=view)


# ========================= RUN =========================

if __name__ == "__main__":
    try:
        log.info("Démarrage de Sanction...")
        bot.run(BOT_TOKEN, log_handler=None)
    except KeyboardInterrupt:
        log.info("Arrêt demandé par l'utilisateur.")
    except Exception as e:
        log.error(f"Erreur fatale au démarrage : {e}", exc_info=True)
        sys.exit(1)
