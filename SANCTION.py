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
DEFAULT_BUYER_IDS = [1312375517927706630, 1312375955737542676, 1173948561881317389]
DEFAULT_PREFIX = "-"
# Volume persistant : DATA_DIR doit pointer vers un dossier persistant (volume Railway)
DATA_DIR = os.environ.get("DATA_DIR")
if not DATA_DIR:
    print("[ERREUR CRITIQUE] DATA_DIR non défini. Configure DATA_DIR=/data dans Railway.")
    sys.exit(1)
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "sanction.db")

# ==== SYSTÈME DE PERMS PAR NIVEAUX (1-9) ====
# Les rangs DB restent 0-4 mais seuls 3 (Sys) et 4 (Buyer) sont utilisés pour le bypass
# Tout le reste passe par les rôles Discord assignés à un niveau de perm

MAX_PERM_LEVEL = 9

# Perm par défaut de chaque commande de modération au premier lancement
# (Tu modifies après via `-setcmdperm <cmd> <niveau>`)
DEFAULT_CMD_PERMS = {
    # Niveau 1 — accès léger
    "warn":        1,
    "casier":      1,
    "sanction":    1,
    "notes":       1,
    "mylimits":    1,  # accessible à tout staff avec perm ≥ 1
    "modstats":    1,
    # Niveau 2
    "mute":        2,
    "vmute":       2,
    "unmute":      2,
    "unvmute":     2,
    "slowmode":    2,
    "clear":       2,  # ex-purge
    # Niveau 3
    "kick":        3,
    "lock":        3,
    "unlock":      3,
    "note":        3,
    "delnote":     3,
    "unwarn":      3,
    "traiter":     3,
    "appels":      3,
    # Niveau 4
    "ban":         4,
    "unban":       4,
    "unsanction":  4,
    # Niveau 5
    "clearwarns":  5,
    "resetcasier": 5,
}

# Limites par défaut : (max_actions, window_minutes) par niveau qui a accès
# Pour chaque commande, on définit le niveau minimum requis et combien d'actions max par fenêtre
# Format : {commande: {niveau: (max, fenêtre_minutes)}}
# Si une commande n'a pas d'entrée pour un niveau donné, c'est illimité à ce niveau (pas le cas par défaut)
DEFAULT_LIMITS = {
    # Ces valeurs sont des defaults raisonnables pour éviter les abus
    # Le modérateur avec ce niveau est limité ; Sys+ bypass toujours
    "warn":   {1: (10, 30), 2: (20, 30), 3: (50, 30), 4: (50, 30), 5: (100, 30)},
    "mute":   {2: (5, 30),  3: (10, 30), 4: (20, 30), 5: (50, 30)},
    "vmute":  {2: (5, 30),  3: (10, 30), 4: (20, 30), 5: (50, 30)},
    "kick":   {3: (3, 30),  4: (10, 30), 5: (20, 30)},
    "ban":    {4: (3, 30),  5: (10, 30)},
    "clear":  {2: (10, 10), 3: (30, 10), 4: (50, 10), 5: (100, 10)},
}

# Auto-escalation par défaut (seuil_warns → action, durée_secondes)
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
    "joins_threshold": 5,
    "window_seconds": 10,
    "action": "timeout",
    "timeout_duration": 3600,
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

    # ===== NOUVEAU SYSTÈME DE PERMS (rôles → niveau 1-9) =====

    # Rôles Discord attribués à un niveau de perm
    c.execute("""CREATE TABLE IF NOT EXISTS role_perms (
        guild_id TEXT NOT NULL,
        role_id TEXT NOT NULL,
        perm_level INTEGER NOT NULL CHECK (perm_level BETWEEN 1 AND 9),
        set_by TEXT,
        set_at TEXT,
        PRIMARY KEY (guild_id, role_id)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_role_perms_guild ON role_perms(guild_id)")

    # Historique des actions pour fenêtre glissante des limites
    c.execute("""CREATE TABLE IF NOT EXISTS action_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        command TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_action_hist ON action_history(user_id, command, created_at)")

    # Historique des deranks automatiques
    c.execute("""CREATE TABLE IF NOT EXISTS derank_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        role_id TEXT NOT NULL,
        removed_at TEXT NOT NULL,
        reason TEXT,
        reranked_at TEXT,
        reranked_by TEXT,
        rerank_reason TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_derank_user ON derank_history(user_id, guild_id)")

    # Default config
    c.execute("INSERT OR IGNORE INTO config VALUES ('prefix', ?)", (DEFAULT_PREFIX,))
    c.execute("INSERT OR IGNORE INTO config VALUES ('buyer_ids', ?)",
              (json.dumps([str(i) for i in DEFAULT_BUYER_IDS]),))
    c.execute("INSERT OR IGNORE INTO config VALUES ('escalation', ?)",
              (json.dumps(DEFAULT_ESCALATION),))
    c.execute("INSERT OR IGNORE INTO config VALUES ('antiraid', ?)",
              (json.dumps(DEFAULT_ANTIRAID),))
    # Perms des commandes (modifiables via -setcmdperm)
    c.execute("INSERT OR IGNORE INTO config VALUES ('cmd_perms', ?)",
              (json.dumps(DEFAULT_CMD_PERMS),))
    # Limites des commandes (modifiables via -setlimit)
    c.execute("INSERT OR IGNORE INTO config VALUES ('limits', ?)",
              (json.dumps(DEFAULT_LIMITS),))

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


# ---- Système de perms (rôles → niveau 1-9) ----

def get_cmd_perms():
    """Retourne le mapping {commande: niveau_perm}."""
    raw = get_config("cmd_perms")
    if not raw:
        return dict(DEFAULT_CMD_PERMS)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return dict(DEFAULT_CMD_PERMS)


def set_cmd_perm(command, level):
    cp = get_cmd_perms()
    cp[command] = int(level)
    set_config("cmd_perms", json.dumps(cp))


def get_cmd_perm(command):
    """Retourne le niveau de perm requis pour une commande (ou None si pas géré)."""
    cp = get_cmd_perms()
    return cp.get(command)


def role_perm_add(guild_id, role_id, level, set_by):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute("""INSERT OR REPLACE INTO role_perms
        (guild_id, role_id, perm_level, set_by, set_at) VALUES (?, ?, ?, ?, ?)""",
        (str(guild_id), str(role_id), int(level), str(set_by), now))
    conn.commit()
    conn.close()


def role_perm_remove(guild_id, role_id):
    conn = get_db()
    cur = conn.execute("DELETE FROM role_perms WHERE guild_id = ? AND role_id = ?",
                       (str(guild_id), str(role_id)))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def role_perm_list(guild_id):
    """Liste tous les rôles configurés dans cette guild."""
    conn = get_db()
    rows = conn.execute("""SELECT role_id, perm_level FROM role_perms
        WHERE guild_id = ? ORDER BY perm_level DESC""",
        (str(guild_id),)).fetchall()
    conn.close()
    return [(r["role_id"], r["perm_level"]) for r in rows]


def role_perm_get_level(guild_id, role_id):
    conn = get_db()
    row = conn.execute("""SELECT perm_level FROM role_perms
        WHERE guild_id = ? AND role_id = ?""",
        (str(guild_id), str(role_id))).fetchone()
    conn.close()
    return int(row["perm_level"]) if row else None


def get_member_perm_level(member):
    """
    Retourne le niveau de perm max d'un membre selon ses rôles.
    0 = aucune perm (simple membre).
    Sys/Buyer sont traités à part (bypass complet dans le check, pas ici).
    """
    if not member or not hasattr(member, "roles"):
        return 0
    conn = get_db()
    guild_id = str(member.guild.id)
    # Récupère tous les rôles-perms de la guild
    rows = conn.execute("""SELECT role_id, perm_level FROM role_perms
        WHERE guild_id = ?""", (guild_id,)).fetchall()
    conn.close()
    if not rows:
        return 0
    configured = {str(r["role_id"]): int(r["perm_level"]) for r in rows}
    member_role_ids = {str(r.id) for r in member.roles}
    max_level = 0
    for rid, lvl in configured.items():
        if rid in member_role_ids and lvl > max_level:
            max_level = lvl
    return max_level


def get_member_perm_role_id(member, target_level):
    """
    Retourne l'ID du rôle du membre qui lui donne ce niveau exact de perm,
    ou None s'il n'en a pas.
    Utilisé pour le derank auto (on retire que le rôle de ce niveau).
    """
    if not member or not hasattr(member, "roles"):
        return None
    conn = get_db()
    rows = conn.execute("""SELECT role_id, perm_level FROM role_perms
        WHERE guild_id = ?""", (str(member.guild.id),)).fetchall()
    conn.close()
    configured = {str(r["role_id"]): int(r["perm_level"]) for r in rows}
    member_role_ids = {str(r.id) for r in member.roles}
    for rid in member_role_ids:
        if rid in configured and configured[rid] == target_level:
            return int(rid)
    return None


# ---- Limites par commande (fenêtre glissante) ----

def get_limits():
    """Retourne le dict {commande: {niveau: [max, fenêtre_min]}}."""
    raw = get_config("limits")
    if not raw:
        return dict(DEFAULT_LIMITS)
    try:
        parsed = json.loads(raw)
        # Les clés niveau sont stockées en string après sérialisation JSON, on reconvertit
        result = {}
        for cmd, levels in parsed.items():
            result[cmd] = {int(k): tuple(v) for k, v in levels.items()}
        return result
    except (json.JSONDecodeError, TypeError, ValueError):
        return dict(DEFAULT_LIMITS)


def set_limit(command, level, max_actions, window_minutes):
    limits = get_limits()
    if command not in limits:
        limits[command] = {}
    limits[command][int(level)] = (int(max_actions), int(window_minutes))
    # On stocke avec les tuples en listes (JSON ne supporte pas les tuples)
    serializable = {cmd: {str(k): list(v) for k, v in lvls.items()}
                    for cmd, lvls in limits.items()}
    set_config("limits", json.dumps(serializable))


def remove_limit(command, level):
    limits = get_limits()
    if command in limits and int(level) in limits[command]:
        del limits[command][int(level)]
        if not limits[command]:
            del limits[command]
        serializable = {cmd: {str(k): list(v) for k, v in lvls.items()}
                        for cmd, lvls in limits.items()}
        set_config("limits", json.dumps(serializable))
        return True
    return False


def get_limit_for(command, level):
    """Retourne (max_actions, window_minutes) pour cette commande à ce niveau, ou None si illimité."""
    limits = get_limits()
    cmd_limits = limits.get(command, {})
    # On cherche le niveau le plus proche en-dessous ou égal
    applicable = None
    for lvl, val in cmd_limits.items():
        if int(lvl) == int(level):
            applicable = val
            break
    # Si pas trouvé exact, on prend la limite du niveau le plus proche en dessous
    if applicable is None:
        candidates = [int(k) for k in cmd_limits.keys() if int(k) <= int(level)]
        if candidates:
            applicable = cmd_limits[max(candidates)]
    return applicable


def record_action(user_id, guild_id, command):
    """Enregistre une action pour le suivi des limites."""
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute("""INSERT INTO action_history (user_id, guild_id, command, created_at)
        VALUES (?, ?, ?, ?)""",
        (str(user_id), str(guild_id), command, now))
    conn.commit()
    conn.close()


def count_recent_actions(user_id, guild_id, command, window_minutes):
    """Compte les actions dans la fenêtre glissante."""
    conn = get_db()
    cutoff = (datetime.now(PARIS_TZ) - timedelta(minutes=window_minutes)).isoformat()
    row = conn.execute("""SELECT COUNT(*) as c FROM action_history
        WHERE user_id = ? AND guild_id = ? AND command = ? AND created_at >= ?""",
        (str(user_id), str(guild_id), command, cutoff)).fetchone()
    conn.close()
    return row["c"] if row else 0


def cleanup_old_actions(days=7):
    """Nettoie les actions de plus de X jours (pour éviter que la table grossisse)."""
    conn = get_db()
    cutoff = (datetime.now(PARIS_TZ) - timedelta(days=days)).isoformat()
    conn.execute("DELETE FROM action_history WHERE created_at < ?", (cutoff,))
    conn.commit()
    conn.close()


# ---- Derank auto ----

def record_derank(user_id, guild_id, role_id, reason):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    cur = conn.execute("""INSERT INTO derank_history
        (user_id, guild_id, role_id, removed_at, reason) VALUES (?, ?, ?, ?, ?)""",
        (str(user_id), str(guild_id), str(role_id), now, reason))
    derank_id = cur.lastrowid
    conn.commit()
    conn.close()
    return derank_id


def record_rerank(derank_id, rerank_by, rerank_reason):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    cur = conn.execute("""UPDATE derank_history
        SET reranked_at = ?, reranked_by = ?, rerank_reason = ?
        WHERE id = ? AND reranked_at IS NULL""",
        (now, str(rerank_by), rerank_reason, derank_id))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def get_last_derank(user_id, guild_id):
    """Retourne le dernier derank non encore rerank pour ce user."""
    conn = get_db()
    row = conn.execute("""SELECT * FROM derank_history
        WHERE user_id = ? AND guild_id = ? AND reranked_at IS NULL
        ORDER BY removed_at DESC LIMIT 1""",
        (str(user_id), str(guild_id))).fetchone()
    conn.close()
    return dict(row) if row else None


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
    return {4: "Buyer", 3: "Sys", 0: "Aucun"}.get(level, "Aucun")


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

def is_sys_or_buyer(user_id):
    return get_rank_db(user_id) >= 3


def can_sanction_target(author_member, target_member_or_id):
    """
    Règles :
    - Sys/Buyer peuvent tout sanctionner (sauf autre Buyer pas protégé).
    - Un membre staff (avec perm) ne peut pas sanctionner quelqu'un de perm égale ou supérieure.
    - Un membre staff ne peut pas sanctionner un Sys/Buyer.
    Retourne (True, None) si OK, (False, error_msg) sinon.
    """
    if hasattr(author_member, "id"):
        author_id = author_member.id
    else:
        author_id = author_member

    # Sys+ bypass complet
    if is_sys_or_buyer(author_id):
        return True, None

    # Target = Sys/Buyer ? Refus.
    target_id = target_member_or_id.id if hasattr(target_member_or_id, "id") else target_member_or_id
    if is_sys_or_buyer(target_id):
        return False, "Tu ne peux pas sanctionner un **Sys** ou **Buyer**."

    # Compare les niveaux de perm si les deux sont des Member
    if hasattr(author_member, "roles") and hasattr(target_member_or_id, "roles"):
        author_perm = get_member_perm_level(author_member)
        target_perm = get_member_perm_level(target_member_or_id)
        if target_perm >= author_perm:
            return False, (
                f"Tu ne peux pas sanctionner quelqu'un de perm **{target_perm}** "
                f"(ton niveau : **{author_perm}**)."
            )
    return True, None


async def check_command_perm(ctx, command):
    """
    Vérifie qu'un user peut utiliser une commande de modération.
    Sys+ bypass. Sinon, il faut avoir un rôle dont le niveau ≥ niveau requis par la commande.
    Retourne (True, None) si OK, (False, error_msg) sinon.
    Les refus sont SILENCIEUX pour les membres lambda (pas de rôle) — bot staff-only.
    """
    # Sys+ bypass
    if is_sys_or_buyer(ctx.author.id):
        return True, None

    required_level = get_cmd_perm(command)
    if required_level is None:
        # Commande inconnue du système, on refuse par sécurité
        return False, "Commande non configurée dans le système de perms."

    member_level = get_member_perm_level(ctx.author)

    # Membre lambda (aucun rôle configuré) : refus silencieux total
    if member_level == 0:
        return False, "__SILENT__"

    if member_level < required_level:
        return False, (
            f"Tu dois avoir **perm {required_level}+** pour utiliser cette commande.\n"
            f"Ton niveau actuel : **perm {member_level}**."
        )
    return True, None


async def check_limit_or_derank(ctx, command):
    """
    Vérifie si l'utilisateur dépasse la limite pour cette commande.
    Si oui : retire le rôle qui lui a donné le niveau + notifie + return False.
    Sys+ bypass complet (pas de limite).
    Retourne True si OK pour procéder, False si bloqué (déjà renvoyé message à l'auteur).
    """
    if is_sys_or_buyer(ctx.author.id):
        return True

    member_level = get_member_perm_level(ctx.author)
    if member_level == 0:
        return False  # ne devrait jamais arriver (check_command_perm bloque avant)

    limit_info = get_limit_for(command, member_level)
    if not limit_info:
        # Pas de limite configurée pour ce niveau → on passe, mais on record quand même
        record_action(ctx.author.id, ctx.guild.id, command)
        return True

    max_actions, window_minutes = limit_info
    recent_count = count_recent_actions(ctx.author.id, ctx.guild.id, command, window_minutes)

    # Si on est à la limite (ou au-dessus), on declenche le derank
    if recent_count >= max_actions:
        await trigger_auto_derank(ctx, command, member_level, max_actions, window_minutes, recent_count)
        return False

    # Record + on continue
    record_action(ctx.author.id, ctx.guild.id, command)
    return True


async def trigger_auto_derank(ctx, command, member_level, max_actions, window_minutes, attempted_count):
    """
    Applique le derank : retire le rôle correspondant au niveau de perm atteint.
    Notifie le staff via DM + log.
    """
    # Trouve le rôle exact au niveau où il a dépassé
    role_id = get_member_perm_role_id(ctx.author, member_level)
    role = ctx.guild.get_role(role_id) if role_id else None

    reason_text = (
        f"Dépassement de limite sur `{command}` "
        f"({attempted_count + 1}e tentative, max autorisé : {max_actions} en {window_minutes}min)"
    )

    removed = False
    if role and role in ctx.author.roles:
        try:
            await ctx.author.remove_roles(role, reason=f"Derank auto : {reason_text}")
            removed = True
        except (discord.Forbidden, discord.HTTPException) as e:
            log.warning(f"Derank auto: retrait rôle {role.id} échoué : {e}")

    # Enregistrer en DB
    if role_id:
        derank_id = record_derank(ctx.author.id, ctx.guild.id, role_id, reason_text)
    else:
        derank_id = None

    # Message dans le salon
    em = error_embed(
        "⛔ Derank automatique",
        f"{ctx.author.mention} a dépassé la limite autorisée pour **`{command}`**.\n"
        f"**Limite :** {max_actions} en {window_minutes}min\n"
        f"**Tentatives dans la fenêtre :** {attempted_count + 1}\n\n"
        + (f"➡️ Le rôle {role.mention} lui a été retiré.\n" if removed and role else
           (f"➡️ Rôle à retirer manuellement.\n" if not removed else ""))
        + f"Un Sys+ peut le rerank via `{get_prefix_cached()}rerank @user <motif>`."
    )
    try:
        await ctx.send(embed=em)
    except discord.HTTPException:
        pass

    # DM au staff derank
    try:
        dm_em = discord.Embed(
            title="⛔ Tu as été derank automatiquement",
            description=(
                f"Sur le serveur **{ctx.guild.name}**.\n\n"
                f"**Commande :** `{command}`\n"
                f"**Limite :** {max_actions} actions en {window_minutes} minutes\n"
                f"**Motif :** Dépassement de limite\n\n"
                f"Ton rôle **{role.name if role else 'staff'}** a été retiré."
                + (f"\n\nDemande à un Sys+ de justifier ton acte pour être rerank."
                   if role else "")
            ),
            color=0xf04747,
        )
        dm_em.set_footer(text="Sanction ・ Meira")
        await ctx.author.send(embed=dm_em)
    except (discord.Forbidden, discord.HTTPException):
        pass

    # Log
    await send_log(
        ctx.guild, "🚨 DERANK AUTO", ctx.author, ctx.author, ctx.author.id,
        desc=(f"Rôle retiré : {role.mention if role else 'introuvable'}\n"
              f"ID derank : `#{derank_id}`" if derank_id else ""),
        reason=reason_text,
        color=0xf04747,
    )


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


# ========================= SYSTÈME DE PERMS (setperm / setcmdperm / setlimit / rerank) =========================

@bot.command(name="setperm")
async def _setperm(ctx, role: discord.Role = None, level: int = None):
    """Attribue un niveau de perm (1-9) à un rôle Discord."""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if role is None or level is None:
        return await ctx.send(embed=error_embed(
            "Usage",
            f"`{get_prefix_cached()}setperm <@role> <1-{MAX_PERM_LEVEL}>`\n"
            f"Ex : `{get_prefix_cached()}setperm @Staff 3`"
        ))
    if level < 1 or level > MAX_PERM_LEVEL:
        return await ctx.send(embed=error_embed(
            "❌ Niveau invalide",
            f"Le niveau doit être entre **1** et **{MAX_PERM_LEVEL}**."
        ))
    role_perm_add(ctx.guild.id, role.id, level, ctx.author.id)
    await ctx.send(embed=success_embed(
        "✅ Perm attribuée",
        f"{role.mention} → **perm {level}**\n"
        f"Les membres avec ce rôle peuvent utiliser toutes les commandes de perm ≤ **{level}**."
    ))
    await send_log(ctx.guild, "Perm attribuée", ctx.author,
                   desc=f"{role.mention} → perm {level}", color=0x43b581)


@bot.command(name="unsetperm")
async def _unsetperm(ctx, role: discord.Role = None):
    """Retire le niveau de perm d'un rôle."""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if role is None:
        return await ctx.send(embed=error_embed("Usage", f"`{get_prefix_cached()}unsetperm <@role>`"))
    if not role_perm_remove(ctx.guild.id, role.id):
        return await ctx.send(embed=error_embed("Pas de perm", f"{role.mention} n'avait pas de perm."))
    await ctx.send(embed=success_embed("✅ Perm retirée", f"{role.mention} n'a plus de perm."))
    await send_log(ctx.guild, "Perm retirée", ctx.author,
                   desc=role.mention, color=0xfaa61a)


@bot.command(name="perms")
async def _perms(ctx):
    """Liste tous les rôles configurés avec leurs niveaux."""
    if not has_min_rank(ctx.author.id, 3) and get_member_perm_level(ctx.author) == 0:
        return  # refus silencieux
    rows = role_perm_list(ctx.guild.id)
    if not rows:
        return await ctx.send(embed=info_embed(
            "🎚️ Aucune perm configurée",
            f"Aucun rôle n'a de niveau de perm.\n"
            f"Utilise `{get_prefix_cached()}setperm @role <1-9>`."
        ))
    by_level = {}
    for role_id, level in rows:
        by_level.setdefault(level, []).append(role_id)
    lines = []
    for lvl in sorted(by_level.keys(), reverse=True):
        role_mentions = []
        for rid in by_level[lvl]:
            role = ctx.guild.get_role(int(rid))
            role_mentions.append(role.mention if role else f"*Rôle supprimé* (`{rid}`)")
        lines.append(f"**Perm {lvl}** : {', '.join(role_mentions)}")
    em = info_embed(f"🎚️ Rôles configurés ({len(rows)})", "\n".join(lines))
    em.set_footer(text="Sanction ・ Plus le niveau est élevé, plus de commandes sont accessibles")
    await ctx.send(embed=em)


@bot.command(name="setcmdperm")
async def _setcmdperm(ctx, command: str = None, level: int = None):
    """Définit le niveau de perm requis pour une commande."""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if command is None or level is None:
        cp = get_cmd_perms()
        cmd_list = ", ".join(f"`{c}`" for c in sorted(cp.keys()))
        return await ctx.send(embed=error_embed(
            "Usage",
            f"`{get_prefix_cached()}setcmdperm <commande> <1-{MAX_PERM_LEVEL}>`\n"
            f"Ex : `{get_prefix_cached()}setcmdperm ban 4`\n\n"
            f"Commandes gérées : {cmd_list}"
        ))
    command = command.lower().strip().lstrip("-").lstrip(get_prefix_cached())
    if command not in get_cmd_perms():
        return await ctx.send(embed=error_embed(
            "❌ Commande inconnue",
            "Cette commande n'est pas gérée par le système de perms.\n"
            f"Voir `{get_prefix_cached()}cmdperms` pour la liste."
        ))
    if level < 1 or level > MAX_PERM_LEVEL:
        return await ctx.send(embed=error_embed(
            "❌ Niveau invalide",
            f"Le niveau doit être entre **1** et **{MAX_PERM_LEVEL}**."
        ))
    set_cmd_perm(command, level)
    await ctx.send(embed=success_embed(
        "✅ Commande reclassée",
        f"`{command}` → **perm {level}**"
    ))
    await send_log(ctx.guild, "Cmd perm modifiée", ctx.author,
                   desc=f"`{command}` → perm {level}", color=0x43b581)


@bot.command(name="cmdperms")
async def _cmdperms(ctx):
    """Liste toutes les commandes avec leur niveau de perm."""
    if not has_min_rank(ctx.author.id, 3) and get_member_perm_level(ctx.author) == 0:
        return  # refus silencieux
    cp = get_cmd_perms()
    if not cp:
        return await ctx.send(embed=info_embed("Aucune commande configurée", "Rien à afficher."))

    by_level = {}
    for cmd, lvl in cp.items():
        by_level.setdefault(lvl, []).append(cmd)

    lines = []
    for lvl in sorted(by_level.keys()):
        cmds_sorted = sorted(by_level[lvl])
        lines.append(f"**Perm {lvl}** : `" + "` · `".join(cmds_sorted) + "`")

    em = info_embed("🎚️ Commandes par niveau", "\n".join(lines))
    em.set_footer(text=f"Sanction ・ Modifie via {get_prefix_cached()}setcmdperm <cmd> <niveau>")
    await ctx.send(embed=em)


@bot.command(name="setlimit")
async def _setlimit(ctx, command: str = None, level: int = None,
                    max_actions: int = None, window_minutes: int = None):
    """Définit une limite pour une commande à un niveau de perm."""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if command is None or level is None or max_actions is None or window_minutes is None:
        return await ctx.send(embed=error_embed(
            "Usage",
            f"`{get_prefix_cached()}setlimit <commande> <niveau> <max_actions> <minutes>`\n\n"
            f"Ex : `{get_prefix_cached()}setlimit ban 4 3 20` → au niveau 4, max 3 bans / 20min"
        ))
    command = command.lower().strip().lstrip("-").lstrip(get_prefix_cached())
    if command not in get_cmd_perms():
        return await ctx.send(embed=error_embed(
            "❌ Commande inconnue",
            f"Voir `{get_prefix_cached()}cmdperms`."
        ))
    if level < 1 or level > MAX_PERM_LEVEL:
        return await ctx.send(embed=error_embed(
            "❌ Niveau invalide", f"Entre 1 et {MAX_PERM_LEVEL}."))
    if max_actions < 1 or max_actions > 1000:
        return await ctx.send(embed=error_embed("❌ Max invalide", "Entre 1 et 1000."))
    if window_minutes < 1 or window_minutes > 10080:
        return await ctx.send(embed=error_embed("❌ Fenêtre invalide", "Entre 1 et 10080 min (7j)."))

    set_limit(command, level, max_actions, window_minutes)
    await ctx.send(embed=success_embed(
        "✅ Limite configurée",
        f"`{command}` au niveau **{level}** : max **{max_actions}** actions / **{window_minutes}min**"
    ))
    await send_log(ctx.guild, "Limite modifiée", ctx.author,
                   desc=f"`{command}` lvl {level} → {max_actions}/{window_minutes}min",
                   color=0x43b581)


@bot.command(name="unsetlimit")
async def _unsetlimit(ctx, command: str = None, level: int = None):
    """Retire la limite d'une commande à un niveau (illimité)."""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if command is None or level is None:
        return await ctx.send(embed=error_embed(
            "Usage",
            f"`{get_prefix_cached()}unsetlimit <commande> <niveau>`"
        ))
    command = command.lower().strip().lstrip("-").lstrip(get_prefix_cached())
    if not remove_limit(command, level):
        return await ctx.send(embed=error_embed("Pas de limite", f"Pas de limite pour `{command}` au niveau {level}."))
    await ctx.send(embed=success_embed("✅ Limite retirée", f"`{command}` au niveau {level} : illimité."))


@bot.command(name="limits")
async def _limits(ctx):
    """Affiche toutes les limites configurées."""
    if not has_min_rank(ctx.author.id, 3) and get_member_perm_level(ctx.author) == 0:
        return  # silencieux
    limits = get_limits()
    if not limits:
        return await ctx.send(embed=info_embed("Aucune limite", "Aucune limite configurée."))

    lines = []
    for cmd in sorted(limits.keys()):
        for lvl in sorted(limits[cmd].keys()):
            max_a, window = limits[cmd][lvl]
            lines.append(f"`{cmd}` ・ perm **{lvl}** → **{max_a}** / **{window}min**")

    em = info_embed("⏱️ Limites par commande et niveau", "\n".join(lines))
    em.set_footer(text=f"Sanction ・ Sys/Buyer bypass toutes les limites")
    await ctx.send(embed=em)


@bot.command(name="mylimits")
async def _mylimits(ctx):
    """Affiche les quotas restants du staff pour chaque commande."""
    # Bypass Sys+
    if is_sys_or_buyer(ctx.author.id):
        return await ctx.send(embed=info_embed(
            "⏱️ Tes limites",
            "Tu es **Sys** ou **Buyer** : aucune limite ne s'applique à toi."
        ))

    member_level = get_member_perm_level(ctx.author)
    if member_level == 0:
        return  # silencieux (pas staff)

    limits = get_limits()
    cmd_perms = get_cmd_perms()

    # Pour chaque commande accessible à son niveau, check sa consommation actuelle
    accessible = [(cmd, lvl) for cmd, lvl in cmd_perms.items() if lvl <= member_level]
    lines = []
    for cmd, required_lvl in sorted(accessible):
        limit_info = get_limit_for(cmd, member_level)
        if not limit_info:
            continue  # pas de limite → pas intéressant d'afficher
        max_a, window = limit_info
        used = count_recent_actions(ctx.author.id, ctx.guild.id, cmd, window)
        remaining = max(0, max_a - used)
        bar = "🟢" if remaining > max_a * 0.5 else ("🟡" if remaining > 0 else "🔴")
        lines.append(f"{bar} `{cmd}` : **{remaining}/{max_a}** restants (fenêtre {window}min)")

    if not lines:
        return await ctx.send(embed=info_embed(
            "⏱️ Tes limites",
            f"Aucune limite active sur les commandes de ton niveau (**perm {member_level}**)."
        ))

    em = info_embed(f"⏱️ Tes quotas — perm {member_level}", "\n".join(lines))
    em.set_footer(text="Sanction ・ Attention : dépasser une limite = derank automatique")
    await ctx.send(embed=em)


@bot.command(name="rerank")
async def _rerank(ctx, *, args: str = None):
    """Rerank un staff derank automatiquement. Usage : -rerank @user <motif justifiant>"""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not args:
        return await ctx.send(embed=error_embed(
            "Usage",
            f"`{get_prefix_cached()}rerank <@user> <motif>`\n\n"
            "Le motif doit justifier pourquoi le staff est réhabilité (min 10 caractères)."
        ))

    # Parse : premier mot = user, reste = motif
    parts = args.strip().split(maxsplit=1)
    if len(parts) < 2:
        return await ctx.send(embed=error_embed(
            "❌ Motif manquant",
            f"Usage : `{get_prefix_cached()}rerank <@user> <motif>`"
        ))
    user_part, motif = parts
    motif = motif.strip()
    if len(motif) < 10:
        return await ctx.send(embed=error_embed(
            "❌ Motif trop court",
            "Le motif doit faire au moins **10 caractères** pour justifier le rerank."
        ))

    display, uid = await resolve_user_or_id(ctx, user_part)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    # Check derniers derank non rerank
    last = get_last_derank(uid, ctx.guild.id)
    if not last:
        return await ctx.send(embed=error_embed(
            "❌ Aucun derank à rerank",
            f"{format_user_display(display, uid)} n'a pas de derank en attente de rerank."
        ))

    # Rétablir le rôle
    role_id = int(last["role_id"])
    role = ctx.guild.get_role(role_id)
    target_member = ctx.guild.get_member(uid)

    if not target_member:
        return await ctx.send(embed=error_embed(
            "❌ Membre absent",
            "Le staff n'est pas sur le serveur. Il faut qu'il revienne d'abord."
        ))

    if not role:
        return await ctx.send(embed=error_embed(
            "❌ Rôle supprimé",
            f"Le rôle initial (`{role_id}`) n'existe plus sur le serveur."
        ))

    try:
        await target_member.add_roles(role, reason=f"Rerank par {ctx.author} : {motif}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed("❌ Permission manquante", "Je ne peux pas ajouter ce rôle."))
    except discord.HTTPException as e:
        return await ctx.send(embed=error_embed("❌ Erreur Discord", str(e)))

    record_rerank(last["id"], ctx.author.id, motif)

    await ctx.send(embed=success_embed(
        "✅ Rerank effectué",
        f"{format_user_display(display, uid)} a récupéré le rôle {role.mention}.\n"
        f"**Motif :** {motif}"
    ))
    await send_log(
        ctx.guild, "Rerank", ctx.author, display, uid,
        desc=f"Rôle restauré : {role.mention}\nDerank initial : #{last['id']}",
        reason=motif, color=0x43b581,
    )

    # DM le staff
    try:
        em = discord.Embed(
            title="✅ Tu as été rerank",
            description=(
                f"Sur le serveur **{ctx.guild.name}**.\n\n"
                f"**Par :** {ctx.author.mention}\n"
                f"**Motif :** {motif}\n\n"
                f"Ton rôle **{role.name}** t'a été restauré."
            ),
            color=0x43b581,
        )
        em.set_footer(text="Sanction ・ Meira")
        await target_member.send(embed=em)
    except (discord.Forbidden, discord.HTTPException):
        pass


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
    ok, err_perm = await check_command_perm(ctx, "warn")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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

    target_member = ctx.guild.get_member(uid)
    allowed, err = can_sanction_target(ctx.author, target_member or uid)
    if not allowed:
        return await ctx.send(embed=error_embed("❌ Permission refusée", err))

    # Check limite + derank auto si dépassement
    if not await check_limit_or_derank(ctx, "warn"):
        return

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
    ok, err_perm = await check_command_perm(ctx, "unwarn")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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

    # Un staff ne peut unwarn que ses propres warns (sauf Sys+)
    if not is_sys_or_buyer(ctx.author.id) and str(sanc["moderator_id"]) != str(ctx.author.id):
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
    if await check_bot_ban(ctx):
        return
    ok, err_perm = await check_command_perm(ctx, "clearwarns")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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
    ok, err_perm = await check_command_perm(ctx, "mute")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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

    # Limite absolue Discord : 28 jours
    if seconds > 60 * 60 * 24 * 28:
        return await ctx.send(embed=error_embed(
            "❌ Durée trop longue",
            "La limite Discord pour un timeout est de **28 jours**."
        ))

    ok_reason, reason_or_err = validate_reason(reason)
    if not ok_reason:
        return await ctx.send(embed=error_embed("❌ Raison requise", reason_or_err + f"\nUsage : `{get_prefix_cached()}mute @user <durée> <raison détaillée>`"))
    reason = reason_or_err

    target_member = ctx.guild.get_member(uid)
    if not target_member:
        return await ctx.send(embed=error_embed(
            "❌ Membre absent",
            f"{format_user_display(display, uid)} n'est pas sur le serveur. On ne peut pas mute quelqu'un qui n'est pas là."
        ))

    allowed, err = can_sanction_target(ctx.author, target_member)
    if not allowed:
        return await ctx.send(embed=error_embed("❌ Permission refusée", err))

    # Check limite + derank
    if not await check_limit_or_derank(ctx, "mute"):
        return

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
    ok, err_perm = await check_command_perm(ctx, "unmute")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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
    ok, err_perm = await check_command_perm(ctx, "vmute")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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

    ok_reason, reason_or_err = validate_reason(reason)
    if not ok_reason:
        return await ctx.send(embed=error_embed("❌ Raison requise", reason_or_err))
    reason = reason_or_err

    target_member = ctx.guild.get_member(uid)
    if not target_member:
        return await ctx.send(embed=error_embed("❌ Membre absent", "Ce membre n'est pas sur le serveur."))

    allowed, err = can_sanction_target(ctx.author, target_member)
    if not allowed:
        return await ctx.send(embed=error_embed("❌ Permission refusée", err))

    if not await check_limit_or_derank(ctx, "vmute"):
        return

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
    ok, err_perm = await check_command_perm(ctx, "unvmute")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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
    ok, err_perm = await check_command_perm(ctx, "kick")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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

    ok_reason, reason_or_err = validate_reason(reason)
    if not ok_reason:
        return await ctx.send(embed=error_embed("❌ Raison requise", reason_or_err))
    reason = reason_or_err

    target_member = ctx.guild.get_member(uid)
    if not target_member:
        return await ctx.send(embed=error_embed(
            "❌ Membre absent",
            "Ce membre n'est pas sur le serveur. Il n'y a rien à kick."
        ))

    allowed, err = can_sanction_target(ctx.author, target_member)
    if not allowed:
        return await ctx.send(embed=error_embed("❌ Permission refusée", err))

    if not await check_limit_or_derank(ctx, "kick"):
        return

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
    ok, err_perm = await check_command_perm(ctx, "ban")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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

    ok_reason, reason_or_err = validate_reason(reason)
    if not ok_reason:
        return await ctx.send(embed=error_embed("❌ Raison requise", reason_or_err))
    reason = reason_or_err

    # DM avant ban (si membre présent)
    target_member = ctx.guild.get_member(uid)

    allowed, err = can_sanction_target(ctx.author, target_member or uid)
    if not allowed:
        return await ctx.send(embed=error_embed("❌ Permission refusée", err))

    if not await check_limit_or_derank(ctx, "ban"):
        return

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
    ok, err_perm = await check_command_perm(ctx, "unban")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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
    ok, err_perm = await check_command_perm(ctx, "unsanction")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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

    # Un staff ne peut unsanction que ses propres sanctions (sauf Sys+)
    if not is_sys_or_buyer(ctx.author.id) and str(sanc["moderator_id"]) != str(ctx.author.id):
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
    """Affiche le casier d'un membre (bot staff-only)."""
    if await check_bot_ban(ctx):
        return

    # Bot staff-only
    ok, err_perm = await check_command_perm(ctx, "casier")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))

    if user_input is None:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}casier <@user|id|nom>`"
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
    ok, err_perm = await check_command_perm(ctx, "sanction")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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
    """Wipe complet du casier d'un membre. Action drastique."""
    if await check_bot_ban(ctx):
        return
    ok, err_perm = await check_command_perm(ctx, "resetcasier")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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

@bot.command(name="clear", aliases=["purge"])
async def _clear(ctx, count: int = None, *, user_input: str = None):
    """
    Supprime les N derniers messages. Optionnel : filtrer par user.
    -clear 50          → 50 derniers messages
    -clear 50 @user    → 50 derniers messages de @user
    """
    if await check_bot_ban(ctx):
        return
    ok, err_perm = await check_command_perm(ctx, "clear")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
    if count is None or count <= 0:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}clear <nombre> [@user]`"
        ))

    # Plafond max pour éviter les catastrophes (200 messages max pour tous, Sys+ exclu)
    if not is_sys_or_buyer(ctx.author.id) and count > 200:
        return await ctx.send(embed=error_embed(
            "❌ Limite de sécurité",
            "Max **200 messages** par commande pour éviter les accidents. Sys+ bypass."
        ))

    if not await check_limit_or_derank(ctx, "clear"):
        return

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
    ok, err_perm = await check_command_perm(ctx, "lock")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))

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
    ok, err_perm = await check_command_perm(ctx, "unlock")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))

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
    ok, err_perm = await check_command_perm(ctx, "slowmode")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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
    ok, err_perm = await check_command_perm(ctx, "note")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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
    ok, err_perm = await check_command_perm(ctx, "notes")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
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
    ok, err_perm = await check_command_perm(ctx, "delnote")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))
    if note_id is None:
        return await ctx.send(embed=error_embed("Argument manquant", f"Usage : `{get_prefix_cached()}delnote <id>`"))

    if not delete_staff_note(note_id):
        return await ctx.send(embed=error_embed("❌ Note introuvable", f"Aucune note avec l'ID `#{note_id}`."))

    await ctx.send(embed=success_embed("✅ Note supprimée", f"Note `#{note_id}` supprimée."))
    await send_log(ctx.guild, "Note supprimée", ctx.author,
                   desc=f"Note `#{note_id}`", color=0xe67e22)


# ========================= APPELS =========================

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
    # Sys+ uniquement (pas dans le système de perms, reste admin)
    if not has_min_rank(ctx.author.id, 3):
        if get_member_perm_level(ctx.author) == 0:
            return  # silencieux
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis pour l'anti-raid."))

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
        if get_member_perm_level(ctx.author) == 0:
            return  # silencieux
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis pour l'escalation."))

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
    """Stats de modération d'un staff (soi-même par défaut)."""
    if await check_bot_ban(ctx):
        return
    ok, err_perm = await check_command_perm(ctx, "modstats")
    if not ok:
        if err_perm == "__SILENT__":
            return
        return await ctx.send(embed=error_embed("❌ Permission refusée", err_perm))

    if user_input is None:
        target = ctx.author
        uid = ctx.author.id
    else:
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


# ========================= HELP (STAFF-ONLY) =========================
#
# Deux commandes :
#  -help    → vue d'ensemble catégorisée (comme avant, mais staff-only)
#  -helpall → vue paginée par niveau de perm (boutons ← →)
#
# Membre lambda sans rôle → refus silencieux total


# --- Mapping des commandes aux catégories pour le -help standard ---
# Les items du help sont DYNAMIQUES : la perm affichée à droite vient de get_cmd_perms()
HELP_CATEGORIES_V2 = {
    "moderation": {
        "emoji": "⚠️",
        "label": "Modération",
        "title": "⚠️  Modération",
        "commands": [
            ("warn @user <raison>",            "Avertir (déclenche escalation)", "warn"),
            ("mute @user <durée> <raison>",    "Mute textuel (timeout Discord)", "mute"),
            ("vmute @user <durée> <raison>",   "Mute vocal",                     "vmute"),
            ("unmute @user",                   "Retirer un mute",                "unmute"),
            ("unvmute @user",                  "Retirer un vmute",               "unvmute"),
            ("kick @user <raison>",            "Expulser du serveur",            "kick"),
            ("ban @user <durée|perm> <r>",     "Bannir (temp ou perm)",          "ban"),
            ("unban <id|mention> [raison]",    "Débannir",                       "unban"),
            ("unwarn <id> [raison]",           "Annuler un warn",                "unwarn"),
            ("unsanction <id> [raison]",       "Annuler toute sanction",         "unsanction"),
            ("clearwarns @user",               "Effacer tous les warns",         "clearwarns"),
            ("resetcasier @user",              "Wipe complet du casier",         "resetcasier"),
        ],
    },
    "casier": {
        "emoji": "📋",
        "label": "Casier",
        "title": "📋  Casier & info",
        "commands": [
            ("casier @user",                   "Voir le casier d'un membre",     "casier"),
            ("sanction <id>",                  "Détail d'une sanction",          "sanction"),
            ("notes @user",                    "Lire les notes staff",           "notes"),
            ("note @user <contenu>",           "Ajouter une note staff",         "note"),
            ("delnote <id>",                   "Supprimer une note",             "delnote"),
            ("modstats [@user]",               "Stats de modération",            "modstats"),
        ],
    },
    "utilitaires": {
        "emoji": "🛠️",
        "label": "Utilitaires",
        "title": "🛠️  Utilitaires",
        "commands": [
            ("clear <n> [@user]",              "Supprimer n messages (ex-purge)","clear"),
            ("slowmode <durée> [#salon]",      "Slowmode (0 pour off)",          "slowmode"),
            ("lock [#salon] <raison>",         "Verrouiller un salon",           "lock"),
            ("unlock [#salon]",                "Déverrouiller",                  "unlock"),
        ],
    },
    "mes_limites": {
        "emoji": "⏱️",
        "label": "Mes limites",
        "title": "⏱️  Mes limites",
        "commands": [
            ("mylimits",                       "Voir tes quotas restants",       "mylimits"),
        ],
    },
    "perms_admin": {
        "emoji": "🎚️",
        "label": "Perms (Sys+)",
        "title": "🎚️  Système de perms — Sys+",
        "sys_only": True,
        "commands_flat": [
            ("setperm @role <1-9>",             "Attribuer un niveau de perm à un rôle"),
            ("unsetperm @role",                 "Retirer le niveau d'un rôle"),
            ("perms",                           "Liste des rôles configurés"),
            ("setcmdperm <cmd> <niveau>",       "Ranger une commande dans un niveau"),
            ("cmdperms",                        "Liste des commandes par niveau"),
            ("setlimit <cmd> <lvl> <max> <min>","Limite par commande et niveau"),
            ("unsetlimit <cmd> <lvl>",          "Retirer une limite (illimité)"),
            ("limits",                          "Voir toutes les limites"),
            ("rerank @user <motif>",            "Rerank après dépassement (motif 10+)"),
        ],
    },
    "config": {
        "emoji": "⚙️",
        "label": "Config (Sys+)",
        "title": "⚙️  Configuration — Sys+",
        "sys_only": True,
        "commands_flat": [
            ("allow #salon",                    "Autoriser un salon"),
            ("unallow #salon",                  "Retirer un salon autorisé"),
            ("allow",                           "Lister les salons autorisés"),
            ("antiraid [on|off|threshold|...]", "Config anti-raid"),
            ("escalation [reset|clear]",        "Config auto-escalation"),
            ("botban @u / botunban @u",         "Ban/unban du bot"),
        ],
    },
    "buyer": {
        "emoji": "👑",
        "label": "Buyer",
        "title": "👑  Buyer (config ultime)",
        "buyer_only": True,
        "commands_flat": [
            ("sys @u / unsys @u",               "Gérer les Sys"),
            ("setlog #salon",                   "Salon de logs"),
            ("prefix [nouveau]",                "Changer le prefix"),
        ],
    },
    "hierarchy": {
        "emoji": "📋",
        "label": "Hiérarchie",
        "title": "📋  Hiérarchie & fonctionnement",
        "always_visible_for_staff": True,
        "commands": [],
    },
}


def user_access_cmd(ctx_author, command):
    """Retourne True si le user peut accéder à cette commande (sans check des limites)."""
    if is_sys_or_buyer(ctx_author.id):
        return True
    required = get_cmd_perm(command)
    if required is None:
        return False
    return get_member_perm_level(ctx_author) >= required


def help_v2_accessible_items(category_key, ctx_author):
    """Retourne les items accessibles à un user pour cette catégorie."""
    cat = HELP_CATEGORIES_V2.get(category_key, {})
    items = []

    # Catégories admin (sys_only/buyer_only/commands_flat) : on check par rang
    if cat.get("sys_only"):
        if not is_sys_or_buyer(ctx_author.id):
            return []
        return [(syntax, desc) for syntax, desc in cat.get("commands_flat", [])]
    if cat.get("buyer_only"):
        if get_rank_db(ctx_author.id) < 4:
            return []
        return [(syntax, desc) for syntax, desc in cat.get("commands_flat", [])]

    # Catégories standard (commands avec clé de cmd_perm)
    for entry in cat.get("commands", []):
        syntax, desc, cmd_key = entry
        if user_access_cmd(ctx_author, cmd_key):
            items.append((syntax, desc))
    return items


def help_v2_category_visible(category_key, ctx_author):
    cat = HELP_CATEGORIES_V2.get(category_key, {})
    if cat.get("always_visible_for_staff"):
        # Visible si staff
        return is_sys_or_buyer(ctx_author.id) or get_member_perm_level(ctx_author) > 0
    return len(help_v2_accessible_items(category_key, ctx_author)) > 0


def _sanction_apply_thumbnail(em, ctx_author):
    """Ajoute l'icône du serveur en thumbnail si dispo."""
    guild = getattr(ctx_author, "guild", None)
    if guild and getattr(guild, "icon", None):
        try:
            em.set_thumbnail(url=guild.icon.url)
        except (AttributeError, TypeError):
            pass


# Sous-titres par catégorie Sanction (nouveau)
CATEGORY_SUBTITLES = {
    "moderation":   "Avertir, mute, kick, ban et gestion des sanctions.",
    "casier":       "Consulter les casiers et les notes staff.",
    "utilitaires":  "Purge, slowmode, lock et autres outils rapides.",
    "mes_limites":  "Suivi personnel de tes quotas de commandes.",
    "perms_admin":  "Configuration fine des niveaux de permission.",
    "config":       "Anti-raid, escalation, salons autorisés.",
    "buyer":        "Config ultime — réservée au Buyer.",
}


def build_help_category_embed_v2(category_key, ctx_author):
    p = get_prefix_cached()
    cat = HELP_CATEGORIES_V2[category_key]

    if category_key == "hierarchy":
        return build_help_hierarchy_embed_v2(ctx_author)

    # Titre sans l'emoji devant (on le remet proprement)
    title_clean = cat["title"]
    # Enlever le double-espace et l'emoji au début si présent
    for em_char in ["⚠️", "📋", "🛠️", "⏱️", "🎚️", "⚙️", "👑"]:
        if title_clean.startswith(em_char):
            title_clean = title_clean[len(em_char):].strip()
            break

    emoji = cat.get("emoji", "📋")
    subtitle = CATEGORY_SUBTITLES.get(category_key, "")

    em = discord.Embed(
        title=f"{emoji}  {title_clean}",
        description=subtitle if subtitle else None,
        color=embed_color(),
    )
    _sanction_apply_thumbnail(em, ctx_author)

    items = help_v2_accessible_items(category_key, ctx_author)
    if not items:
        em.add_field(
            name="⛔ Aucune commande accessible",
            value="Ton niveau de perm est trop bas pour cette catégorie.",
            inline=False,
        )
    else:
        # Une ligne par commande, format `{prefix}{syntax}` — description
        lines = [f"`{p}{syntax}` — {desc}" for syntax, desc in items]
        # Si beaucoup de commandes, on split en 2 fields pour éviter la limite 1024 chars
        half = (len(lines) + 1) // 2
        if len(lines) > 8:
            em.add_field(name="Commandes", value="\n".join(lines[:half]), inline=False)
            em.add_field(name="\u200b", value="\n".join(lines[half:]), inline=False)
        else:
            em.add_field(name="Commandes", value="\n".join(lines), inline=False)

    # Astuce pour la catégorie modération
    if category_key == "moderation":
        em.add_field(
            name="💡 Astuce",
            value=(
                f"Les warns déclenchent une **escalation automatique** "
                f"(mute → kick → ban) selon la config `{p}escalation`."
            ),
            inline=False,
        )

    em.set_footer(text="Sanction ・ Meira")
    return em


def build_help_hierarchy_embed_v2(ctx_author):
    em = discord.Embed(
        title="📋  Hiérarchie & fonctionnement",
        description="Les rangs du bot et le système de permissions.",
        color=embed_color(),
    )
    _sanction_apply_thumbnail(em, ctx_author)

    rank = get_rank_db(ctx_author.id)
    member_perm = get_member_perm_level(ctx_author)

    # Rangs DB
    rangs_desc = []
    if rank == 4:
        rangs_desc.append("👑 **Buyer**  ← toi — Config ultime, bypass total")
    else:
        rangs_desc.append("👑 **Buyer** — Config ultime, bypass total")
    if rank == 3:
        rangs_desc.append("🔧 **Sys**  ← toi — Bypass tout, gère les perms")
    else:
        rangs_desc.append("🔧 **Sys** — Bypass tout, gère les perms")
    rangs_desc.append("🛡️ **Staff** — Rôles + niveaux de perm configurés")

    em.add_field(
        name="🏛️ Rangs du bot",
        value="\n".join(rangs_desc),
        inline=False,
    )

    # Niveaux de perm
    em.add_field(
        name="🎚️ Niveaux de permission (1-9)",
        value=(
            "Les **rôles Discord** sont assignés à un niveau par les Sys+.\n"
            "Chaque **commande** a un niveau requis.\n"
            "Avoir un rôle perm X → accès à toutes les commandes de perm **≤ X**."
        ),
        inline=False,
    )

    # Ton niveau perso
    if rank < 3 and member_perm > 0:
        em.add_field(
            name=f"🎯 Ton niveau actuel : perm {member_perm}",
            value=(
                f"Tu peux utiliser toutes les commandes de perm 1 à {member_perm}.\n"
                f"Tape `{get_prefix_cached()}helpall` pour voir ce que tu peux faire."
            ),
            inline=False,
        )
    elif rank >= 3:
        em.add_field(
            name=f"🎯 Ton rang : {rank_name(rank)}",
            value="Bypass total — pas de limite, pas de niveau requis.",
            inline=False,
        )

    # Limites & derank auto
    em.add_field(
        name="⏱️ Limites & derank automatique",
        value=(
            "Chaque commande a une limite **X actions / Y minutes**.\n"
            "Dépasser = **retrait automatique du rôle perm concerné** + DM.\n"
            "Demande un `rerank` à un Sys+ en justifiant ton acte.\n"
            "Sys/Buyer ne sont **jamais** limités."
        ),
        inline=False,
    )

    em.set_footer(text="Sanction ・ Meira")
    return em


def build_help_home_embed_v2(ctx_author):
    p = get_prefix_cached()
    rank = get_rank_db(ctx_author.id)
    member_perm = get_member_perm_level(ctx_author)

    if is_sys_or_buyer(ctx_author.id):
        status_line = f"**Ton rang :** {rank_name(rank)} ・ Accès total (bypass)"
    elif member_perm > 0:
        status_line = f"**Ton niveau de perm :** {member_perm} / {MAX_PERM_LEVEL}"
    else:
        status_line = "**Aucun accès au bot.**"

    em = discord.Embed(
        title="🛡️  Panel d'aide — Sanction",
        description=(
            f"Bot de **modération staff-only** pour Meira.\n"
            f"**Prefix :** `{p}` ・ {status_line}\n\n"
            f"*Choisis une catégorie ci-dessous pour voir ses commandes.*"
        ),
        color=embed_color(),
    )
    _sanction_apply_thumbnail(em, ctx_author)

    category_descs = {
        "moderation":  "Warn, mute, kick, ban, unsanction",
        "casier":      "Casier, sanctions, notes, modstats",
        "utilitaires": "Clear, lock, unlock, slowmode",
        "mes_limites": "Tes quotas restants",
        "perms_admin": "Gérer le système de perms (Sys+)",
        "config":      "Anti-raid, escalation, salons (Sys+)",
        "buyer":       "Prefix, setlog, sys/unsys (Buyer)",
        "hierarchy":   "Comment fonctionne le système",
    }

    # Séparer staff (modération) et admin (sys/buyer)
    staff_keys = ["moderation", "casier", "utilitaires", "mes_limites"]
    admin_keys = ["perms_admin", "config", "buyer", "hierarchy"]

    staff_lines = []
    for key in staff_keys:
        if help_v2_category_visible(key, ctx_author):
            cat = HELP_CATEGORIES_V2[key]
            staff_lines.append(f"{cat['emoji']} **{cat['label']}** — {category_descs[key]}")
    if staff_lines:
        em.add_field(name="🛡️ Modération", value="\n".join(staff_lines), inline=False)

    admin_lines = []
    for key in admin_keys:
        if help_v2_category_visible(key, ctx_author):
            cat = HELP_CATEGORIES_V2[key]
            admin_lines.append(f"{cat['emoji']} **{cat['label']}** — {category_descs[key]}")
    if admin_lines:
        em.add_field(name="⚙️ Admin & Config", value="\n".join(admin_lines), inline=False)

    em.add_field(
        name="💡 Astuce",
        value=f"Utilise `{p}helpall` pour un affichage **paginé par niveau de perm**.",
        inline=False,
    )
    em.set_footer(text=f"Sanction ・ Meira ・ {get_french_time()}")
    return em


def build_help_embed_for_v2(key, ctx_author):
    if key == "home":
        return build_help_home_embed_v2(ctx_author)
    return build_help_category_embed_v2(key, ctx_author)


class HelpDropdownV2(discord.ui.Select):
    def __init__(self, ctx_author):
        self.ctx_author = ctx_author
        options = [discord.SelectOption(label="Accueil", emoji="🏠", value="home")]
        for key, cat in HELP_CATEGORIES_V2.items():
            if help_v2_category_visible(key, ctx_author):
                options.append(discord.SelectOption(
                    label=cat["label"], emoji=cat["emoji"], value=key
                ))
        super().__init__(
            placeholder="📂 Choisis une catégorie...",
            min_values=1, max_values=1, options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        key = self.values[0]
        await interaction.response.edit_message(
            embed=build_help_embed_for_v2(key, self.ctx_author), view=self.view
        )


class HelpViewV2(discord.ui.View):
    def __init__(self, ctx_author):
        super().__init__(timeout=120)
        self.ctx_author = ctx_author
        self.add_item(HelpDropdownV2(ctx_author))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.ctx_author.id:
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
    """Help dynamique — staff-only."""
    if await check_bot_ban(ctx):
        return
    # Refus silencieux pour membres sans accès
    if not is_sys_or_buyer(ctx.author.id) and get_member_perm_level(ctx.author) == 0:
        return  # SILENT
    view = HelpViewV2(ctx.author)
    await ctx.send(embed=build_help_home_embed_v2(ctx.author), view=view)


# ========================= HELPALL PAGINÉ PAR NIVEAU =========================

def build_helpall_page(level, ctx_author):
    """
    Page du helpall pour un niveau de perm donné.
    Affiche toutes les commandes qui sont au niveau EXACT = level.
    """
    p = get_prefix_cached()
    cmd_perms = get_cmd_perms()

    # Liste les commandes au niveau exact
    cmds_at_level = sorted([cmd for cmd, lvl in cmd_perms.items() if lvl == level])

    # Construction embed
    em = discord.Embed(
        title=f"🎚️  Perm {level} / {MAX_PERM_LEVEL}",
        color=embed_color(),
    )
    _sanction_apply_thumbnail(em, ctx_author)

    member_perm = get_member_perm_level(ctx_author)
    if is_sys_or_buyer(ctx_author.id):
        access_note = "✅ **Sys/Buyer** — tu as accès à tous les niveaux."
    elif member_perm >= level:
        access_note = f"✅ Tu as accès à ce niveau (ton niveau : **{member_perm}**)."
    else:
        access_note = f"🔒 Ton niveau ({member_perm}) est trop bas pour ces commandes."

    if not cmds_at_level:
        em.description = f"{access_note}\n\n*Aucune commande configurée au niveau {level}.*"
    else:
        # Descriptions courtes par commande
        descriptions = {
            "warn":        "Avertir un membre",
            "mute":        "Mute textuel (timeout Discord)",
            "vmute":       "Mute vocal",
            "unmute":      "Retirer un mute",
            "unvmute":     "Retirer un vmute",
            "kick":        "Expulser du serveur",
            "ban":         "Bannir (temporaire ou perm)",
            "unban":       "Débannir",
            "unwarn":      "Annuler un warn",
            "unsanction":  "Annuler une sanction",
            "clearwarns":  "Effacer tous les warns d'un membre",
            "resetcasier": "Wipe complet du casier",
            "casier":      "Voir le casier d'un membre",
            "sanction":    "Détail d'une sanction par ID",
            "note":        "Ajouter une note staff privée",
            "notes":       "Lire les notes d'un membre",
            "delnote":     "Supprimer une note",
            "clear":       "Supprimer n messages",
            "lock":        "Verrouiller un salon",
            "unlock":      "Déverrouiller un salon",
            "slowmode":    "Configurer le slowmode",
            "modstats":    "Stats de modération",
            "mylimits":    "Tes quotas restants avant limite",
        }
        lines = []
        for cmd in cmds_at_level:
            desc = descriptions.get(cmd, "(pas de description)")
            # Limite actuelle pour ce niveau
            limit = get_limit_for(cmd, level)
            limit_str = ""
            if limit:
                max_a, window = limit
                limit_str = f" ・ *max {max_a}/{window}min*"
            lines.append(f"• `{p}{cmd}` — {desc}{limit_str}")
        em.description = f"{access_note}\n\n" + "\n".join(lines)

    em.set_footer(text=f"Sanction ・ Page {level}/{MAX_PERM_LEVEL} ・ ← → pour naviguer")
    return em


class HelpAllView(discord.ui.View):
    def __init__(self, ctx_author, start_level=1):
        super().__init__(timeout=120)
        self.ctx_author = ctx_author
        self.current_level = start_level
        self.max_accessible = MAX_PERM_LEVEL if is_sys_or_buyer(ctx_author.id) else get_member_perm_level(ctx_author)
        self._update_buttons()

    def _update_buttons(self):
        # Bouton gauche : désactivé si on est au niveau 1
        self.prev_btn.disabled = (self.current_level <= 1)
        # Bouton droit : désactivé si on est au max accessible
        self.next_btn.disabled = (self.current_level >= self.max_accessible)

    @discord.ui.button(emoji="⬅️", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_level > 1:
            self.current_level -= 1
            self._update_buttons()
            await interaction.response.edit_message(
                embed=build_helpall_page(self.current_level, self.ctx_author),
                view=self,
            )

    @discord.ui.button(emoji="➡️", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_level < self.max_accessible:
            self.current_level += 1
            self._update_buttons()
            await interaction.response.edit_message(
                embed=build_helpall_page(self.current_level, self.ctx_author),
                view=self,
            )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.ctx_author.id:
            await interaction.response.send_message(
                "Ce menu n'est pas à toi.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


@bot.command(name="helpall")
async def _helpall(ctx):
    """Help paginé par niveau de perm. ← → pour naviguer jusqu'à ton niveau max."""
    if await check_bot_ban(ctx):
        return
    # Bot staff-only
    if not is_sys_or_buyer(ctx.author.id) and get_member_perm_level(ctx.author) == 0:
        return  # SILENT

    view = HelpAllView(ctx.author, start_level=1)
    await ctx.send(embed=build_helpall_page(1, ctx.author), view=view)


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
