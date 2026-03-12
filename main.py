import asyncio
import io
import json
import os
import random
import re
import secrets
import time
import uuid
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Optional

import base58
import qrcode

# Load .env when running locally (file is gitignored; Render uses env vars)
_env_dir = os.path.dirname(os.path.abspath(__file__))
_env_path = os.path.join(_env_dir, ".env")
_env_path_txt = os.path.join(_env_dir, ".env.txt")  # Windows often saves as .env.txt
if os.path.isfile(_env_path):
    from dotenv import load_dotenv
    load_dotenv(_env_path)
elif os.path.isfile(_env_path_txt):
    from dotenv import load_dotenv
    load_dotenv(_env_path_txt)

import discord
from discord.ext import commands
from discord.ui import Button, View

from keep_alive import keep_alive


@dataclass(frozen=True)
class QueueConfig:
    queue_text_channel_id: int
    match_text_channel_id: Optional[int] = None  # None = bot creates temp channel per match and deletes after
    category_id: Optional[int] = None
    name: Optional[str] = None
    waiting_room_voice_channel_id: Optional[int] = None  # move players here when match ends; None = disconnect


@dataclass
class MatchSession:
    guild_id: int
    queue_text_channel_id: int
    match_text_channel_id: int
    category_id: Optional[int]
    session_id: str
    player_ids: list[int]

    phase: str = "captain_vote"  # lobby | captain_vote | draft | in_match | result_vote

    lobby_voice_channel_id: Optional[int] = None  # all 8 must join before captain vote starts
    lobby_created_by_bot: bool = False

    captain_votes: dict[int, list[int]] = field(default_factory=dict)  # voter_id -> [cap1_id, cap2_id]
    captain_ids: Optional[tuple[int, int]] = None

    team1_ids: list[int] = field(default_factory=list)
    team2_ids: list[int] = field(default_factory=list)
    remaining_ids: set[int] = field(default_factory=set)

    pick_order: list[int] = field(default_factory=list)  # list of team numbers (1/2), length 6
    pick_index: int = 0
    last_pick_ts: float = 0.0

    team1_voice_channel_id: Optional[int] = None
    team2_voice_channel_id: Optional[int] = None

    match_text_channel_created_by_bot: bool = False  # if True, delete this channel on cleanup
    match_slot_number: Optional[int] = None  # e.g. 1001 for cod4match-1001; used for game-based channel names

    vote_message_id: Optional[int] = None
    draft_message_id: Optional[int] = None
    result_message_id: Optional[int] = None

    result_votes: dict[int, int] = field(default_factory=dict)  # voter_id -> team (1/2)
    result_started_ts: Optional[float] = None

    cancel_votes: set[int] = field(default_factory=set)  # user_ids who voted to cancel
    cancel_vote_message_id: Optional[int] = None
    cancel_vote_started_ts: Optional[float] = None


@dataclass
class QueueState:
    guild_id: int
    queue_text_channel_id: int
    match_text_channel_id: Optional[int]  # None = create temp channel per match
    category_id: Optional[int]
    name: Optional[str]
    current_message_id: Optional[int] = None
    queued_user_ids: list[int] = field(default_factory=list)
    active_match_participants: set[int] = field(default_factory=set)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


MATCH_SIZE = _env_int("MATCH_SIZE", 8)


def load_queue_configs() -> dict[int, QueueConfig]:
    raw = os.getenv("QUEUES_JSON", "").strip()
    if not raw:
        _env_dir = os.path.dirname(os.path.abspath(__file__))
        _example = 'QUEUES_JSON=[{"queue_text_channel_id": YOUR_CHANNEL_ID, "match_text_channel_id": null}]'
        raise RuntimeError(
            "Missing QUEUES_JSON env var. For local runs, create a .env file in "
            + _env_dir
            + " with a line: " + _example + " (If you named the file .env.txt, the bot will load it.)"
        )

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"QUEUES_JSON is not valid JSON: {e}") from e

    if not isinstance(data, list) or not data:
        raise RuntimeError("QUEUES_JSON must be a non-empty JSON array.")

    configs: dict[int, QueueConfig] = {}
    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            raise RuntimeError(f"QUEUES_JSON[{idx}] must be an object.")
        if "queue_text_channel_id" not in item:
            raise RuntimeError(f"QUEUES_JSON[{idx}] must include queue_text_channel_id.")
        qid = int(item["queue_text_channel_id"])
        raw_mid = item.get("match_text_channel_id")
        mid = int(raw_mid) if raw_mid is not None else None
        cid = int(item["category_id"]) if "category_id" in item and item["category_id"] is not None else None
        name = str(item["name"]) if "name" in item and item["name"] is not None else None
        raw_wvc = item.get("waiting_room_voice_channel_id")
        wvc = int(raw_wvc) if raw_wvc is not None else None

        if qid in configs:
            raise RuntimeError(f"Duplicate queue_text_channel_id in QUEUES_JSON: {qid}")
        configs[qid] = QueueConfig(
            queue_text_channel_id=qid,
            match_text_channel_id=mid,
            category_id=cid,
            name=name,
            waiting_room_voice_channel_id=wvc,
        )

    return configs


intents = discord.Intents.default()
intents.voice_states = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


QUEUE_CONFIGS: dict[int, QueueConfig] = {}
QUEUE_STATES: dict[tuple[int, int], QueueState] = {}
ACTIVE_SESSIONS: dict[str, MatchSession] = {}
QUEUE_LOCKS: dict[tuple[int, int], asyncio.Lock] = {}
SESSION_LOCKS: dict[str, asyncio.Lock] = {}

VOTE_SECONDS = _env_int("VOTE_SECONDS", 60)
DRAFT_SECONDS = _env_int("DRAFT_SECONDS", 180)
RESULT_VOTE_SECONDS = _env_int("RESULT_VOTE_SECONDS", 60)
RESULT_VOTE_MAJORITY = (MATCH_SIZE // 2) + 1  # 5 for 8 players; session closes only with this many votes for one team
CANCEL_VOTE_SECONDS = _env_int("CANCEL_VOTE_SECONDS", 60)
LOBBY_VOICE_SECONDS = _env_int("LOBBY_VOICE_SECONDS", 300)  # time to get all 8 into lobby VC before cancel

# Member fetch throttle + cache to reduce Discord 429 rate limits (GET guild member)
_MEMBER_CACHE_TTL_SECONDS = 7200  # 2 hours; matches take ~1h, players re-queue after
_MEMBER_FETCH_INTERVAL = 0.2  # min seconds between fetch_member calls
_last_member_fetch_time: float = 0.0
_member_cache: dict[tuple[int, int], tuple[discord.Member, float]] = {}  # (guild_id, user_id) -> (member, cached_at)
_member_fetch_lock = asyncio.Lock()


async def get_or_fetch_member(guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
    """Return member from cache (if valid TTL) or guild cache, else throttle + fetch and cache. Reduces 429s."""
    global _last_member_fetch_time
    key = (guild.id, user_id)
    now = time.time()
    cached = _member_cache.get(key)
    if cached is not None:
        member, cached_at = cached
        if now - cached_at <= _MEMBER_CACHE_TTL_SECONDS:
            return member
        del _member_cache[key]
    m = guild.get_member(user_id)
    if m is not None:
        _member_cache[key] = (m, now)
        return m
    async with _member_fetch_lock:
        elapsed = time.time() - _last_member_fetch_time
        if elapsed < _MEMBER_FETCH_INTERVAL:
            await asyncio.sleep(_MEMBER_FETCH_INTERVAL - elapsed)
        try:
            m = await guild.fetch_member(user_id)
        except (discord.NotFound, discord.HTTPException):
            return None
        _last_member_fetch_time = time.time()
        _member_cache[key] = (m, _last_member_fetch_time)
        return m

# Game-based temp channel naming: cod4match-1001, cod4lobby-1001, etc. (slots 1001-1010 per game)
MATCH_SLOT_MIN = 1001
MATCH_SLOT_MAX = 1010  # max 10 concurrent matches per game


def _queue_name_to_slug(name: Optional[str]) -> str:
    """Convert queue display name to channel name slug, e.g. 'COD4 Queue' -> 'cod4'."""
    if not name or not name.strip():
        return "8s"
    s = name.strip().lower().replace(" queue", "").replace(" ", "")
    return s if s else "8s"


def _next_match_slot(
    guild: discord.Guild,
    category: Optional[discord.CategoryChannel],
    slug: str,
) -> int:
    """Return first available slot in [MATCH_SLOT_MIN, MATCH_SLOT_MAX] for channels named {slug}match-NNNN in category."""
    used: set[int] = set()
    pattern = re.compile(re.escape(slug) + r"match-(\d{4})\Z")
    if category is not None:
        for ch in category.channels:
            if ch.name:
                m = pattern.match(ch.name)
                if m:
                    try:
                        n = int(m.group(1))
                        if MATCH_SLOT_MIN <= n <= MATCH_SLOT_MAX:
                            used.add(n)
                    except ValueError:
                        pass
    for slot in range(MATCH_SLOT_MIN, MATCH_SLOT_MAX + 1):
        if slot not in used:
            return slot
    return MATCH_SLOT_MIN  # fallback if all slots used


def get_session_by_match_channel(channel_id: int) -> Optional[MatchSession]:
    for session in ACTIVE_SESSIONS.values():
        if session.match_text_channel_id == channel_id:
            return session
    return None

# Wager (Solana Pay QR)
WAGER_SOLANA_WALLET = os.getenv("WAGER_SOLANA_WALLET", "").strip()
WAGER_SPL_MINT = os.getenv("WAGER_SPL_MINT", "").strip() or None  # e.g. USDC mint; empty = SOL
WAGER_LABEL = os.getenv("WAGER_LABEL", "8s Wager").strip()
# Trust Wallet on iOS shows amount * 1000 when given user units. Set to 1 to send amount/1000 so Trust shows correct value.
WAGER_TRUST_WALLET_FIX = os.getenv("WAGER_TRUST_WALLET_FIX", "").strip().lower() in ("1", "true", "yes")


def build_solana_pay_url(
    recipient: str,
    amount: float,
    reference_b58: str,
    spl_mint: Optional[str] = None,
    label: str = "8s Wager",
    message: str = "",
    trust_wallet_fix: bool = False,
) -> str:
    """Build a Solana Pay transfer request URL (solana:...). Amount in user units (e.g. 25 for 25 USDC)."""
    if amount < 0:
        raise ValueError("Amount must be non-negative.")
    # Trust Wallet bug: it displays amount * 1000. Send amount/1000 so Trust shows the right number.
    if spl_mint and trust_wallet_fix:
        url_amount = amount / 1000.0
        amount_str = str(int(url_amount)) if url_amount == int(url_amount) else str(url_amount)
    else:
        amount_str = str(int(amount)) if amount == int(amount) else str(amount)
    params: dict[str, str] = {
        "amount": amount_str,
        "reference": reference_b58,
        "label": label,
    }
    if message:
        params["message"] = message
    if spl_mint:
        params["spl-token"] = spl_mint
    query = urllib.parse.urlencode(params)
    return f"solana:{recipient}?{query}"


def generate_wager_qr_bytes(url: str) -> bytes:
    """Generate a QR code image (PNG bytes) for a Solana Pay URL."""
    qr = qrcode.QRCode(version=1, box_size=6, border=2)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf.read()


def get_queue_lock(key: tuple[int, int]) -> asyncio.Lock:
    lock = QUEUE_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        QUEUE_LOCKS[key] = lock
    return lock


def get_session_lock(session_id: str) -> asyncio.Lock:
    lock = SESSION_LOCKS.get(session_id)
    if lock is None:
        lock = asyncio.Lock()
        SESSION_LOCKS[session_id] = lock
    return lock


def choose_two_captains(
    player_ids: list[int], votes: dict[int, list[int]]
) -> tuple[tuple[int, int], dict[int, int]]:
    counts: dict[int, int] = {pid: 0 for pid in player_ids}
    for ballot in votes.values():
        for candidate_id in ballot[:2]:
            if candidate_id in counts:
                counts[candidate_id] += 1

    if all(v == 0 for v in counts.values()):
        cap1, cap2 = random.sample(player_ids, 2)
        return (cap1, cap2), counts

    max1 = max(counts.values())
    top1 = [pid for pid, c in counts.items() if c == max1]
    cap1 = random.choice(top1)

    remaining_counts = {pid: c for pid, c in counts.items() if pid != cap1}
    max2 = max(remaining_counts.values())
    top2 = [pid for pid, c in remaining_counts.items() if c == max2]
    cap2 = random.choice(top2)

    return (cap1, cap2), counts


def build_pick_order(first_team: int) -> list[int]:
    second_team = 2 if first_team == 1 else 1
    return [first_team, second_team, second_team, first_team, first_team, second_team]


def render_players(guild: discord.Guild, ids: list[int]) -> str:
    names: list[str] = []
    for pid in ids:
        m = guild.get_member(pid)
        if m is None:
            names.append(f"<@{pid}>")
        else:
            names.append(m.mention)
    return ", ".join(names) if names else "(none)"


def _captain_vote_counts(votes: dict[int, list[int]], player_ids: list[int]) -> dict[int, int]:
    """Return candidate_id -> vote count from captain vote ballots."""
    counts = {pid: 0 for pid in player_ids}
    for ballot in votes.values():
        for cid in ballot[:2]:
            if cid in counts:
                counts[cid] += 1
    return counts


def format_captain_vote_message_content(guild: discord.Guild, session: MatchSession) -> str:
    """Build the captain vote message text with vote count shown under each player."""
    intro = (
        f"✅ **All {MATCH_SIZE} players are in the lobby.**\n\n"
        f"Each player vote for **2 captains** (2 clicks total). Voting ends in **{VOTE_SECONDS}s**.\n\n"
    )
    counts = _captain_vote_counts(session.captain_votes, session.player_ids)
    lines = [f"<@{pid}> — **{counts.get(pid, 0)}** vote{'s' if counts.get(pid, 0) != 1 else ''}" for pid in session.player_ids]
    return intro + "**Vote tally (captain votes per player):**\n" + "\n".join(lines)


async def resolve_display_names(guild: discord.Guild, ids: list[int]) -> dict[int, str]:
    """Resolve display names for button labels; uses throttled+cached get_or_fetch_member. Never returns raw IDs."""
    result: dict[int, str] = {}
    for pid in ids:
        m = await get_or_fetch_member(guild, pid)
        if m is not None:
            name = m.display_name or m.name or "Player"
        else:
            name = "Player"
        if len(name) > 80:
            name = name[:77] + "..."
        result[pid] = name
    return result


class VoteCaptainButton(Button):
    def __init__(self, session_id: str, candidate_id: int, label: str):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label=label,
            custom_id=f"capvote:{session_id}:{candidate_id}",
        )
        self.session_id = session_id
        self.candidate_id = candidate_id

    async def callback(self, interaction: discord.Interaction) -> None:
        session = ACTIVE_SESSIONS.get(self.session_id)
        if session is None:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return

        should_finalize = False
        async with get_session_lock(self.session_id):
            if session.phase != "captain_vote":
                await interaction.response.send_message("Captain voting is closed.", ephemeral=True)
                return

            voter_id = interaction.user.id
            if voter_id not in session.player_ids:
                await interaction.response.send_message("Only queued players can vote.", ephemeral=True)
                return

            ballot = session.captain_votes.setdefault(voter_id, [])
            if self.candidate_id in ballot:
                await interaction.response.send_message("You already voted for that player.", ephemeral=True)
                return
            if len(ballot) >= 2:
                await interaction.response.send_message("You already used both votes.", ephemeral=True)
                return

            ballot.append(self.candidate_id)

            remaining = 2 - len(ballot)
            if remaining == 1:
                await interaction.response.send_message("Vote recorded. Pick 1 more captain.", ephemeral=True)
            else:
                await interaction.response.send_message("Vote recorded. You are done voting.", ephemeral=True)

            if all(len(session.captain_votes.get(pid, [])) >= 2 for pid in session.player_ids):
                should_finalize = True

        if should_finalize:
            await finalize_captain_vote(self.session_id, reason="all_votes_in")
        else:
            session = ACTIVE_SESSIONS.get(self.session_id)
            if session is not None and session.vote_message_id is not None:
                guild = bot.get_guild(session.guild_id)
                if guild is not None:
                    match_text = guild.get_channel(session.match_text_channel_id)
                    if isinstance(match_text, discord.TextChannel):
                        try:
                            msg = await match_text.fetch_message(session.vote_message_id)
                            new_content = format_captain_vote_message_content(guild, session)
                            await msg.edit(content=new_content)
                        except (discord.NotFound, discord.HTTPException):
                            pass


class CaptainVoteView(View):
    def __init__(self, session_id: str, guild: discord.Guild, player_ids: list[int], display_names: Optional[dict[int, str]] = None):
        super().__init__(timeout=None)
        self.session_id = session_id

        for pid in player_ids:
            if display_names is not None and pid in display_names:
                label = display_names[pid]
            else:
                member = guild.get_member(pid)
                label = member.display_name if member else "Player"
                if len(label) > 80:
                    label = label[:77] + "..."
            self.add_item(VoteCaptainButton(session_id, pid, label))


class DraftPickButton(Button):
    def __init__(self, session_id: str, pick_id: int, label: str):
        super().__init__(
            style=discord.ButtonStyle.success,
            label=label,
            custom_id=f"draftpick:{session_id}:{pick_id}",
        )
        self.session_id = session_id
        self.pick_id = pick_id

    async def callback(self, interaction: discord.Interaction) -> None:
        session = ACTIVE_SESSIONS.get(self.session_id)
        if session is None:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return

        allowed = False
        async with get_session_lock(self.session_id):
            if session.phase != "draft":
                await interaction.response.send_message("Drafting is not active.", ephemeral=True)
                return

            if session.captain_ids is None:
                await interaction.response.send_message("Draft is not ready yet.", ephemeral=True)
                return

            if session.pick_index >= len(session.pick_order):
                await interaction.response.send_message("Draft is already complete.", ephemeral=True)
                return

            team_to_pick = session.pick_order[session.pick_index]
            captain_id = session.captain_ids[team_to_pick - 1]
            if interaction.user.id != captain_id:
                await interaction.response.send_message("It is not your pick.", ephemeral=True)
                return

            if self.pick_id not in session.remaining_ids:
                await interaction.response.send_message("That player is no longer available.", ephemeral=True)
                return

            allowed = True

        if allowed:
            await interaction.response.defer()
            await apply_pick_and_advance(self.session_id, picked_id=self.pick_id)


class DraftPickView(View):
    def __init__(self, session_id: str, guild: discord.Guild, remaining_ids: list[int], display_names: Optional[dict[int, str]] = None):
        super().__init__(timeout=None)
        self.session_id = session_id

        for pid in remaining_ids:
            if display_names is not None and pid in display_names:
                label = display_names[pid]
            else:
                member = guild.get_member(pid)
                label = member.display_name if member else "Player"
                if len(label) > 80:
                    label = label[:77] + "..."
            self.add_item(DraftPickButton(session_id, pid, label))


class StartResultVoteButton(Button):
    def __init__(self, session_id: str):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label="Start winner vote",
            custom_id=f"startresult:{session_id}",
        )
        self.session_id = session_id

    async def callback(self, interaction: discord.Interaction) -> None:
        session = ACTIVE_SESSIONS.get(self.session_id)
        if session is None:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return

        async with get_session_lock(self.session_id):
            if session.phase != "in_match":
                await interaction.response.send_message("Winner voting isn’t available right now.", ephemeral=True)
                return
            if interaction.user.id not in session.player_ids:
                await interaction.response.send_message("Only match players can start the vote.", ephemeral=True)
                return

            session.phase = "result_vote"
            session.result_started_ts = time.time()
            session.result_votes.clear()

        await interaction.response.send_message(
            f"Vote which team won (need {RESULT_VOTE_MAJORITY}/{MATCH_SIZE} majority; vote ends in {RESULT_VOTE_SECONDS}s if no majority):",
            view=ResultVoteView(session.session_id),
        )
        bot.loop.create_task(result_vote_timeout_task(self.session_id))


class StartResultVoteView(View):
    def __init__(self, session_id: str):
        super().__init__(timeout=None)
        self.add_item(StartResultVoteButton(session_id))


class ResultVoteButton(Button):
    def __init__(self, session_id: str, team: int, label: str):
        super().__init__(
            style=discord.ButtonStyle.success if team == 1 else discord.ButtonStyle.danger,
            label=label,
            custom_id=f"result:{session_id}:{team}",
        )
        self.session_id = session_id
        self.team = team

    async def callback(self, interaction: discord.Interaction) -> None:
        session = ACTIVE_SESSIONS.get(self.session_id)
        if session is None:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return

        async with get_session_lock(self.session_id):
            if session.phase != "result_vote":
                await interaction.response.send_message("Winner voting is closed.", ephemeral=True)
                return
            voter_id = interaction.user.id
            if voter_id not in session.player_ids:
                await interaction.response.send_message("Only match players can vote.", ephemeral=True)
                return

            session.result_votes[voter_id] = self.team
            count1 = sum(1 for t in session.result_votes.values() if t == 1)
            count2 = sum(1 for t in session.result_votes.values() if t == 2)
            done = (count1 >= RESULT_VOTE_MAJORITY and count1 > count2) or (
                count2 >= RESULT_VOTE_MAJORITY and count2 > count1
            )

        await interaction.response.send_message(
            f"Vote recorded. Current tally: Team 1 = {count1}, Team 2 = {count2}. (Need {RESULT_VOTE_MAJORITY}/{MATCH_SIZE} for one team to win.)",
            ephemeral=True,
        )

        if done:
            await finalize_result_vote(self.session_id, reason="majority")


class ResultVoteView(View):
    def __init__(self, session_id: str):
        super().__init__(timeout=None)
        self.add_item(ResultVoteButton(session_id, 1, "Team 1 won"))
        self.add_item(ResultVoteButton(session_id, 2, "Team 2 won"))


class CancelVoteYesButton(Button):
    def __init__(self, session_id: str):
        super().__init__(
            style=discord.ButtonStyle.danger,
            label="Yes, cancel match",
            custom_id=f"cancelvote:yes:{session_id}",
        )
        self.session_id = session_id

    async def callback(self, interaction: discord.Interaction) -> None:
        session = ACTIVE_SESSIONS.get(self.session_id)
        if session is None:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return
        async with get_session_lock(self.session_id):
            if session.cancel_vote_message_id is None:
                await interaction.response.send_message("Cancel vote has ended.", ephemeral=True)
                return
            if interaction.user.id not in session.player_ids:
                await interaction.response.send_message("Only match players can vote.", ephemeral=True)
                return
            session.cancel_votes.add(interaction.user.id)
            n = len(session.cancel_votes)
            total = len(session.player_ids)
            need = (total // 2) + 1
        await interaction.response.send_message(
            f"Vote recorded. **{n}/{total}** voted to cancel (need {need} to cancel).",
            ephemeral=True,
        )
        if n >= need:
            await cancel_session(self.session_id, reason="cancel_vote_passed")


class CancelVoteNoButton(Button):
    def __init__(self, session_id: str):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="No, keep playing",
            custom_id=f"cancelvote:no:{session_id}",
        )
        self.session_id = session_id

    async def callback(self, interaction: discord.Interaction) -> None:
        session = ACTIVE_SESSIONS.get(self.session_id)
        if session is None:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return
        async with get_session_lock(self.session_id):
            if session.cancel_vote_message_id is None:
                await interaction.response.send_message("Cancel vote has ended.", ephemeral=True)
                return
            if interaction.user.id not in session.player_ids:
                await interaction.response.send_message("Only match players can vote.", ephemeral=True)
                return
        await interaction.response.send_message("Vote recorded. Match continues unless majority votes to cancel.", ephemeral=True)


class CancelVoteView(View):
    def __init__(self, session_id: str):
        super().__init__(timeout=None)
        self.add_item(CancelVoteYesButton(session_id))
        self.add_item(CancelVoteNoButton(session_id))


async def cancel_vote_timeout_task(session_id: str) -> None:
    await asyncio.sleep(CANCEL_VOTE_SECONDS)
    session = ACTIVE_SESSIONS.get(session_id)
    if session is None:
        return
    async with get_session_lock(session_id):
        if session.cancel_vote_message_id is None:
            return
        session.cancel_votes.clear()
        session.cancel_vote_message_id = None
        session.cancel_vote_started_ts = None
    guild = bot.get_guild(session.guild_id)
    if guild is not None:
        ch = guild.get_channel(session.match_text_channel_id)
        if isinstance(ch, discord.TextChannel):
            await ch.send("Cancel vote ended. Match continues.")


def format_queue_message(state: QueueState) -> str:
    title = state.name or "8s Queue"
    count = len(state.queued_user_ids)
    queued = " ".join(f"<@{uid}>" for uid in state.queued_user_ids) or "(empty)"
    return (
        f"**{title}** ({count}/{MATCH_SIZE})\n"
        f"Click **Join Queue** to enter and **Leave Queue** to exit.\n\n"
        f"Queued:\n{queued}"
    )


class QueueJoinButton(Button):
    def __init__(self, queue_key: tuple[int, int]):
        super().__init__(
            style=discord.ButtonStyle.success,
            label="Join Queue",
            custom_id=f"queue:join:{queue_key[0]}:{queue_key[1]}",
        )
        self.queue_key = queue_key

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.channel is None:
            await interaction.response.send_message("Queues only work inside servers.", ephemeral=True)
            return

        state = QUEUE_STATES.get(self.queue_key)
        if state is None:
            await interaction.response.send_message("This queue is not configured.", ephemeral=True)
            return

        user_id = interaction.user.id
        if interaction.user.bot:
            await interaction.response.send_message("Bots cannot join the queue.", ephemeral=True)
            return

        start_match_player_ids: Optional[list[int]] = None

        async with get_queue_lock(self.queue_key):
            if user_id in state.active_match_participants:
                await interaction.response.send_message("You’re currently in an active match for this queue.", ephemeral=True)
                return

            if user_id in state.queued_user_ids:
                await interaction.response.send_message("You’re already in the queue.", ephemeral=True)
                return

            if len(state.queued_user_ids) >= MATCH_SIZE:
                await interaction.response.send_message("This queue is full. Please use the newest queue message.", ephemeral=True)
                return

            state.queued_user_ids.append(user_id)

            if len(state.queued_user_ids) >= MATCH_SIZE:
                start_match_player_ids = state.queued_user_ids[:MATCH_SIZE]
                state.queued_user_ids = state.queued_user_ids[MATCH_SIZE:]
                state.active_match_participants.update(start_match_player_ids)

        # Update current queue message
        await interaction.response.defer()
        try:
            await interaction.message.edit(content=format_queue_message(state), view=QueueView(self.queue_key))
        except discord.HTTPException:
            pass

        # If we filled, lock the old message and post a new one immediately.
        if start_match_player_ids is not None:
            try:
                await interaction.message.edit(
                    content=(
                        f"✅ Queue filled ({MATCH_SIZE}/{MATCH_SIZE}). Starting match now…\n\n"
                        f"Players:\n{' '.join(f'<@{uid}>' for uid in start_match_player_ids)}"
                    ),
                    view=None,
                )
            except discord.HTTPException:
                pass

            # Post new queue message right away.
            channel = interaction.channel
            if isinstance(channel, discord.TextChannel):
                async with get_queue_lock(self.queue_key):
                    state.current_message_id = None
                    msg = await channel.send(content=format_queue_message(state), view=QueueView(self.queue_key))
                    state.current_message_id = msg.id

            bot.loop.create_task(start_match_from_queue(state, start_match_player_ids))


class QueueLeaveButton(Button):
    def __init__(self, queue_key: tuple[int, int]):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Leave Queue",
            custom_id=f"queue:leave:{queue_key[0]}:{queue_key[1]}",
        )
        self.queue_key = queue_key

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Queues only work inside servers.", ephemeral=True)
            return

        state = QUEUE_STATES.get(self.queue_key)
        if state is None:
            await interaction.response.send_message("This queue is not configured.", ephemeral=True)
            return

        user_id = interaction.user.id
        changed = False
        async with get_queue_lock(self.queue_key):
            if user_id in state.queued_user_ids:
                state.queued_user_ids = [uid for uid in state.queued_user_ids if uid != user_id]
                changed = True

        if not changed:
            await interaction.response.send_message("You are not currently in the queue.", ephemeral=True)
            return

        await interaction.response.defer()
        try:
            await interaction.message.edit(content=format_queue_message(state), view=QueueView(self.queue_key))
        except discord.HTTPException:
            pass


class QueueView(View):
    def __init__(self, queue_key: tuple[int, int]):
        super().__init__(timeout=None)
        self.add_item(QueueJoinButton(queue_key))
        self.add_item(QueueLeaveButton(queue_key))


async def start_match_from_queue(state: QueueState, player_ids: list[int]) -> None:
    guild = bot.get_guild(state.guild_id)
    if guild is None:
        async with get_queue_lock((state.guild_id, state.queue_text_channel_id)):
            state.active_match_participants.difference_update(player_ids)
        return

    match_text_channel: discord.TextChannel
    match_channel_created_by_bot = False
    slot: Optional[int] = None
    slug = "8s"

    if state.match_text_channel_id is not None:
        ch = guild.get_channel(state.match_text_channel_id)
        if not isinstance(ch, discord.TextChannel):
            async with get_queue_lock((state.guild_id, state.queue_text_channel_id)):
                state.active_match_participants.difference_update(player_ids)
            return
        match_text_channel = ch
    else:
        category: Optional[discord.CategoryChannel] = None
        if state.category_id is not None:
            cat = guild.get_channel(state.category_id)
            if isinstance(cat, discord.CategoryChannel):
                category = cat
        if category is None:
            queue_ch = guild.get_channel(state.queue_text_channel_id)
            if isinstance(queue_ch, discord.TextChannel) and queue_ch.category is not None:
                category = queue_ch.category
        overwrites: dict[Any, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
        }
        if guild.me is not None:
            overwrites[guild.me] = discord.PermissionOverwrite(
                view_channel=True, read_message_history=True, send_messages=True
            )
        for pid in player_ids:
            m = await get_or_fetch_member(guild, pid)
            if m is not None:
                overwrites[m] = discord.PermissionOverwrite(
                    view_channel=True, read_message_history=True, send_messages=True
                )
        slug = _queue_name_to_slug(state.name)
        slot = _next_match_slot(guild, category, slug)
        try:
            match_text_channel = await guild.create_text_channel(
                name=f"{slug}match-{slot}",
                category=category,
                overwrites=overwrites,
            )
        except discord.Forbidden:
            async with get_queue_lock((state.guild_id, state.queue_text_channel_id)):
                state.active_match_participants.difference_update(player_ids)
            return
        match_channel_created_by_bot = True

    session_id = uuid.uuid4().hex[:6]
    category_for_lobby: Optional[discord.CategoryChannel] = None
    if match_text_channel.category_id:
        cat = guild.get_channel(match_text_channel.category_id)
        if isinstance(cat, discord.CategoryChannel):
            category_for_lobby = cat

    lobby_overwrites: dict[Any, discord.PermissionOverwrite] = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False),
    }
    if guild.me is not None:
        lobby_overwrites[guild.me] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
    for pid in player_ids:
        m = await get_or_fetch_member(guild, pid)
        if m is not None:
            lobby_overwrites[m] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
    # Use game-based lobby name when we created the match channel (slot is set)
    lobby_slot = slot if match_channel_created_by_bot else None
    lobby_name = f"{slug}lobby-{lobby_slot}" if lobby_slot is not None else f"8s-lobby-{session_id[:6]}"
    try:
        lobby_vc = await guild.create_voice_channel(
            name=lobby_name,
            category=category_for_lobby,
            overwrites=lobby_overwrites,
        )
    except discord.Forbidden:
        async with get_queue_lock((state.guild_id, state.queue_text_channel_id)):
            state.active_match_participants.difference_update(player_ids)
        if match_channel_created_by_bot:
            try:
                await match_text_channel.delete()
            except discord.Forbidden:
                pass
        return

    session = MatchSession(
        guild_id=guild.id,
        queue_text_channel_id=state.queue_text_channel_id,
        match_text_channel_id=match_text_channel.id,
        category_id=state.category_id,
        session_id=session_id,
        player_ids=player_ids,
        phase="lobby",
        lobby_voice_channel_id=lobby_vc.id,
        lobby_created_by_bot=True,
        match_text_channel_created_by_bot=match_channel_created_by_bot,
        match_slot_number=slot,
    )
    ACTIVE_SESSIONS[session_id] = session
    get_session_lock(session_id)

    queue_channel = guild.get_channel(state.queue_text_channel_id)
    queue_name = queue_channel.mention if isinstance(queue_channel, discord.TextChannel) else "the queue"
    lobby_mention = lobby_vc.mention
    await match_text_channel.send(
        f"🔥 **{MATCH_SIZE} players queued** in {queue_name}.\n\n"
        f"**Everyone must join the voice channel {lobby_mention}** to confirm you're here. "
        f"When all **{MATCH_SIZE}** players are in the channel, the captain vote will start.\n\n"
        f"You have **{LOBBY_VOICE_SECONDS // 60} minutes** to join. Players: {render_players(guild, session.player_ids)}",
    )
    bot.loop.create_task(lobby_timeout_task(session_id))


async def lobby_timeout_task(session_id: str) -> None:
    await asyncio.sleep(LOBBY_VOICE_SECONDS)
    session = ACTIVE_SESSIONS.get(session_id)
    if session is None:
        return
    async with get_session_lock(session_id):
        if session.phase != "lobby":
            return
    await cancel_session(session_id, reason="lobby_timeout")


async def start_captain_vote_from_lobby(session_id: str) -> None:
    session = ACTIVE_SESSIONS.get(session_id)
    if session is None:
        return
    guild = bot.get_guild(session.guild_id)
    if guild is None:
        await cancel_session(session_id, reason="guild_missing")
        return
    async with get_session_lock(session_id):
        if session.phase != "lobby":
            return
        session.phase = "captain_vote"
    match_text = guild.get_channel(session.match_text_channel_id)
    if not isinstance(match_text, discord.TextChannel):
        return
    queue_channel = guild.get_channel(session.queue_text_channel_id)
    queue_name = queue_channel.mention if isinstance(queue_channel, discord.TextChannel) else "the queue"
    captain_display_names = await resolve_display_names(guild, session.player_ids)
    vote_view = CaptainVoteView(session_id, guild, session.player_ids, captain_display_names)
    content = format_captain_vote_message_content(guild, session)
    msg = await match_text.send(content=content, view=vote_view)
    async with get_session_lock(session_id):
        session = ACTIVE_SESSIONS.get(session_id)
        if session is not None:
            session.vote_message_id = msg.id
    bot.loop.create_task(captain_vote_timeout_task(session_id))


async def cancel_session(session_id: str, reason: str) -> None:
    session = ACTIVE_SESSIONS.get(session_id)
    if session is None:
        return

    guild = bot.get_guild(session.guild_id)
    match_ch_id = session.match_text_channel_id
    created_by_bot = session.match_text_channel_created_by_bot

    if guild is not None:
        match_text = guild.get_channel(session.match_text_channel_id)
        if isinstance(match_text, discord.TextChannel):
            await match_text.send(f"⚠️ Match cancelled (`{reason}`).")

    queue_key = (session.guild_id, session.queue_text_channel_id)
    state = QUEUE_STATES.get(queue_key)
    if state is not None:
        async with get_queue_lock(queue_key):
            state.active_match_participants.difference_update(session.player_ids)

    if session.lobby_voice_channel_id is not None and session.lobby_created_by_bot and guild is not None:
        lobby_ch = guild.get_channel(session.lobby_voice_channel_id)
        if isinstance(lobby_ch, discord.VoiceChannel):
            for m in list(lobby_ch.members):
                try:
                    await m.move_to(None)
                except discord.Forbidden:
                    pass
            try:
                await lobby_ch.delete()
            except discord.Forbidden:
                pass
    await cleanup_voice_channels(session)
    ACTIVE_SESSIONS.pop(session_id, None)
    SESSION_LOCKS.pop(session_id, None)

    if created_by_bot and guild is not None and match_ch_id is not None:
        ch = guild.get_channel(match_ch_id)
        if isinstance(ch, discord.TextChannel):
            try:
                await ch.delete()
            except discord.Forbidden:
                pass


async def captain_vote_timeout_task(session_id: str) -> None:
    await asyncio.sleep(VOTE_SECONDS)
    await finalize_captain_vote(session_id, reason="timeout")


async def finalize_captain_vote(session_id: str, reason: str) -> None:
    session = ACTIVE_SESSIONS.get(session_id)
    if session is None:
        return

    async with get_session_lock(session_id):
        if session.phase != "captain_vote":
            return

        guild = bot.get_guild(session.guild_id)
        if guild is None:
            await cancel_session(session_id, reason="guild_missing")
            return

        captains, counts = choose_two_captains(session.player_ids, session.captain_votes)
        session.captain_ids = captains
        cap1, cap2 = captains

        session.team1_ids = [cap1]
        session.team2_ids = [cap2]
        session.remaining_ids = set(session.player_ids) - {cap1, cap2}

        # Team 1 always picks first; pick order: 1, 2, 2, 1, 1, 2
        session.pick_order = build_pick_order(1)
        session.pick_index = 0
        session.last_pick_ts = time.time()
        session.phase = "draft"

    match_text = guild.get_channel(session.match_text_channel_id)
    if not isinstance(match_text, discord.TextChannel):
        await cancel_session(session_id, reason="invalid_match_text_channel")
        return

    remaining_sorted = sorted(session.remaining_ids)
    draft_display_names = await resolve_display_names(guild, remaining_sorted)
    draft_view = DraftPickView(session.session_id, guild, remaining_sorted, draft_display_names)

    team_to_pick = session.pick_order[session.pick_index]
    captain_id = session.captain_ids[team_to_pick - 1] if session.captain_ids else None
    captain_member = guild.get_member(captain_id) if captain_id else None
    picker = captain_member.mention if captain_member else f"<@{captain_id}>"

    msg = await match_text.send(
        f"🧢 **Captains chosen** (reason: `{reason}`): <@{session.captain_ids[0]}> vs <@{session.captain_ids[1]}>\n\n"
        f"**Draft started**. {picker} is picking now.\n"
        f"Available: {render_players(guild, remaining_sorted)}\n\n"
        f"Draft auto-picks after **{DRAFT_SECONDS}s** of inactivity.",
        view=draft_view,
    )
    session.draft_message_id = msg.id

    bot.loop.create_task(draft_timeout_task(session_id))


async def draft_timeout_task(session_id: str) -> None:
    while True:
        session = ACTIVE_SESSIONS.get(session_id)
        if session is None:
            return
        if session.phase != "draft":
            return

        await asyncio.sleep(2)
        now = time.time()
        async with get_session_lock(session_id):
            session = ACTIVE_SESSIONS.get(session_id)
            if session is None or session.phase != "draft":
                return
            if not session.remaining_ids:
                return
            if now - session.last_pick_ts < DRAFT_SECONDS:
                continue

            picked_id = random.choice(list(session.remaining_ids))

        await apply_pick_and_advance(session_id, picked_id=picked_id, autopick=True)


async def apply_pick_and_advance(session_id: str, picked_id: int, autopick: bool = False) -> None:
    session = ACTIVE_SESSIONS.get(session_id)
    if session is None:
        return
    guild = bot.get_guild(session.guild_id)
    if guild is None:
        await cancel_session(session_id, reason="guild_missing")
        return

    async with get_session_lock(session_id):
        session = ACTIVE_SESSIONS.get(session_id)
        if session is None or session.phase != "draft":
            return
        if session.pick_index >= len(session.pick_order):
            return
        if picked_id not in session.remaining_ids:
            return

        team_to_pick = session.pick_order[session.pick_index]
        if team_to_pick == 1:
            session.team1_ids.append(picked_id)
        else:
            session.team2_ids.append(picked_id)
        session.remaining_ids.remove(picked_id)
        session.pick_index += 1
        session.last_pick_ts = time.time()

        # Auto-place last remaining player onto the team that's due to pick (no manual pick needed)
        if len(session.remaining_ids) == 1 and session.pick_index < len(session.pick_order):
            last_id = next(iter(session.remaining_ids))
            team_to_pick = session.pick_order[session.pick_index]
            if team_to_pick == 1:
                session.team1_ids.append(last_id)
            else:
                session.team2_ids.append(last_id)
            session.remaining_ids.clear()
            session.pick_index += 1

        done = session.pick_index >= len(session.pick_order) or not session.remaining_ids
        if done:
            session.phase = "in_match"

    match_text = guild.get_channel(session.match_text_channel_id)
    if not isinstance(match_text, discord.TextChannel):
        await cancel_session(session_id, reason="invalid_match_text_channel")
        return

    if session.draft_message_id is None:
        await cancel_session(session_id, reason="missing_draft_message")
        return

    try:
        msg = await match_text.fetch_message(session.draft_message_id)
    except discord.NotFound:
        msg = None

    picked_member = guild.get_member(picked_id)
    picked_label = picked_member.mention if picked_member else f"<@{picked_id}>"

    if session.phase != "in_match":
        remaining_sorted = sorted(session.remaining_ids)
        pick_display_names = await resolve_display_names(guild, remaining_sorted)
        draft_view = DraftPickView(session.session_id, guild, remaining_sorted, pick_display_names)

        team_to_pick = session.pick_order[session.pick_index]
        captain_id = session.captain_ids[team_to_pick - 1] if session.captain_ids else None
        captain_member = guild.get_member(captain_id) if captain_id else None
        picker = captain_member.mention if captain_member else f"<@{captain_id}>"

        content = (
            f"✅ Pick {'(auto)' if autopick else ''}: {picked_label}\n\n"
            f"Now picking: {picker}\n"
            f"Team 1: {render_players(guild, session.team1_ids)}\n"
            f"Team 2: {render_players(guild, session.team2_ids)}\n\n"
            f"Available: {render_players(guild, remaining_sorted)}"
        )
        if msg is not None:
            await msg.edit(content=content, view=draft_view)
        else:
            new_msg = await match_text.send(content, view=draft_view)
            session.draft_message_id = new_msg.id
        return

    # Draft finished => create temp voice channels and move players.
    content = (
        f"🏁 Draft complete.\n"
        f"Team 1: {render_players(guild, session.team1_ids)}\n"
        f"Team 2: {render_players(guild, session.team2_ids)}\n\n"
        f"Creating team voice channels and moving players now…"
    )
    if msg is not None:
        await msg.edit(content=content, view=None)
    else:
        await match_text.send(content)

    await create_team_channels_and_move(session_id)


async def create_team_channels_and_move(session_id: str) -> None:
    session = ACTIVE_SESSIONS.get(session_id)
    if session is None:
        return
    guild = bot.get_guild(session.guild_id)
    if guild is None:
        await cancel_session(session_id, reason="guild_missing")
        return

    category: Optional[discord.CategoryChannel] = None
    if session.category_id is not None:
        ch = guild.get_channel(session.category_id)
        if isinstance(ch, discord.CategoryChannel):
            category = ch
    if category is None:
        match_text = guild.get_channel(session.match_text_channel_id)
        if isinstance(match_text, discord.TextChannel) and match_text.category is not None:
            category = match_text.category
    if category is None:
        queue_text = guild.get_channel(session.queue_text_channel_id)
        if isinstance(queue_text, discord.TextChannel) and queue_text.category is not None:
            category = queue_text.category

    async def overwrites_for_team(team_ids: list[int]) -> dict[Any, discord.PermissionOverwrite]:
        overwrites: dict[Any, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False),
        }
        if guild.me is not None:
            overwrites[guild.me] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
        for pid in team_ids:
            m = await get_or_fetch_member(guild, pid)
            if m is not None:
                overwrites[m] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
        return overwrites

    queue_state = QUEUE_STATES.get((session.guild_id, session.queue_text_channel_id))
    slug = _queue_name_to_slug(queue_state.name if queue_state else None)
    slot = session.match_slot_number
    if slot is not None:
        team1_name = f"{slug}team1-{slot}"
        team2_name = f"{slug}team2-{slot}"
    else:
        base_name = f"8s-{session.session_id}"
        team1_name = f"{base_name}-team1"
        team2_name = f"{base_name}-team2"

    try:
        team1_overwrites = await overwrites_for_team(session.team1_ids)
        team2_overwrites = await overwrites_for_team(session.team2_ids)
        team1_chan = await guild.create_voice_channel(
            name=team1_name,
            category=category,
            overwrites=team1_overwrites,
        )
        team2_chan = await guild.create_voice_channel(
            name=team2_name,
            category=category,
            overwrites=team2_overwrites,
        )
    except discord.Forbidden:
        await cancel_session(session_id, reason="missing_permissions_create_channels")
        return

    async with get_session_lock(session_id):
        session.team1_voice_channel_id = team1_chan.id
        session.team2_voice_channel_id = team2_chan.id

    async def move_team(team_ids: list[int], dest: discord.VoiceChannel) -> None:
        for pid in team_ids:
            m = guild.get_member(pid)
            if m is None:
                continue
            if m.voice is None or m.voice.channel is None:
                continue
            try:
                await m.move_to(dest)
            except discord.Forbidden:
                continue

    await move_team(session.team1_ids, team1_chan)
    await move_team(session.team2_ids, team2_chan)

    match_text = guild.get_channel(session.match_text_channel_id)
    if isinstance(match_text, discord.TextChannel):
        await match_text.send(
            f"🎮 **Match started**\n"
            f"Team 1 VC: {team1_chan.mention}\n"
            f"Team 2 VC: {team2_chan.mention}\n\n"
            f"Only your team can see and join your team's voice channel. If you weren't moved automatically, join your team VC above.\n\n"
            f"When the match ends, click below to start the winner vote.",
            view=StartResultVoteView(session.session_id),
        )

    if session.lobby_voice_channel_id is not None and session.lobby_created_by_bot:
        lobby_ch = guild.get_channel(session.lobby_voice_channel_id)
        async with get_session_lock(session_id):
            session.lobby_voice_channel_id = None
        if isinstance(lobby_ch, discord.VoiceChannel):
            for m in list(lobby_ch.members):
                try:
                    await m.move_to(None)
                except discord.Forbidden:
                    pass
            try:
                await lobby_ch.delete()
            except discord.Forbidden:
                pass


async def result_vote_timeout_task(session_id: str) -> None:
    await asyncio.sleep(RESULT_VOTE_SECONDS)
    await finalize_result_vote(session_id, reason="timeout")


async def finalize_result_vote(session_id: str, reason: str) -> None:
    session = ACTIVE_SESSIONS.get(session_id)
    if session is None:
        return
    guild = bot.get_guild(session.guild_id)
    if guild is None:
        await cancel_session(session_id, reason="guild_missing")
        return

    async with get_session_lock(session_id):
        session = ACTIVE_SESSIONS.get(session_id)
        if session is None:
            return
        if session.phase != "result_vote":
            return

        count1 = sum(1 for t in session.result_votes.values() if t == 1)
        count2 = sum(1 for t in session.result_votes.values() if t == 2)
        if count1 >= RESULT_VOTE_MAJORITY and count1 > count2:
            winner = 1
        elif count2 >= RESULT_VOTE_MAJORITY and count2 > count1:
            winner = 2
        else:
            winner = 0

        # Timeout with no 5/8 majority: do not close session; reset so they can start vote again
        if reason == "timeout" and winner == 0:
            session.phase = "in_match"
            session.result_votes.clear()
            session.result_started_ts = None

    # If timeout and no majority, send message and return without closing session
    if reason == "timeout" and winner == 0:
        match_text = guild.get_channel(session.match_text_channel_id)
        if isinstance(match_text, discord.TextChannel):
            await match_text.send(
                "Not enough players voted. Match remains open. You can start the winner vote again when ready."
            )
        return

    match_text = guild.get_channel(session.match_text_channel_id)
    if isinstance(match_text, discord.TextChannel):
        if winner == 0:
            msg = f"🏁 Result vote ended (`{reason}`). **No decision** (Team 1 = {count1}, Team 2 = {count2})."
        else:
            msg = (
                f"🏁 Result vote ended (`{reason}`). **Team {winner} wins!** "
                f"(Team 1 = {count1}, Team 2 = {count2})."
            )
        await match_text.send(msg)

    await end_match_and_cleanup(session_id)


async def cleanup_voice_channels(session: MatchSession) -> None:
    guild = bot.get_guild(session.guild_id)
    if guild is None:
        return

    for cid in [session.team1_voice_channel_id, session.team2_voice_channel_id]:
        if cid is None:
            continue
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.VoiceChannel):
            try:
                await ch.delete()
            except discord.Forbidden as e:
                print(f"Could not delete voice channel {cid}: {e}")


async def end_match_and_cleanup(session_id: str) -> None:
    session = ACTIVE_SESSIONS.get(session_id)
    if session is None:
        return
    guild = bot.get_guild(session.guild_id)
    if guild is None:
        ACTIVE_SESSIONS.pop(session_id, None)
        return

    # Move players to this queue's waiting room VC (or disconnect if not configured).
    cfg = QUEUE_CONFIGS.get(session.queue_text_channel_id)
    waiting_room: Optional[discord.VoiceChannel] = None
    if cfg is not None and cfg.waiting_room_voice_channel_id is not None:
        wch = guild.get_channel(cfg.waiting_room_voice_channel_id)
        if isinstance(wch, discord.VoiceChannel):
            waiting_room = wch

    for cid in [session.team1_voice_channel_id, session.team2_voice_channel_id]:
        if cid is None:
            continue
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.VoiceChannel):
            for m in list(ch.members):
                try:
                    await m.move_to(waiting_room)
                except discord.Forbidden:
                    continue

    await cleanup_voice_channels(session)

    if session.match_text_channel_created_by_bot and session.match_text_channel_id is not None:
        match_ch = guild.get_channel(session.match_text_channel_id)
        if isinstance(match_ch, discord.TextChannel):
            try:
                await match_ch.delete()
            except discord.Forbidden:
                pass

    queue_key = (session.guild_id, session.queue_text_channel_id)
    state = QUEUE_STATES.get(queue_key)
    if state is not None:
        async with get_queue_lock(queue_key):
            state.active_match_participants.difference_update(session.player_ids)

    # Refresh the existing queue message in this game's channel only (no new message, no other channels).
    if state is not None and state.current_message_id is not None and guild is not None:
        queue_ch = guild.get_channel(session.queue_text_channel_id)
        if isinstance(queue_ch, discord.TextChannel):
            try:
                msg = await queue_ch.fetch_message(state.current_message_id)
                await msg.edit(content=format_queue_message(state), view=QueueView(queue_key))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

    ACTIVE_SESSIONS.pop(session_id, None)
    SESSION_LOCKS.pop(session_id, None)


def _wager_disabled_embed() -> discord.Embed:
    embed = discord.Embed(
        title="Wager not configured",
        description="Set `WAGER_SOLANA_WALLET` in the bot's environment to enable wagers.",
        color=discord.Color.orange(),
    )
    return embed


async def _send_wager_response(
    destination: discord.abc.Messageable,
    amount: float,
    token_name: str,
    pay_url: str,
    reference: str,
    author_name: str,
    followup: Optional[Any] = None,
) -> None:
    send = followup.send if followup is not None else destination.send
    amt_str = str(int(amount)) if amount == int(amount) else str(amount)
    qr_bytes = generate_wager_qr_bytes(pay_url)
    f = discord.File(io.BytesIO(qr_bytes), filename="wager-qr.png")
    embed = discord.Embed(
        title=f"Wager: {amount} {token_name}",
        description=(
            f"**Scan the QR code** with your wallet app to pay **{amt_str} {token_name}**.\n\n"
            "Share this wager with your opponent so they can scan the same QR to match the amount. "
            "Both payments use the reference below so the recipient can match them."
        ),
        color=discord.Color.green(),
    )
    embed.set_image(url="attachment://wager-qr.png")
    ref_help = (
        f"`{reference}`\n\n"
        "This reference is attached to your payment so the recipient can identify it. "
        "They can look up incoming transactions to the wager wallet and match by this reference."
    )
    embed.add_field(name="Reference (for tracking)", value=ref_help[:1024], inline=False)
    embed.add_field(
        name="View incoming payments",
        value=f"[Open on Solscan](https://solscan.io/account/{WAGER_SOLANA_WALLET})",
        inline=False,
    )
    embed.set_footer(text=f"Requested by {author_name}")
    await send(embed=embed, file=f)


@bot.command(name="wager")
async def cmd_wager(ctx: commands.Context, amount: Optional[float] = None):
    """Create a Solana Pay QR for a wager. Usage: !wager <amount> (e.g. !wager 10 for 10 USDC/SOL)."""
    if not WAGER_SOLANA_WALLET:
        await ctx.send(embed=_wager_disabled_embed())
        return
    if amount is None or amount <= 0:
        await ctx.send("Usage: `!wager <amount>` (e.g. `!wager 10` for 10 USDC or 10 SOL).")
        return
    token_name = "USDC" if WAGER_SPL_MINT else "SOL"
    ref_bytes = secrets.token_bytes(32)
    reference_b58 = base58.b58encode(ref_bytes).decode("ascii")
    try:
        pay_url = build_solana_pay_url(
            recipient=WAGER_SOLANA_WALLET,
            amount=amount,
            reference_b58=reference_b58,
            spl_mint=WAGER_SPL_MINT,
            label=WAGER_LABEL,
            message=f"Wager {amount} {token_name}",
            trust_wallet_fix=False,
        )
    except ValueError as e:
        await ctx.send(str(e))
        return
    author_name = ctx.author.display_name or str(ctx.author)
    await _send_wager_response(ctx, amount, token_name, pay_url, reference_b58, author_name)


@bot.tree.command(name="wager", description="Generate a Solana Pay QR code to deposit your wager stake.")
async def slash_wager(interaction: discord.Interaction, amount: float):
    """Slash command: /wager <amount>."""
    if not WAGER_SOLANA_WALLET:
        await interaction.response.send_message(embed=_wager_disabled_embed(), ephemeral=True)
        return
    if amount <= 0:
        await interaction.response.send_message("Amount must be positive (e.g. 10 for 10 USDC or SOL).", ephemeral=True)
        return
    token_name = "USDC" if WAGER_SPL_MINT else "SOL"
    ref_bytes = secrets.token_bytes(32)
    reference_b58 = base58.b58encode(ref_bytes).decode("ascii")
    try:
        pay_url = build_solana_pay_url(
            recipient=WAGER_SOLANA_WALLET,
            amount=amount,
            reference_b58=reference_b58,
            spl_mint=WAGER_SPL_MINT,
            label=WAGER_LABEL,
            message=f"Wager {amount} {token_name}",
            trust_wallet_fix=False,
        )
    except ValueError as e:
        await interaction.response.send_message(str(e), ephemeral=True)
        return
    author_name = interaction.user.display_name or str(interaction.user)
    await interaction.response.defer(ephemeral=False)
    await _send_wager_response(
        interaction.channel,
        amount,
        token_name,
        pay_url,
        reference_b58,
        author_name,
        followup=interaction.followup,
    )


@bot.command(name="cancelmatch")
async def cmd_cancelmatch(ctx: commands.Context) -> None:
    """Start a cancel-match vote. Only works in this match's channel; only match players can vote."""
    session = get_session_by_match_channel(ctx.channel.id)
    if session is None:
        await ctx.send("This command can only be used in an active match channel.")
        return
    if ctx.author.id not in session.player_ids:
        await ctx.send("Only match players can start a cancel vote.")
        return
    async with get_session_lock(session.session_id):
        if session.cancel_vote_message_id is not None:
            await ctx.send("A cancel vote is already in progress. Vote in the message above.")
            return
        session.cancel_votes.clear()
        session.cancel_vote_started_ts = time.time()
    need = (len(session.player_ids) // 2) + 1
    msg = await ctx.send(
        f"**Cancel match?** Vote below. Need **{need}** of {len(session.player_ids)} players to cancel. "
        f"(Vote ends in {CANCEL_VOTE_SECONDS}s.)",
        view=CancelVoteView(session.session_id),
    )
    async with get_session_lock(session.session_id):
        session.cancel_vote_message_id = msg.id
    bot.loop.create_task(cancel_vote_timeout_task(session.session_id))


@bot.tree.command(name="cancelmatch", description="Start a vote to cancel this match (match channel only).")
async def slash_cancelmatch(interaction: discord.Interaction) -> None:
    session = get_session_by_match_channel(interaction.channel.id if interaction.channel else 0)
    if session is None:
        await interaction.response.send_message("This command can only be used in an active match channel.", ephemeral=True)
        return
    if interaction.user.id not in session.player_ids:
        await interaction.response.send_message("Only match players can start a cancel vote.", ephemeral=True)
        return
    async with get_session_lock(session.session_id):
        if session.cancel_vote_message_id is not None:
            await interaction.response.send_message("A cancel vote is already in progress. Vote in the message above.", ephemeral=True)
            return
        session.cancel_votes.clear()
        session.cancel_vote_started_ts = time.time()
    need = (len(session.player_ids) // 2) + 1
    await interaction.response.send_message(
        f"**Cancel match?** Vote below. Need **{need}** of {len(session.player_ids)} players to cancel. "
        f"(Vote ends in {CANCEL_VOTE_SECONDS}s.)",
        view=CancelVoteView(session.session_id),
    )
    msg = await interaction.original_response()
    async with get_session_lock(session.session_id):
        session.cancel_vote_message_id = msg.id
    bot.loop.create_task(cancel_vote_timeout_task(session.session_id))


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
) -> None:
    if after.channel is None:
        return
    channel_id = after.channel.id
    session: Optional[MatchSession] = None
    for s in ACTIVE_SESSIONS.values():
        if s.lobby_voice_channel_id == channel_id and s.phase == "lobby":
            session = s
            break
    if session is None:
        return
    member_ids_in_channel = {m.id for m in after.channel.members if not m.bot}
    if set(session.player_ids) <= member_ids_in_channel:
        await start_captain_vote_from_lobby(session.session_id)


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (discord.py {discord.__version__})")
    print(f"Queues loaded: {len(QUEUE_CONFIGS)}")
    try:
        synced = await bot.tree.sync()
        print(f"Slash commands synced: {len(synced)}")
    except Exception as e:
        print(f"Slash command sync failed: {e}")

    if getattr(bot, "_queue_messages_posted", False):
        return
    bot._queue_messages_posted = True  # type: ignore[attr-defined]

    expected_custom_id_prefix = "queue:join:"
    for cfg in QUEUE_CONFIGS.values():
        ch = bot.get_channel(cfg.queue_text_channel_id)
        if not isinstance(ch, discord.TextChannel):
            print(f"Queue text channel not found or not text: {cfg.queue_text_channel_id}")
            continue

        key = (ch.guild.id, ch.id)
        state = QueueState(
            guild_id=ch.guild.id,
            queue_text_channel_id=ch.id,
            match_text_channel_id=cfg.match_text_channel_id,
            category_id=cfg.category_id,
            name=cfg.name,
        )
        QUEUE_STATES[key] = state

        # Look for an existing queue message from us (avoid reposting on restart); restore queued users from content.
        existing_message_id: Optional[int] = None
        try:
            async for msg in ch.history(limit=30):
                if msg.author != bot.user:
                    continue
                for row in msg.components:
                    for child in getattr(row, "children", []):
                        cid = getattr(child, "custom_id", None)
                        if cid and cid.startswith(expected_custom_id_prefix):
                            parts = cid.split(":")
                            if len(parts) >= 4 and int(parts[2]) == ch.guild.id and int(parts[3]) == ch.id:
                                existing_message_id = msg.id
                                break
                    if existing_message_id is not None:
                        break
                if existing_message_id is not None:
                    break
        except (discord.Forbidden, discord.HTTPException):
            pass

        if existing_message_id is not None:
            state.current_message_id = existing_message_id
            bot.add_view(QueueView(key), message_id=existing_message_id)
            try:
                existing_msg = await ch.fetch_message(existing_message_id)
                # Restore queued user IDs from <@id> mentions in the message
                state.queued_user_ids = [int(uid) for uid in re.findall(r"<@!?(\d+)>", existing_msg.content)]
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
        else:
            msg = await ch.send(content=format_queue_message(state), view=QueueView(key))
            state.current_message_id = msg.id


def main():
    global QUEUE_CONFIGS
    QUEUE_CONFIGS = load_queue_configs()

    token = os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing DISCORD_TOKEN env var.")

    keep_alive()
    bot.run(token)


if __name__ == "__main__":
    main()
