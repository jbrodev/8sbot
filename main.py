import asyncio
import json
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

import discord
from discord.ext import commands
from discord.ui import Button, View

from keep_alive import keep_alive


MATCH_SIZE = 8


@dataclass(frozen=True)
class QueueConfig:
    queue_voice_channel_id: int
    match_text_channel_id: int
    category_id: Optional[int] = None
    name: Optional[str] = None


@dataclass
class MatchSession:
    guild_id: int
    queue_voice_channel_id: int
    match_text_channel_id: int
    category_id: Optional[int]
    session_id: str
    player_ids: list[int]

    phase: str = "captain_vote"  # captain_vote | draft | in_match | result_vote

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

    vote_message_id: Optional[int] = None
    draft_message_id: Optional[int] = None
    result_message_id: Optional[int] = None

    result_votes: dict[int, int] = field(default_factory=dict)  # voter_id -> team (1/2)
    result_started_ts: Optional[float] = None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def load_queue_configs() -> dict[int, QueueConfig]:
    raw = os.getenv("QUEUES_JSON", "").strip()
    if not raw:
        raise RuntimeError("Missing QUEUES_JSON env var.")

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
        if "queue_voice_channel_id" not in item or "match_text_channel_id" not in item:
            raise RuntimeError(
                f"QUEUES_JSON[{idx}] must include queue_voice_channel_id and match_text_channel_id."
            )
        qid = int(item["queue_voice_channel_id"])
        mid = int(item["match_text_channel_id"])
        cid = int(item["category_id"]) if "category_id" in item and item["category_id"] is not None else None
        name = str(item["name"]) if "name" in item and item["name"] is not None else None

        if qid in configs:
            raise RuntimeError(f"Duplicate queue_voice_channel_id in QUEUES_JSON: {qid}")
        configs[qid] = QueueConfig(
            queue_voice_channel_id=qid,
            match_text_channel_id=mid,
            category_id=cid,
            name=name,
        )

    return configs


intents = discord.Intents.default()
intents.voice_states = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


QUEUE_CONFIGS: dict[int, QueueConfig] = {}
ACTIVE_SESSIONS: dict[tuple[int, int], MatchSession] = {}
SESSION_LOCKS: dict[tuple[int, int], asyncio.Lock] = {}

VOTE_SECONDS = _env_int("VOTE_SECONDS", 60)
DRAFT_SECONDS = _env_int("DRAFT_SECONDS", 180)
RESULT_VOTE_SECONDS = _env_int("RESULT_VOTE_SECONDS", 60)


def get_session_key(guild_id: int, queue_voice_channel_id: int) -> tuple[int, int]:
    return (guild_id, queue_voice_channel_id)


def get_lock(key: tuple[int, int]) -> asyncio.Lock:
    lock = SESSION_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        SESSION_LOCKS[key] = lock
    return lock


async def set_queue_locked(queue_channel: discord.VoiceChannel, locked: bool) -> None:
    default_role = queue_channel.guild.default_role
    overwrite = queue_channel.overwrites_for(default_role)
    overwrite.connect = False if locked else None
    await queue_channel.set_permissions(default_role, overwrite=overwrite)


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


class VoteCaptainButton(Button):
    def __init__(self, session_key: tuple[int, int], session_id: str, candidate_id: int, label: str):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label=label,
            custom_id=f"capvote:{session_id}:{candidate_id}",
        )
        self.session_key = session_key
        self.candidate_id = candidate_id

    async def callback(self, interaction: discord.Interaction) -> None:
        session = ACTIVE_SESSIONS.get(self.session_key)
        if session is None:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return

        async with get_lock(self.session_key):
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
                await finalize_captain_vote(self.session_key, reason="all_votes_in")


class CaptainVoteView(View):
    def __init__(self, session_key: tuple[int, int], session_id: str, guild: discord.Guild, player_ids: list[int]):
        super().__init__(timeout=None)
        self.session_key = session_key
        self.session_id = session_id

        for pid in player_ids:
            member = guild.get_member(pid)
            label = member.display_name if member else str(pid)
            if len(label) > 80:
                label = label[:77] + "..."
            self.add_item(VoteCaptainButton(session_key, session_id, pid, label))


class DraftPickButton(Button):
    def __init__(self, session_key: tuple[int, int], session_id: str, pick_id: int, label: str):
        super().__init__(
            style=discord.ButtonStyle.success,
            label=label,
            custom_id=f"draftpick:{session_id}:{pick_id}",
        )
        self.session_key = session_key
        self.pick_id = pick_id

    async def callback(self, interaction: discord.Interaction) -> None:
        session = ACTIVE_SESSIONS.get(self.session_key)
        if session is None:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return

        async with get_lock(self.session_key):
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

            await interaction.response.defer()
            await apply_pick_and_advance(self.session_key, picked_id=self.pick_id)


class DraftPickView(View):
    def __init__(self, session_key: tuple[int, int], session_id: str, guild: discord.Guild, remaining_ids: list[int]):
        super().__init__(timeout=None)
        self.session_key = session_key
        self.session_id = session_id

        for pid in remaining_ids:
            member = guild.get_member(pid)
            label = member.display_name if member else str(pid)
            if len(label) > 80:
                label = label[:77] + "..."
            self.add_item(DraftPickButton(session_key, session_id, pid, label))


class StartResultVoteButton(Button):
    def __init__(self, session_key: tuple[int, int], session_id: str):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label="Start winner vote",
            custom_id=f"startresult:{session_id}",
        )
        self.session_key = session_key

    async def callback(self, interaction: discord.Interaction) -> None:
        session = ACTIVE_SESSIONS.get(self.session_key)
        if session is None:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return

        async with get_lock(self.session_key):
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
            "Vote which team won:",
            view=ResultVoteView(self.session_key, session.session_id),
        )
        bot.loop.create_task(result_vote_timeout_task(self.session_key))


class StartResultVoteView(View):
    def __init__(self, session_key: tuple[int, int], session_id: str):
        super().__init__(timeout=None)
        self.add_item(StartResultVoteButton(session_key, session_id))


class ResultVoteButton(Button):
    def __init__(self, session_key: tuple[int, int], session_id: str, team: int, label: str):
        super().__init__(
            style=discord.ButtonStyle.success if team == 1 else discord.ButtonStyle.danger,
            label=label,
            custom_id=f"result:{session_id}:{team}",
        )
        self.session_key = session_key
        self.team = team

    async def callback(self, interaction: discord.Interaction) -> None:
        session = ACTIVE_SESSIONS.get(self.session_key)
        if session is None:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return

        async with get_lock(self.session_key):
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

            done = len(session.result_votes) >= len(session.player_ids)

        await interaction.response.send_message(
            f"Vote recorded. Current tally: Team 1 = {count1}, Team 2 = {count2}.",
            ephemeral=True,
        )

        if done:
            await finalize_result_vote(self.session_key, reason="all_votes_in")


class ResultVoteView(View):
    def __init__(self, session_key: tuple[int, int], session_id: str):
        super().__init__(timeout=None)
        self.add_item(ResultVoteButton(session_key, session_id, 1, "Team 1 won"))
        self.add_item(ResultVoteButton(session_key, session_id, 2, "Team 2 won"))


async def start_match_if_ready(guild: discord.Guild, queue_channel: discord.VoiceChannel) -> None:
    cfg = QUEUE_CONFIGS.get(queue_channel.id)
    if cfg is None:
        return

    key = get_session_key(guild.id, queue_channel.id)
    if key in ACTIVE_SESSIONS:
        return

    players = [m for m in queue_channel.members if not m.bot]
    if len(players) != MATCH_SIZE:
        return

    session_id = uuid.uuid4().hex[:6]
    session = MatchSession(
        guild_id=guild.id,
        queue_voice_channel_id=queue_channel.id,
        match_text_channel_id=cfg.match_text_channel_id,
        category_id=cfg.category_id,
        session_id=session_id,
        player_ids=[m.id for m in players],
    )
    ACTIVE_SESSIONS[key] = session
    get_lock(key)

    try:
        await set_queue_locked(queue_channel, True)
    except discord.Forbidden:
        pass

    match_text_channel = guild.get_channel(cfg.match_text_channel_id)
    if not isinstance(match_text_channel, discord.TextChannel):
        await cancel_session(key, reason="invalid_match_text_channel")
        return

    vote_view = CaptainVoteView(key, session_id, guild, session.player_ids)
    msg = await match_text_channel.send(
        f"🔥 **8 players found** in `{queue_channel.name}`.\n\n"
        f"Each player vote for **2 captains** (2 clicks total). Voting ends in **{VOTE_SECONDS}s**.\n\n"
        f"Players: {render_players(guild, session.player_ids)}",
        view=vote_view,
    )
    session.vote_message_id = msg.id

    bot.loop.create_task(captain_vote_timeout_task(key))


async def cancel_session(key: tuple[int, int], reason: str) -> None:
    session = ACTIVE_SESSIONS.get(key)
    if session is None:
        return

    guild = bot.get_guild(session.guild_id)
    if guild is not None:
        queue_channel = guild.get_channel(session.queue_voice_channel_id)
        if isinstance(queue_channel, discord.VoiceChannel):
            try:
                await set_queue_locked(queue_channel, False)
            except discord.Forbidden:
                pass

        match_text = guild.get_channel(session.match_text_channel_id)
        if isinstance(match_text, discord.TextChannel):
            await match_text.send(f"⚠️ Match cancelled (`{reason}`). Queue unlocked.")

    await cleanup_voice_channels(session)
    ACTIVE_SESSIONS.pop(key, None)


async def captain_vote_timeout_task(key: tuple[int, int]) -> None:
    await asyncio.sleep(VOTE_SECONDS)
    await finalize_captain_vote(key, reason="timeout")


async def finalize_captain_vote(key: tuple[int, int], reason: str) -> None:
    session = ACTIVE_SESSIONS.get(key)
    if session is None:
        return

    async with get_lock(key):
        if session.phase != "captain_vote":
            return

        guild = bot.get_guild(session.guild_id)
        if guild is None:
            await cancel_session(key, reason="guild_missing")
            return

        captains, counts = choose_two_captains(session.player_ids, session.captain_votes)
        session.captain_ids = captains
        cap1, cap2 = captains

        session.team1_ids = [cap1]
        session.team2_ids = [cap2]
        session.remaining_ids = set(session.player_ids) - {cap1, cap2}

        cap1_votes = counts.get(cap1, 0)
        cap2_votes = counts.get(cap2, 0)
        if cap1_votes > cap2_votes:
            first_team = 1
        elif cap2_votes > cap1_votes:
            first_team = 2
        else:
            first_team = random.choice([1, 2])

        session.pick_order = build_pick_order(first_team)
        session.pick_index = 0
        session.last_pick_ts = time.time()
        session.phase = "draft"

    match_text = guild.get_channel(session.match_text_channel_id)
    if not isinstance(match_text, discord.TextChannel):
        await cancel_session(key, reason="invalid_match_text_channel")
        return

    remaining_sorted = sorted(session.remaining_ids)
    draft_view = DraftPickView(key, session.session_id, guild, remaining_sorted)

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

    bot.loop.create_task(draft_timeout_task(key))


async def draft_timeout_task(key: tuple[int, int]) -> None:
    while True:
        session = ACTIVE_SESSIONS.get(key)
        if session is None:
            return
        if session.phase != "draft":
            return

        await asyncio.sleep(2)
        now = time.time()
        async with get_lock(key):
            session = ACTIVE_SESSIONS.get(key)
            if session is None or session.phase != "draft":
                return
            if not session.remaining_ids:
                return
            if now - session.last_pick_ts < DRAFT_SECONDS:
                continue

            picked_id = random.choice(list(session.remaining_ids))

        await apply_pick_and_advance(key, picked_id=picked_id, autopick=True)


async def apply_pick_and_advance(key: tuple[int, int], picked_id: int, autopick: bool = False) -> None:
    session = ACTIVE_SESSIONS.get(key)
    if session is None:
        return
    guild = bot.get_guild(session.guild_id)
    if guild is None:
        await cancel_session(key, reason="guild_missing")
        return

    async with get_lock(key):
        session = ACTIVE_SESSIONS.get(key)
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

        done = session.pick_index >= len(session.pick_order) or not session.remaining_ids
        if done:
            session.phase = "in_match"

    match_text = guild.get_channel(session.match_text_channel_id)
    if not isinstance(match_text, discord.TextChannel):
        await cancel_session(key, reason="invalid_match_text_channel")
        return

    if session.draft_message_id is None:
        await cancel_session(key, reason="missing_draft_message")
        return

    try:
        msg = await match_text.fetch_message(session.draft_message_id)
    except discord.NotFound:
        msg = None

    picked_member = guild.get_member(picked_id)
    picked_label = picked_member.mention if picked_member else f"<@{picked_id}>"

    if session.phase != "in_match":
        remaining_sorted = sorted(session.remaining_ids)
        draft_view = DraftPickView(key, session.session_id, guild, remaining_sorted)

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

    await create_team_channels_and_move(key)


async def create_team_channels_and_move(key: tuple[int, int]) -> None:
    session = ACTIVE_SESSIONS.get(key)
    if session is None:
        return
    guild = bot.get_guild(session.guild_id)
    if guild is None:
        await cancel_session(key, reason="guild_missing")
        return

    queue_channel = guild.get_channel(session.queue_voice_channel_id)
    if not isinstance(queue_channel, discord.VoiceChannel):
        await cancel_session(key, reason="invalid_queue_channel")
        return

    category: Optional[discord.CategoryChannel] = None
    if session.category_id is not None:
        ch = guild.get_channel(session.category_id)
        if isinstance(ch, discord.CategoryChannel):
            category = ch
    if category is None:
        category = queue_channel.category

    def overwrites_for_team(team_ids: list[int]) -> dict[Any, discord.PermissionOverwrite]:
        overwrites: dict[Any, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False)
        }
        for pid in team_ids:
            m = guild.get_member(pid)
            if m is not None:
                overwrites[m] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
        return overwrites

    base_name = f"8s-{session.session_id}"
    team1_name = f"{base_name}-team1"
    team2_name = f"{base_name}-team2"

    try:
        team1_chan = await guild.create_voice_channel(
            name=team1_name,
            category=category,
            overwrites=overwrites_for_team(session.team1_ids),
        )
        team2_chan = await guild.create_voice_channel(
            name=team2_name,
            category=category,
            overwrites=overwrites_for_team(session.team2_ids),
        )
    except discord.Forbidden:
        await cancel_session(key, reason="missing_permissions_create_channels")
        return

    async with get_lock(key):
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
            f"When the match ends, click below to start the winner vote.",
            view=StartResultVoteView(key, session.session_id),
        )


async def result_vote_timeout_task(key: tuple[int, int]) -> None:
    await asyncio.sleep(RESULT_VOTE_SECONDS)
    await finalize_result_vote(key, reason="timeout")


async def finalize_result_vote(key: tuple[int, int], reason: str) -> None:
    session = ACTIVE_SESSIONS.get(key)
    if session is None:
        return
    guild = bot.get_guild(session.guild_id)
    if guild is None:
        await cancel_session(key, reason="guild_missing")
        return

    async with get_lock(key):
        session = ACTIVE_SESSIONS.get(key)
        if session is None:
            return
        if session.phase != "result_vote":
            return

        count1 = sum(1 for t in session.result_votes.values() if t == 1)
        count2 = sum(1 for t in session.result_votes.values() if t == 2)
        if count1 > count2:
            winner = 1
        elif count2 > count1:
            winner = 2
        else:
            winner = 0

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

    await end_match_and_cleanup(key)


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
            except discord.Forbidden:
                pass


async def end_match_and_cleanup(key: tuple[int, int]) -> None:
    session = ACTIVE_SESSIONS.get(key)
    if session is None:
        return
    guild = bot.get_guild(session.guild_id)
    if guild is None:
        ACTIVE_SESSIONS.pop(key, None)
        return

    # Disconnect any remaining users in temp channels, then delete channels.
    for cid in [session.team1_voice_channel_id, session.team2_voice_channel_id]:
        if cid is None:
            continue
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.VoiceChannel):
            for m in list(ch.members):
                try:
                    await m.move_to(None)
                except discord.Forbidden:
                    continue

    await cleanup_voice_channels(session)

    queue_channel = guild.get_channel(session.queue_voice_channel_id)
    if isinstance(queue_channel, discord.VoiceChannel):
        try:
            await set_queue_locked(queue_channel, False)
        except discord.Forbidden:
            pass

    ACTIVE_SESSIONS.pop(key, None)


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    # Leaving a configured queue during captain vote/draft cancels that queue's match.
    if before.channel and isinstance(before.channel, discord.VoiceChannel) and before.channel.id in QUEUE_CONFIGS:
        key = get_session_key(member.guild.id, before.channel.id)
        session = ACTIVE_SESSIONS.get(key)
        if session is not None and session.phase in {"captain_vote", "draft"} and member.id in session.player_ids:
            # Re-check current non-bot members in queue channel.
            players_now = [m for m in before.channel.members if not m.bot]
            if len(players_now) < MATCH_SIZE:
                await cancel_session(key, reason="player_left_queue")

    # Joining a configured queue may trigger match start.
    if after.channel and isinstance(after.channel, discord.VoiceChannel):
        await start_match_if_ready(member.guild, after.channel)


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (discord.py {discord.__version__})")
    print(f"Queues loaded: {len(QUEUE_CONFIGS)}")


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
