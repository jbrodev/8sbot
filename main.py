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


@dataclass(frozen=True)
class QueueConfig:
    queue_text_channel_id: int
    match_text_channel_id: int
    category_id: Optional[int] = None
    name: Optional[str] = None


@dataclass
class MatchSession:
    guild_id: int
    queue_text_channel_id: int
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


@dataclass
class QueueState:
    guild_id: int
    queue_text_channel_id: int
    match_text_channel_id: int
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
        if "queue_text_channel_id" not in item or "match_text_channel_id" not in item:
            raise RuntimeError(
                f"QUEUES_JSON[{idx}] must include queue_text_channel_id and match_text_channel_id."
            )
        qid = int(item["queue_text_channel_id"])
        mid = int(item["match_text_channel_id"])
        cid = int(item["category_id"]) if "category_id" in item and item["category_id"] is not None else None
        name = str(item["name"]) if "name" in item and item["name"] is not None else None

        if qid in configs:
            raise RuntimeError(f"Duplicate queue_text_channel_id in QUEUES_JSON: {qid}")
        configs[qid] = QueueConfig(
            queue_text_channel_id=qid,
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
QUEUE_STATES: dict[tuple[int, int], QueueState] = {}
ACTIVE_SESSIONS: dict[str, MatchSession] = {}
QUEUE_LOCKS: dict[tuple[int, int], asyncio.Lock] = {}
SESSION_LOCKS: dict[str, asyncio.Lock] = {}

VOTE_SECONDS = _env_int("VOTE_SECONDS", 60)
DRAFT_SECONDS = _env_int("DRAFT_SECONDS", 180)
RESULT_VOTE_SECONDS = _env_int("RESULT_VOTE_SECONDS", 60)


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


async def resolve_display_names(guild: discord.Guild, ids: list[int]) -> dict[int, str]:
    """Resolve display names for button labels; fetch member if not in cache. Never returns raw IDs."""
    result: dict[int, str] = {}
    for pid in ids:
        m = guild.get_member(pid)
        if m is None:
            try:
                m = await guild.fetch_member(pid)
            except (discord.NotFound, discord.HTTPException):
                pass
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
            "Vote which team won:",
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

            done = len(session.result_votes) >= len(session.player_ids)

        await interaction.response.send_message(
            f"Vote recorded. Current tally: Team 1 = {count1}, Team 2 = {count2}.",
            ephemeral=True,
        )

        if done:
            await finalize_result_vote(self.session_id, reason="all_votes_in")


class ResultVoteView(View):
    def __init__(self, session_id: str):
        super().__init__(timeout=None)
        self.add_item(ResultVoteButton(session_id, 1, "Team 1 won"))
        self.add_item(ResultVoteButton(session_id, 2, "Team 2 won"))


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
        # Release players back into queue eligibility.
        async with get_queue_lock((state.guild_id, state.queue_text_channel_id)):
            state.active_match_participants.difference_update(player_ids)
        return

    match_text_channel = guild.get_channel(state.match_text_channel_id)
    if not isinstance(match_text_channel, discord.TextChannel):
        async with get_queue_lock((state.guild_id, state.queue_text_channel_id)):
            state.active_match_participants.difference_update(player_ids)
        return

    session_id = uuid.uuid4().hex[:6]
    session = MatchSession(
        guild_id=guild.id,
        queue_text_channel_id=state.queue_text_channel_id,
        match_text_channel_id=state.match_text_channel_id,
        category_id=state.category_id,
        session_id=session_id,
        player_ids=player_ids,
    )
    ACTIVE_SESSIONS[session_id] = session
    get_session_lock(session_id)

    queue_channel = guild.get_channel(state.queue_text_channel_id)
    queue_name = queue_channel.mention if isinstance(queue_channel, discord.TextChannel) else "the queue"

    captain_display_names = await resolve_display_names(guild, session.player_ids)
    vote_view = CaptainVoteView(session_id, guild, session.player_ids, captain_display_names)
    msg = await match_text_channel.send(
        f"🔥 **{MATCH_SIZE} players queued** in {queue_name}.\n\n"
        f"Each player vote for **2 captains** (2 clicks total). Voting ends in **{VOTE_SECONDS}s**.\n\n"
        f"Players: {render_players(guild, session.player_ids)}",
        view=vote_view,
    )
    session.vote_message_id = msg.id

    bot.loop.create_task(captain_vote_timeout_task(session_id))


async def cancel_session(session_id: str, reason: str) -> None:
    session = ACTIVE_SESSIONS.get(session_id)
    if session is None:
        return

    guild = bot.get_guild(session.guild_id)
    if guild is not None:
        match_text = guild.get_channel(session.match_text_channel_id)
        if isinstance(match_text, discord.TextChannel):
            await match_text.send(f"⚠️ Match cancelled (`{reason}`).")

    queue_key = (session.guild_id, session.queue_text_channel_id)
    state = QUEUE_STATES.get(queue_key)
    if state is not None:
        async with get_queue_lock(queue_key):
            state.active_match_participants.difference_update(session.player_ids)

    await cleanup_voice_channels(session)
    ACTIVE_SESSIONS.pop(session_id, None)
    SESSION_LOCKS.pop(session_id, None)


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
            m = guild.get_member(pid)
            if m is None:
                try:
                    m = await guild.fetch_member(pid)
                except (discord.NotFound, discord.HTTPException):
                    continue
            if m is not None:
                overwrites[m] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
        return overwrites

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

    queue_key = (session.guild_id, session.queue_text_channel_id)
    state = QUEUE_STATES.get(queue_key)
    if state is not None:
        async with get_queue_lock(queue_key):
            state.active_match_participants.difference_update(session.player_ids)

    ACTIVE_SESSIONS.pop(session_id, None)
    SESSION_LOCKS.pop(session_id, None)


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (discord.py {discord.__version__})")
    print(f"Queues loaded: {len(QUEUE_CONFIGS)}")

    if getattr(bot, "_queue_messages_posted", False):
        return
    bot._queue_messages_posted = True  # type: ignore[attr-defined]

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
