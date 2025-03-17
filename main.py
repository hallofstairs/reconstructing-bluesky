# %% Imports

import json
import os
import typing as t
from datetime import datetime, timezone

import numpy as np

from utils import Like, Post, Record, records

# Constants

IN_DIR = "./data/firehose-2023-05-01"

REFRESH_SIZE = 20
MAX_POSTS_PER_USER = 60
MAX_POSTS_PER_SESSION = REFRESH_SIZE * 5
END_DATE = datetime(2023, 3, 1, tzinfo=timezone.utc)

SESSION_IDLE_THRESHOLD = 30  # minutes
SESSIONS_PATH = f"./data/sessions-{END_DATE.date()}.jsonl"

if os.path.exists(SESSIONS_PATH):
    os.remove(SESSIONS_PATH)


# %%


FeedType = t.Literal["following", "profile"]

# TODO: Make this more programmatic
BOTS = ["did:plc:fjsmdevv3mmzc3dpd36u5yxc"]


class Users:
    class Info(t.TypedDict):
        posts: list[str]
        following: list[str]
        last_interaction_ts: int | None
        feed: list[str]

    class FeedView(t.TypedDict):
        source: FeedType
        posts: list[Post]

    class Session(t.TypedDict):
        """Basic information about a session."""

        did: str
        session_num: int
        start_ts: int
        end_ts: int
        impressions: list[str]
        actions: list[Record]

    def __init__(self) -> None:
        self.info: dict[str, Users.Info] = {}
        self.sessions: dict[str, Users.Session] = {}

    def is_bot(self, did: str) -> bool:
        return did in BOTS

    def log_user(self, did: str) -> None:
        self.info[did] = {
            "following": [],
            "posts": [],
            "last_interaction_ts": -1,
            "feed": [],
        }

    def is_new_session(
        self, did: str, now_ts: int, idle_threshold: int = SESSION_IDLE_THRESHOLD
    ) -> bool:
        last_interaction_ts = self.info[did]["last_interaction_ts"]
        if last_interaction_ts is None:
            return True

        mins_since_last_record = (now_ts - last_interaction_ts) / 60_000
        return mins_since_last_record > idle_threshold

    def get_following_feed(self, did: str) -> list:
        """Reconstruct a user's chronolical Following feed at any given time."""
        feed: list[str] = []

        # If user has posted anything:
        if self.info[did]["posts"]:
            # Insert most recent K posts into their chron. feed
            feed.extend(list(self.info[did]["posts"])[-MAX_POSTS_PER_USER:])

        # Add posts from followings' timelines
        for following_did in self.info[did]["following"]:
            if self.info[following_did]["posts"]:
                feed.extend(
                    list(self.info[following_did]["posts"])[-MAX_POSTS_PER_USER:]
                )

        return sorted(feed, key=lambda x: x.split("/")[-1], reverse=True)[
            :MAX_POSTS_PER_SESSION
        ]

    # TODO: Verify, especially number of posts
    def get_profile_feed(self, subject_did: str) -> list:
        """Reconstruct a single user's timeline of posts."""
        return list(self.info[subject_did]["posts"])[-REFRESH_SIZE:][:REFRESH_SIZE]

    def log_session(self, did: str, now_ts: int) -> None:
        next_session_num = (
            self.sessions[did]["session_num"] + 1 if did in self.sessions else 0
        )

        # Write previous session to disk, if it exists
        if did in self.sessions:
            with open(SESSIONS_PATH, "a") as f:
                json.dump(self.sessions[did], f)
                f.write("\n")

        self.info[did]["feed"] = self.get_following_feed(did)
        self.sessions[did] = {
            "session_num": next_session_num,
            "did": did,
            "start_ts": now_ts,
            "end_ts": now_ts,
            "impressions": self.info[did]["feed"],
            "actions": [],
        }

    def dump_sessions(self) -> None:
        sorted_sessions = sorted(self.sessions.values(), key=lambda x: x["end_ts"])
        for session in sorted_sessions:
            with open(SESSIONS_PATH, "a") as f:
                json.dump(session, f)
                f.write("\n")

    def get_idx(self, val: str, vals: list[str]) -> int | None:
        try:
            idx = vals.index(val)
        except ValueError:
            idx = None

        return idx

    def log_like(self, did: str, subject_uri: str, record: Like) -> None:
        return
        # session_id = self.get_session_id(did)

        # Log the record
        # self.sessions[session_id]["actions"].append(record)

        # user_feed = self.info[did]["feed"]

        # Update seen in following
        # idx = self.get_idx(subject_uri, user_feed)
        # if idx:
        #     self.sessions[session_id]["impressions"] = self.info[did]["feed"][: idx + 1]

    def log_post(self, did: str, uri: str) -> None:
        if uri in self.info[did]["posts"]:  # This shouldn't happen, but precaution
            print(f"WARNING! Post {uri} already exists for user {did}")

        self.info[did]["posts"].append(uri)

    def log_follow(self, did: str, subject_did: str) -> None:
        if subject_did not in self.info:
            # TODO: I'm pretty sure this would be a deleted user?
            self.log_user(subject_did)

        # TODO:
        # Update session information
        # Start new profile view

        if subject_did in self.info[did]["following"]:
            return  # This happens semi-frequently, bug in data repos

        self.info[did]["following"].append(subject_did)
        self.sessions[did]["impressions"].extend(self.get_profile_feed(subject_did))


class Posts:
    class Info(t.TypedDict):
        parent_uri: str | None  # Allows thread reconstruction on-the-fly
        subject_uri: str | None

    def __init__(self) -> None:
        self.info: dict[str, Posts.Info] = {}
        self.deleted = set[str]()

    def log_post(self, uri: str) -> None:
        self.info[uri] = {
            "parent_uri": None,
            "subject_uri": None,
        }

    def log_deleted(self, uri: str) -> None:
        self.deleted.add(uri)


users = Users()
posts = Posts()

# === Firehose Iteration ===

# Iterate through each historical record in Bluesky's firehose
for record in records(IN_DIR, end_date=END_DATE):
    ts = record["ts"]
    did = record["did"]

    if did not in users.info:
        users.log_user(did)

    # Session management
    if did not in BOTS and users.is_new_session(did, ts):
        users.log_session(did, ts)

    if record["$type"] == "app.bsky.feed.post":
        # Skip replies and quotes, for now
        # if "reply" in record or "embed" in record:
        #     continue

        users.log_post(did, record["uri"])
        posts.log_post(record["uri"])

    if record["$type"] == "app.bsky.feed.like":
        if did not in BOTS:
            users.log_like(did, record["subject"]["uri"], record)

    if record["$type"] == "app.bsky.graph.follow":
        users.log_follow(did, record["subject"])

    users.info[did]["last_interaction_ts"] = ts

    if did not in BOTS:
        users.sessions[did]["end_ts"] = ts
        users.sessions[did]["actions"].append(record)

    # TODO: Blocks

    # Every time a user starts a new session, they land on their chronological feed screen
    # So, at the start of each session, we need to gather a user's feed up-to-date chron feed
    # Based on the actions they take during that session, we can guess how many of the posts
    #   from their chron feed they've seen, as well as other posts from other screens
    # If a user likes a post from their chronological feed:
    #   - Mark all posts until that idx as seen (TODO: Until end of refresh?)
    # If a user follows a user:
    #   - Mark the top N posts of that user as seen (TODO: What's N?) or last idx of liked, if liked
    # If a user replies to a post from their chronological feed, that is a new screen
    #   - Mark all parent replies in that thread as seen

    # If they take an action based on that feed, we:

users.dump_sessions()

# %% Validation


# Read in sessions file
with open(SESSIONS_PATH, "r") as f:
    data = [json.loads(line.strip()) for line in f if line.strip()]
    sessions = sorted(data, key=lambda x: x["end_ts"])


# TODO: nRank scoring

MIN_INTERACTIONS = 1

interactive_users = set()
precisions = []
recalls = []

for data in sessions:
    # Number of total interactions
    interactions = [
        record["subject"]["uri"]
        for record in data["actions"]
        if record["$type"] in ["app.bsky.feed.like", "app.bsky.feed.repost"]
    ]

    # Number of unique URIs interacted with
    interacted_uris = set(interactions)
    if len(interacted_uris) < MIN_INTERACTIONS:
        continue

    impression_uris = set(data["impressions"])
    interactive_users.add(data["did"])
    captured_uris = interacted_uris.intersection(impression_uris)

    recall = (
        len(captured_uris) / len(interacted_uris) if len(interacted_uris) > 0 else 0
    )
    precision = (
        len(captured_uris) / len(impression_uris) if len(impression_uris) > 0 else 0
    )
    precisions.append(precision)
    recalls.append(recall)

print("\n=== AGGREGATED FEED STATS ===")

mean_precision = np.mean(precisions)
mean_recall = np.mean(recalls)

var_precision = np.var(precisions)
var_recall = np.var(recalls)

# Calculate median precision and recall
median_precision = np.median(precisions)
median_recall = np.median(recalls)

print(
    f"- # sessions: {len(sessions)} "
    f"(mean: {len(sessions) / len(users.info):.1f} sessions/user)"
)
print(
    f"- # interactive users: {len(interactive_users)}/{len(users.info)} "
    f"({len(interactive_users) / len(users.info):.4f}%)"
)
print(
    f"- Mean Precision: {mean_precision:.4f} (variance: {var_precision:.4f}), Median: {median_precision:.4f}"
)
print(
    f"- Mean Recall: {mean_recall:.4f} (variance: {var_recall:.4f}), Median: {median_recall:.4f}"
)

# %%
