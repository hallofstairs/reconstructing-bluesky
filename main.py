# %% Imports

import datetime
import typing as t

import numpy as np

from utils import Like, Post, Record, records

# %%
# TODO: Can write session information to disk after end of session to save memory

# Constants

RAW_STREAM_DIR = "./data/stream-2023-07-01"

REFRESH_SIZE = 20
MAX_POSTS_PER_USER = 60
MAX_POSTS_PER_SESSION = REFRESH_SIZE * 5

SESSION_IDLE_THRESHOLD = 30 * 60  # 30 minutes
END_DATE = "2023-02-01"

# TODO: You can also reconstruct deleted post users, timestamps from URI


def to_ts(created_at: str) -> int:
    return int(datetime.datetime.fromisoformat(created_at).timestamp())


# === Prediction Tracking ===

# What posts do we predict that each user has seen?
predicted_seen: dict[str, list[tuple[str, str]]] = {}

# What posts do we know that each user has seen based on their interactions?
true_seen: dict[str, list[tuple[str, str]]] = {}


FeedType = t.Literal["following", "profile"]


class Users:
    class Info(t.TypedDict):
        posts: list[str]
        following: list[str]
        curr_session_num: int
        last_interaction_ts: int | None
        feed: list[str]

    class FeedView(t.TypedDict):
        source: FeedType
        posts: list[Post]

    class Session(t.TypedDict):
        """Basic information about a session."""

        did: str
        start_ts: int
        end_ts: int
        impressions: list[str]
        actions: list[Record]

    def __init__(self) -> None:
        self.info: dict[str, Users.Info] = {}
        self.sessions: dict[str, Users.Session] = {}

    def log_user(self, did: str) -> None:
        self.info[did] = {
            "following": [],
            "posts": [],
            "curr_session_num": 0,
            "last_interaction_ts": -1,
            "feed": [],
        }

    def log_post(self, did: str, uri: str) -> None:
        if uri in self.info[did]["posts"]:  # This shouldn't happen, but precaution
            print(f"WARNING! Post {uri} already exists for user {did}")

        self.info[did]["posts"].append(uri)
        self.sessions[self.get_session_id(did)]["actions"].append(record)

    def log_follow(self, did: str, subject_did: str) -> None:
        if subject_did not in self.info:
            # TODO: I'm pretty sure this would be a deleted user?
            self.log_user(subject_did)

        # TODO:
        # Update session information
        # User followed another user, meaning they saw their profile
        # Start new profile view

        if subject_did in self.info[did]["following"]:
            return  # This happens semi-frequently, bug in data repos

        self.info[did]["following"].append(subject_did)
        self.sessions[self.get_session_id(did)]["actions"].append(record)

    def get_session_id(self, did: str) -> str:
        return f"{did}-{self.info[did]['curr_session_num']}"

    def is_new_session(
        self, did: str, now_ts: int, idle_threshold: int = SESSION_IDLE_THRESHOLD
    ) -> bool:
        last_interaction_ts = self.info[did]["last_interaction_ts"]
        if last_interaction_ts is None:
            return True

        time_since_last_record = now_ts - last_interaction_ts
        return time_since_last_record > idle_threshold

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
        return list(self.info[subject_did]["posts"])[-REFRESH_SIZE:][
            :MAX_POSTS_PER_SESSION
        ]

    def log_session(self, did: str, now_ts: int) -> None:
        self.info[did]["curr_session_num"] += 1
        session_id = self.get_session_id(did)
        self.info[did]["feed"] = self.get_following_feed(did)

        self.sessions[session_id] = {
            "did": did,
            "start_ts": now_ts,
            "end_ts": now_ts,
            "impressions": [],
            "actions": [],
        }

    def get_idx(self, val: str, vals: list[str]) -> int | None:
        try:
            idx = vals.index(val)
        except ValueError:
            idx = None

        return idx

    def log_like(self, did: str, subject_uri: str, record: Like) -> None:
        session_id = self.get_session_id(did)

        # Log the record
        self.sessions[session_id]["actions"].append(record)

        user_feed = self.info[did]["feed"]

        # Update seen in following
        idx = self.get_idx(subject_uri, user_feed)
        if idx:
            self.sessions[session_id]["impressions"] = self.info[did]["feed"][: idx + 1]

        self.sessions[self.get_session_id(did)]["actions"].append(record)


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
for record in records(RAW_STREAM_DIR, end_date=END_DATE, log=True):
    now_ts = to_ts(record["createdAt"])
    did = record["did"]

    if did not in users.info:
        users.log_user(did)

    # Session management
    if users.is_new_session(did, now_ts):
        users.log_session(did, now_ts)

    if record["$type"] == "app.bsky.feed.post":
        # Skip replies and quotes, for now
        if "reply" in record or "embed" in record:
            continue

        users.log_post(did, record["uri"])
        posts.log_post(record["uri"])

    if record["$type"] == "app.bsky.feed.like":
        users.log_like(did, record["subject"]["uri"], record)

    if record["$type"] == "app.bsky.graph.follow":
        users.log_follow(did, record["subject"])

    users.info[did]["last_interaction_ts"] = now_ts
    users.sessions[users.get_session_id(did)]["end_ts"] = now_ts

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

# TODO: Evaluation at the end -- compare records to captured
# ranking
# recall
# precision

# %%

t_did = "did:plc:hd5g6z5wauicendc3zilioir"

users.sessions[f"{t_did}-1"]

# %% Validate

MIN_INTERACTIONS = 1

n_interactive = 0
precisions = []
recalls = []
dids = []
n_interactions = []
n_true_seen = []

# TODO: Can check if post is in posts ref to see if deleted

for _id, data in users.sessions.items():
    true_uris = set(
        [
            record["uri"]
            for record in data["actions"]
            if record["$type"] == "app.bsky.feed.like"
        ]
    )

    if len(true_uris) < MIN_INTERACTIONS:
        continue

    impressions = set(data["impressions"])

    n_interactive += 1
    n_interactions.append(len(true_uris))
    dids.append(did)

    n_true_seen.append(len(true_uris))

    correct_preds = true_uris.intersection(pred_uris)

    precision = len(correct_preds) / len(pred_uris) if len(pred_uris) > 0 else 0
    recall = len(correct_preds) / len(true_uris) if len(true_uris) > 0 else 0

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
        f"- # interactive users: {n_interactive}/{len(true_seen)} "
        f"({n_interactive / len(true_seen):.4f}%)"
    )

    # Mean percentage of a user's reconstructed feed that was interacted with by them
    print(
        f"- Mean Precision: {mean_precision:.4f} (variance: {var_precision:.4f}), Median: {median_precision:.4f}"
    )

    # Mean percentage of a user's interactions that are included within their reconstructed feed
    print(
        f"- Mean Recall: {mean_recall:.4f} (variance: {var_recall:.4f}), Median: {median_recall:.4f}"
    )

    # Number of posts deleted from the network (estimated)
    print(
        f"- Deleted posts: {len(deleted_posts)}/{len(post_info)} ({len(deleted_posts) / len(post_info) * 100:.4f}%)"
    )
