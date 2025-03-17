import mmap
import os
import typing as t
from datetime import datetime
from enum import Enum

import ujson as json
from tqdm import tqdm

T = t.TypeVar("T")

# === Iteration utils ===


class jsonl[T]:
    @classmethod
    def iter(cls, path: str) -> t.Generator[T, None, None]:
        with open(path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                for line in iter(mm.readline, b""):
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        print(f"JSONDecodeError: {line}")
                        continue


def records(
    stream_path: str, end_date: str = "2023-07-01", batch_size: int = 1_000_000
) -> t.Generator["Record", None, None]:
    """
    Generator that yields records from the stream for the given date range.

    End date is inclusive.
    """

    files = sorted(
        [f for f in os.listdir(stream_path) if f.endswith(".json")],
        key=lambda x: int(x.split(".")[0]),
    )
    for filename in tqdm(files, total=len(files) * batch_size):
        with open(f"{stream_path}/{filename}", "r") as f:
            records: list[Record] = json.load(f)["records"]
            for record in records:
                if record["ts"] > int(
                    datetime.fromisoformat(end_date).timestamp() * 1_000
                ):
                    break
                yield record


# === Data types ===


class CidUri(t.TypedDict):
    cid: str
    uri: str


ExistingPost = t.TypedDict(
    "ExistingPost",
    {
        "$type": t.Literal["app.bsky.feed.post"],
        "ts": int,
        "did": str,
        "uri": str,
        "text": str,
        "createdAt": str,
        "reply": t.Optional[dict[t.Literal["root", "parent"], CidUri]],
        "embed": t.Optional[dict[str, t.Any]],
    },
)

DeletedPost = t.TypedDict(
    "DeletedPost",
    {
        "$type": t.Literal["app.bsky.feed.post"],
        "ts": int,
        "did": str,
        "uri": str,
        "deleted": t.Literal[True],
    },
)

Follow = t.TypedDict(
    "Follow",
    {
        "$type": t.Literal["app.bsky.graph.follow"],
        "ts": int,
        "did": str,  # DID of the follower
        "uri": str,  # URI of the follow record
        "createdAt": str,  # Timestamp of the follow
        "subject": str,  # DID of the followed user
    },
)

Repost = t.TypedDict(
    "Repost",
    {
        "$type": t.Literal["app.bsky.feed.repost"],
        "ts": int,
        "did": str,
        "uri": str,
        "createdAt": str,
        "subject": CidUri,
    },
)

Like = t.TypedDict(
    "Like",
    {
        "$type": t.Literal["app.bsky.feed.like"],
        "ts": int,
        "did": str,
        "uri": str,
        "createdAt": str,
        "subject": CidUri,
    },
)

Block = t.TypedDict(
    "Block",
    {
        "$type": t.Literal["app.bsky.graph.block"],
        "ts": int,
        "did": str,
        "uri": str,
        "createdAt": str,
        "subject": str,
    },
)

Profile = t.TypedDict(
    "Profile",
    {
        "$type": t.Literal["app.bsky.actor.profile"],
        "ts": int,
        "did": str,
        "createdAt": str,
    },
)

Post = ExistingPost | DeletedPost
Record = Post | Follow | Repost | Like | Block | Profile


# === Data utils ===


class TimeFormat(str, Enum):
    minute = "%Y-%m-%dT%H:%M"
    hourly = "%Y-%m-%dT%H"
    daily = "%Y-%m-%d"
    weekly = "%Y-%W"
    monthly = "%Y-%m"


def truncate_timestamp(timestamp: str, format: TimeFormat) -> str:
    """
    Get the relevant subset of a timestamp for a given grouping.

    e.g. "2023-01-01" for "daily, "2023-01" for "monthly"
    """
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).strftime(format)


def did_from_uri(uri: str) -> str:
    if not uri:
        raise ValueError("\nMisformatted URI (empty string)")

    try:
        return uri.split("/")[2]
    except Exception:
        raise ValueError(f"\nMisformatted URI: {uri}")


def rkey_from_uri(uri: str) -> str:
    return uri.split("/")[-1]


class s32:
    S32_CHAR = "234567abcdefghijklmnopqrstuvwxyz"

    @classmethod
    def encode(cls, i: int) -> str:
        s = ""
        while i:
            c = i % 32
            i = i // 32
            s = s32.S32_CHAR[c] + s
        return s

    @classmethod
    def decode(cls, s: str) -> int:
        i = 0
        for c in s:
            i = i * 32 + s32.S32_CHAR.index(c)
        return i


def parse_rkey(rev: str) -> tuple[int, int]:
    """Extract the data from the rkey of a URI. Returns (timestamp, clock_id) tuple.
    timestamp is Unix timestamp in microseconds."""

    timestamp = s32.decode(rev[:-2])  # unix, microseconds
    clock_id = s32.decode(rev[-2:])

    return timestamp, clock_id
