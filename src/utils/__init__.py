import mmap
import sys
import time
import typing as t
from datetime import datetime, timedelta
from enum import Enum

import ujson as json

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
    stream_path: str,
    start_date: str = "2022-11-17",
    end_date: str = "2023-07-01",
    log: bool = True,
) -> t.Generator["Record", None, None]:
    """
    Generator that yields records from the stream for the given date range.

    End date is inclusive.
    """

    def generate_timestamps(start_date: str, end_date: str) -> list[str]:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        delta = end_dt - start_dt

        return [
            (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(delta.days + 1)
        ]

    for ts in tq(generate_timestamps(start_date, end_date), active=log):
        for record in jsonl[Record].iter(f"{stream_path}/{ts}.jsonl"):
            yield record


def tq(iterable: t.Iterable[T], active: bool = True) -> t.Generator[T, None, None]:
    total = len(iterable) if isinstance(iterable, t.Sized) else None

    start_time = time.time()
    estimated_time_remaining = 0

    for i, item in enumerate(iterable):
        if active:
            if total:
                elapsed_time = time.time() - start_time
                items_per_second = (i + 1) / elapsed_time if elapsed_time > 0 else 0
                estimated_time_remaining = (
                    (total - i - 1) / items_per_second if items_per_second > 0 else 0
                )
                sys.stdout.write(
                    f"\r{i + 1}/{total} ({((i + 1) / total) * 100:.2f}%) - {estimated_time_remaining / 60:.1f}m until done"
                )
                sys.stdout.flush()
            else:
                sys.stdout.write(f"\rProcessed: {i + 1}")
                sys.stdout.flush()

        yield item


# === Data types ===


class CidUri(t.TypedDict):
    cid: str
    uri: str


Post = t.TypedDict(
    "Post",
    {
        "$type": t.Literal["app.bsky.feed.post"],
        "did": str,
        "uri": str,
        "text": str,
        "createdAt": str,
        "reply": t.Optional[dict[t.Literal["root", "parent"], CidUri]],
        "embed": t.Optional[dict[str, t.Any]],
    },
)

Follow = t.TypedDict(
    "Follow",
    {
        "$type": t.Literal["app.bsky.graph.follow"],
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
        "did": str,
        "createdAt": str,
    },
)

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


def parse_rkey(rev: str) -> tuple[datetime, int]:
    """Extract the data from the rkey of a URI"""

    timestamp = s32.decode(rev[:-2])  # unix, microseconds
    clock_id = s32.decode(rev[-2:])

    timestamp = datetime.fromtimestamp(timestamp / 1_000_000)
    return timestamp, clock_id
