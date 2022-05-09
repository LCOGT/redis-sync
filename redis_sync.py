import asyncio

import click
import redis.asyncio

from collections.abc import AsyncIterator


@click.command()
@click.option("--src-url",
    required=True,
    help="e.g rediss://[[username]:[password]]@localhost:6379/?db=0"
)
@click.option("--dst-url",
    required=True,
    help="e.g rediss://[[username]:[password]]@localhost:6379/?db=0"
)
@click.option("--match-keys",
    type=str,
    default=None,
    help="match pattern for keys (default: all keys)"
)
@click.option("--parallel",
    type=click.IntRange(1, 1000),
    default=10,
    show_default=True,
    help="number keys to copy in parallel"
)
@click.option("--dst-replace/--dst-no-replace",
    default=True,
    show_default=True,
    help="whether to replace the key on DST if it already exists"
)
def cli(*args, **kwargs) -> None:
    """Copy keys (and TTL metadata) from one Redis to another."""
    asyncio.run(copy(*args, **kwargs))


async def copy(
    src_url: str,
    dst_url: str,
    parallel: int,
    match_keys: str | None,
    dst_replace: bool,
) -> None:
    src = redis.asyncio.from_url(src_url)
    await src.ping()
    click.echo(f"Connected to SRC")

    dst = redis.asyncio.from_url(dst_url)
    await dst.ping()
    click.echo(f"Connected to DST")

    async for keys in scan_iter_batch(src, match=match_keys, count=parallel):
        tasks = [copy_one(k, src, dst, dst_replace) for k in keys]
        await asyncio.gather(*tasks)

    await src.close()
    await dst.close()
    click.echo("Done")


async def scan_iter_batch(
    r: redis.asyncio.Redis,
    **kwargs
) -> AsyncIterator:
    cursor = "0"
    while cursor != 0:
        cursor, keys = await r.scan(cursor=cursor, **kwargs)
        yield keys


async def copy_one(
    key: bytes,
    src: redis.asyncio.Redis,
    dst: redis.asyncio.Redis,
    replace: bool,
) -> None:
    src_dump = await src.dump(key)
    await dst.restore(key, 0, src_dump, replace=replace)

    # restore TTL info
    src_ttl = await src.pttl(key)
    if src_ttl > 0:
        await dst.pexpire(key, src_ttl)

    key_fmt = key.decode("unicode_escape")
    ttl_fmt = f"{src_ttl} ms" if src_ttl > 0 else "n/a"
    click.echo(f"Copied key '{key_fmt}' (TTL: {ttl_fmt})")
