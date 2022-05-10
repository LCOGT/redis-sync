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
@click.option("--dst-flushdb/--dst-no-flushdb",
    default=False,
    show_default=True,
    help="whether to remove all keys on DST (issues a FLUSHDB !!!)"
)
@click.option("--silent/--no-silent",
    default=False,
    show_default=True,
    help="try to be silent"
)
@click.option("--prompts/--no-prompts",
    default=True,
    show_default=True,
    help="enable/disable prompts"
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
    dst_flushdb: bool,
    silent: bool,
    prompts: bool,
) -> None:
    src = redis.asyncio.from_url(src_url)
    await src.ping()
    click.echo(f"Connected to SRC")

    dst = redis.asyncio.from_url(dst_url)
    await dst.ping()
    click.echo(f"Connected to DST")

    try:
        if dst_flushdb:
            click.secho("Flushing DB on DST", fg="red")
            if prompts:
                click.confirm("Do you want to continue?", abort=True)
            await dst.flushdb()

        key_count = 0
        async for (i, keys) in scan_iter_batch(src, match=match_keys, count=parallel):
            tasks = [copy_one(k, src, dst, dst_replace, silent) for k in keys]
            await asyncio.gather(*tasks)
            key_count = key_count + len(keys)

            if silent:
                click.echo("copied batch %s (total keys: %s)" % (i+1, key_count))
    finally:
        await src.close()
        await src.connection_pool.disconnect()
        await dst.close()
        await dst.connection_pool.disconnect()
    click.echo("Done")


async def scan_iter_batch(
    r: redis.asyncio.Redis,
    **kwargs
) -> AsyncIterator:
    i = 0
    cursor = "0"
    prefetch = asyncio.create_task(r.scan(cursor=cursor, **kwargs))
    while cursor != 0:
        cursor, keys = await prefetch
        if cursor != 0:
            prefetch = asyncio.create_task(r.scan(cursor=cursor, **kwargs))

        yield i, keys
        i = i + 1


async def copy_one(
    key: bytes,
    src: redis.asyncio.Redis,
    dst: redis.asyncio.Redis,
    replace: bool,
    silent: bool,
) -> None:
    src_dump = await src.dump(key)
    await dst.restore(key, 0, src_dump, replace=replace)

    # restore TTL info
    src_ttl = await src.pttl(key)
    if src_ttl > 0:
        await dst.pexpire(key, src_ttl)

    if not silent:
        key_fmt = key.decode("unicode_escape")
        ttl_fmt = f"{src_ttl} ms" if src_ttl > 0 else "n/a"
        click.echo(f"Copied key '{key_fmt}' (TTL: {ttl_fmt})")
