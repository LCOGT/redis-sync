import asyncio
import hashlib
import functools

import click
import redis.asyncio

from collections.abc import AsyncGenerator, AsyncIterator

from aiostream import stream
from tqdm.asyncio import tqdm


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
@click.option("--progress-interval",
    type=click.FloatRange(0.5, 60),
    default=1,
    show_default=True,
    help="minimum time interval between progress updates"
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
@click.option("--verbose/--no-verbose",
    default=False,
    show_default=True,
    help="spam stdout"
)
@click.option("--verbose-hash-key/--no-verbose-hash-key",
    default=False,
    show_default=True,
    help="print the hash of the key instead of the key itself"
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
    verbose: bool,
    prompts: bool,
    progress_interval: float,
    verbose_hash_key: bool,
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

        keys = ScanIter(
            src,
            match=match_keys,
            count=parallel,
        )

        copiers = [copier(keys, src, dst, dst_replace) for _ in range(parallel)]

        pbar = tqdm(desc="Copied", mininterval=progress_interval, unit=" keys")

        async with stream.merge(*copiers).stream() as s:
            async for key, ttl in s:
                pbar.update()
                if verbose:
                    if verbose_hash_key:
                        key_fmt = "sha1: %s" % hashlib.sha1(key).hexdigest()
                    else:
                        key_fmt = key.decode("unicode_escape")
                    ttl_fmt = f"{ttl} ms" if ttl > 0 else "n/a"
                    pbar.write(f"Copied key '{key_fmt}' (TTL: {ttl_fmt})")

        pbar.close()
    finally:
        await src.close()
        await src.connection_pool.disconnect()
        await dst.close()
        await dst.connection_pool.disconnect()
    click.echo("Done")


async def copier(
    keys: "ScanIter",
    src: redis.asyncio.Redis,
    dst: redis.asyncio.Redis,
    replace: bool,
) -> AsyncGenerator[tuple[bytes, int], None]:
    async for key in keys:
        ttl = await copy_one(key, src, dst, replace)
        yield key, ttl


async def copy_one(
    key: bytes,
    src: redis.asyncio.Redis,
    dst: redis.asyncio.Redis,
    replace: bool,
) -> int:
    src_dump = await src.dump(key)
    await dst.restore(key, 0, src_dump, replace=replace)

    # restore TTL info
    src_ttl = await src.pttl(key)
    if src_ttl > 0:
        await dst.pexpire(key, src_ttl)

    return src_ttl


class ScanIter(AsyncIterator):

    def __init__(
        self,
        r: redis.asyncio.Redis,
        **kwargs
    ):
        self._scan_iter = functools.partial(r.scan_iter, **kwargs)
        self._q = asyncio.Queue(maxsize=kwargs.get("count", 100) * 2)
        self._feed_task = None

    def __aiter__(self):
        if self._feed_task is None:
            self._feed_task = asyncio.create_task(self._feed())

        return self

    async def _feed(self):
        async for k in self._scan_iter():
            await self._q.put(k)

        await self._q.join()

    async def __anext__(self) -> bytes:
        if not self._feed_task or self._feed_task.done():
            raise StopAsyncIteration()

        q_get = asyncio.create_task(self._q.get())
        done, _ = await asyncio.wait(
            (q_get, self._feed_task),
            return_when=asyncio.FIRST_COMPLETED
        )

        if q_get in done:
            item = await q_get
            self._q.task_done()
            return item

        q_get.cancel()

        raise StopAsyncIteration()
