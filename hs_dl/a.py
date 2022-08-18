import asyncio
import time

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

progress = Progress(
    TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    DownloadColumn(),
    "•",
    TransferSpeedColumn(),
    "•",
    TimeRemainingColumn(),
)


async def func(i):
    task = progress.add_task("测试", total=1024*1024*1024, filename=f"任务：{i}")
    for i in range(100):
        await asyncio.sleep(0.1)
        progress.update(task, advance=1024*1024*20)


async def main():
    progress.start()
    await asyncio.gather(
        func(1),
        func(2),
        func(3),
        func(4),
    )
    progress.stop()


if __name__ == '__main__':
    asyncio.run(main())
