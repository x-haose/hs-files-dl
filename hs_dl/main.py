import asyncio
import math
import sys
import itertools
import time
from operator import itemgetter

import aiofiles
from loguru import logger
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    Console,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from hs_dl.request import Request, RequestException

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
    console=Console(record=True)
)


def format_size(filesize: float):
    """
    格式化输出文件大小
    :param filesize: 文件大小
    :return: 返回格式化的字符串
    """
    for count in ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']:
        if -1024.0 < filesize < 1024.0:
            return f"{filesize:3.1f} {count}"
        filesize /= 1024.0
    return f"{filesize:3.1f} YB"


class HSDownloader(object):

    def __init__(self, url: str, save_path: str, concurrency: int = 64):
        """
        :param url: 要下载的地址
        :param save_path: 保存的路径
        :param concurrency: 并发的数量
        """
        self.url = url
        self.save_path = save_path
        self.headers = {}
        self._sem = asyncio.Semaphore(concurrency)
        self._head_headers = None

        self.block_number = 32
        self.target_size = self.block_number * 10 * 1024 * 1024
        self.task_id = None

        logger.remove()
        logger.add(f"{save_path}.log", level="DEBUG")
        logger.add(sys.stderr, level="INFO")

    async def start(self):
        """
        开始下载
        :return:
        """
        await self.get_head_headers()

        s_time = time.perf_counter()
        logger.info(f"开始下载资源，总大小：{self.file_size}")

        # 开始下载任务
        result = await self.start_download()

        e_time = time.perf_counter()

        # 计算平均下载速度
        average_speed = self.content_length / (e_time - s_time)
        average_speed = format_size(average_speed)

        logger.success(f"下载成功, 总用时：{e_time - s_time:.3f}s 平均速度：{average_speed}/s")

        # 保存文件
        await self.save_file(self.save_path, b''.join([d['content'] for d in result]))

    async def save_file(self, filepath, content):
        """
        保存文件
        :param filepath: 文件路径
        :param content: 内容
        :return:
        """
        async with aiofiles.open(filepath, mode='wb') as f:
            await f.write(content)

    async def start_download(self):
        """
        开始下载
        :return:
        """
        # 第一种下载方案：资源总大小小于等于 64 * 10 = 640 MB: 任务数量固定为64个，大小平均分
        # 第二种下载方案：资源总大小大于 64 * 10 = 640 MB: 每个任务下载的大小固定为10M，任务数量动态计算
        if self.content_length <= self.target_size:
            block_size = int(self.content_length / self.block_number)
            block_number = self.block_number
        else:
            block_size = 10 * 1024 * 1024
            block_number = math.ceil(self.content_length / block_size)

        if not self.accept_ranges:
            block_size = 0
            block_number = 1

        # 计算每个任务的参数，存到字典中
        args_dict = dict()
        for index in range(block_number):
            s = index * block_size if index == 0 else args_dict.get(index - 1).get('e') + 1
            e = s + block_size if index < block_number - 1 else self.content_length
            args_dict[index] = {"s": s, "e": e if self.accept_ranges else None}

        # 转换成元组列表
        args = [
            (k, v['s'], v['e'])
            for k, v in args_dict.items()
        ]

        # 开启多任务
        tasks = itertools.starmap(self.get_content, args)
        progress.start()
        self.task_id = progress.add_task('', filename="总进度", total=self.content_length)
        result = await asyncio.gather(*tasks)
        progress.stop()

        return sorted(result, key=itemgetter('index'))

    async def get_head_headers(self):
        """
        获取HEAD请求的响应头
        :return:
        """
        req = Request('HEAD', self.url, sem=self._sem)
        try:
            resp = await req.request()
        except RequestException:
            return self.network_error_exit()
        else:
            self._head_headers = resp.headers
        finally:
            await req.close()

        return self._head_headers

    @property
    def accept_ranges(self):
        """
        是否接受断点续传
        :return:
        """
        return self._head_headers.get('Accept-Ranges') is not True

    @property
    def content_length(self):
        """
        获取资源总大小
        :return:
        """
        return int(self._head_headers.get('Content-Length'))

    @property
    def file_size(self):
        """
        格式化输出文件大小
        :return:
        """
        return format_size(self.content_length)

    async def get_content(self, index, start=None, end=None):
        """
        获取内容
        :param index: 索引
        :param start: 开始范围
        :param end: 结束范围
        :return: 返回内容的数据
        """
        logger.debug(f"开始下载：bytes={start}-{end}")

        if end:
            headers = dict(self.headers, Range=f"bytes={start}-{end}")
        else:
            headers = self.headers.copy()

        end = end or self.content_length
        task_id = progress.add_task("", filename=f"任务: {index}", total=end - start)

        req = Request("GET", self.url, sem=self._sem, headers=headers)
        try:
            content = b''
            resp = await req.request(stream=True)
            async for data in resp.aiter_bytes():
                content += data
                progress.update(task_id, advance=len(data))
                progress.update(self.task_id, advance=len(data))

        except RequestException:
            return self.network_error_exit()
        finally:
            await req.close()

        return {'index': index, 'content': content}

    def network_error_exit(self):
        """
        发生网络错误异常
        :return:
        """
        logger.critical(f"网络请求发生错误，下载失败！")
        sys.exit(-1)


async def main():
    url = "https://download.virtualbox.org/virtualbox/6.1.36/VirtualBox-6.1.36-152435-Win.exe"
    download = HSDownloader(url, "VirtualBox-6.1.36-152435-Win.exe")
    await download.start()


if __name__ == '__main__':
    asyncio.run(main())
