import asyncio
import cgi
import math
import os
import itertools
import time
from datetime import datetime
from operator import itemgetter
from pathlib import Path
from urllib.parse import unquote

import fire
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
from rich.table import Table

from hs_dl.request import Request, RequestException
from hs_dl.curl import CurlParser


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

    def __init__(
            self,
            url: str,
            *,
            save_path: str = None,
            save_name: str = None,
            concurrency: int = 64,
            headers: dict = None,
            method: str = None,
            data: dict = None
    ):
        """
        :param url: 要下载的地址
        :param save_path: 保存的文件路径
        :param save_path: 保存的文件名字
        :param concurrency: 并发的数量
        :param headers: 请求头
        :param method: 请求方法
        :param data: 请求的数据
        """
        # 解析curl字符串
        parser = CurlParser(url)
        self.url = parser.url
        self.headers = parser.headers or headers or {}
        self.method = parser.method or method or "GET"
        self.data = parser.data or data or {}

        # 默认下载路径为当前目录下单downloads文件夹
        self.save_path = Path(save_path or 'downloads').absolute()
        # 文件名默认从url中获取
        self.save_name = save_name or unquote(Path(self.url).name)
        # 路径不存在则创建
        if not self.save_path.exists():
            self.save_path.mkdir(parents=True)

        self._sem = asyncio.Semaphore(concurrency)
        self._head_headers = None

        self.block_number = 32
        self.target_size = self.block_number * 10 * 1024 * 1024
        self.task_id = None

        # 设置日志文件夹，不存在则创建
        self.log_path = Path("logs") / f"{datetime.now().date().isoformat()}.log"
        if not self.log_path.parent.exists():
            self.log_path.parent.mkdir(parents=True)

        # 初始化日志
        logger.remove()
        logger.add(self.log_path, level="DEBUG")

        # 初始化rich控制台
        self.console = Console(record=True)
        # 初始化rich进度条
        self.progress = Progress(
            TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            DownloadColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            console=self.console
        )

    def print_download_info(self):
        """
        打印下载信息
        :return:
        """
        self.console.rule("开始下载")
        headers = ["资源名字", "资源路径", "日志文件路径", "是否允许断点续传", "资源总大小"]
        datas = [
            self.save_name,
            str(self.save_path / self.save_name),
            str(self.log_path.absolute()),
            '是' if self.accept_ranges else '否',
            self.file_size
        ]
        table = Table(*headers, show_header=True, header_style="bold magenta")
        table.add_row(*datas)
        self.console.print(table)

    async def start(self):
        """
        开始下载
        :return:
        """
        await self.get_head_headers()

        s_time = time.perf_counter()
        logger.info(
            f"开始下载资源：\n"
            f"资源名字：{self.save_name}\n"
            f"资源路径：{self.save_path / self.save_name}\n"
            f"是否允许断点续传：{self.accept_ranges}\n"
            f"总大小：{self.file_size}"
        )
        self.print_download_info()

        # 开始下载任务
        result = await self.start_download()

        e_time = time.perf_counter()

        # 计算平均下载速度
        average_speed = self.content_length / (e_time - s_time)
        average_speed = format_size(average_speed)

        self.console.print(f"下载成功, 总用时：{e_time - s_time:.3f}s 平均速度：{average_speed}/s")

        # 保存文件
        await self.save_file(self.save_path / self.save_name, b''.join([d['content'] for d in result]))

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

        if not self.accept_ranges or self.content_length == 0:
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
        self.progress.start()
        self.task_id = self.progress.add_task('', filename="总进度", total=self.content_length)
        result = await asyncio.gather(*tasks)
        self.progress.stop()

        return sorted(result, key=itemgetter('index'))

    async def get_head_headers(self):
        """
        获取HEAD请求的响应头
        :return:
        """
        req = Request('HEAD', self.url, sem=self._sem, headers=self.headers)
        try:
            resp = await req.request()
        except RequestException:
            self._head_headers = {}
        else:
            if 'Content-Disposition' in resp.headers:
                self.save_name = cgi.parse_header(resp.headers.get('Content-Disposition'))[1]['filename']
            self._head_headers = resp.headers
            await resp.aclose()
        finally:
            await req.close()

        return self._head_headers

    @property
    def accept_ranges(self):
        """
        是否接受断点续传
        :return:
        """
        return self._head_headers.get('Accept-Ranges') is not None

    @property
    def content_length(self):
        """
        获取资源总大小
        :return:
        """
        return int(self._head_headers.get('Content-Length', 0))

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
        task_id = self.progress.add_task("", filename=f"任务: {index}", total=end - start)

        req = Request(self.method, self.url, sem=self._sem, headers=headers, data=self.data)
        try:
            content = b''
            resp = await req.request(stream=True)
            async for data in resp.aiter_bytes():
                content += data
                self.progress.update(task_id, advance=len(data))
                self.progress.update(self.task_id, advance=len(data))

        except RequestException:
            return self.network_error_exit()
        else:
            await resp.aclose()
        finally:
            await req.close()

        return {'index': index, 'content': content}

    def network_error_exit(self):
        """
        发生网络错误异常
        :return:
        """
        logger.critical(f"网络请求发生错误，下载失败！")
        self.console.log("网络请求发生错误，下载失败！", highlight=True)
        getattr(os, '_exit')(1)


async def main(
        url: str,
        *,
        save_path: str = None,
        save_name: str = None,
        headers: dict = None,
        method: str = None,
        data: dict = None
):
    """
    高速下载普通文件
    :param url: 下载地址
    :param save_path: 保存的文件夹，默认为./downloads文件夹
    :param save_name: 保存至的文件名， 默认从url中获取
    :param headers: 请求头
    :param method: 请求方法
    :param data: 请求的数据
    :return:
    """

    if url == "curl":
        lines = []
        print("请输入从浏览器复制的curl字符串，并输入:wq结束：", end='')
        for line in iter(input, ':wq'):
            lines.append(line)
        url = ''.join(lines).replace('\\', ' ')

    download = HSDownloader(
        url,
        save_path=save_path,
        save_name=save_name,
        headers=headers,
        method=method,
        data=data,
    )
    await download.start()


if __name__ == '__main__':
    fire.Fire(main)
