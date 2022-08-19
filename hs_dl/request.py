import functools
import asyncio
from dataclasses import dataclass, field
from typing import Union, Dict

from httpx import AsyncClient, Timeout, Response
from loguru import logger
from tenacity import (
    AsyncRetrying, Future, RetryCallState,
    stop_after_attempt, wait_fixed, wait_random
)


class RequestException(Exception):
    """
    请求发生异常
    """


class RequestStateException(Exception):
    """
    请求状态发生异常
    """

    def __init__(self, code):
        self.code = code


@dataclass
class Request(object):
    # 请求的方法
    method: str
    # 请求的地址
    url: str
    # 信号量，控制并发
    sem: asyncio.Semaphore = field(default=None)
    # 请求的JSON数据
    json: dict = field(default=None)
    # 请求UA
    user_agent: str = field(default=None)
    # 请求头
    headers: dict = field(default=None)
    # 传递的cookies
    cookies: dict = field(default=None)
    # 请求超时时间
    timeout: float = field(default=None)
    # 设置允许的状态码
    allow_codes: list = field(default_factory=list)
    # 代理设置
    proxies: Union[Dict, str] = field(default=None, init=False)
    # 重试次数
    retrys_count: int = field(default=15)
    # 重试延时
    retries_delay: int = field(default_factory=int)

    def __post_init__(self):
        # 设置httpx会话
        self.session = AsyncClient(proxies=self.proxies, verify=False)

        # 设置session的headers、cookies、timeout
        self.session.headers = self.headers
        self.session.cookies = self.cookies
        self.session.timeout = Timeout(self.timeout)

        # user_agent设置到headers中
        if self.user_agent:
            self.session.headers.update({"user-agent": self.user_agent})

        self.headers = dict(self.session.headers)

        # 设置允许的状态码 默认是200-300之间
        allow_codes = list(range(200, 300))
        if not self.allow_codes:
            self.allow_codes = allow_codes
        else:
            self.allow_codes.extend(allow_codes)

    async def close(self):
        """
        关闭session
        :return:
        """
        await self.session.aclose()

    def retry_handler(self, retry_state: RetryCallState):
        """
        处理重试之后和重试失败
        :param retry_state:
        :return:
        """
        outcome: Future = retry_state.outcome
        attempt_number = retry_state.attempt_number
        exception = outcome.exception()
        exception_type = type(exception).__name__
        exception_msg = list(exception.args)

        if isinstance(exception, RequestStateException):
            exception_msg = f"响应状态:{exception.code} 不在规定的列表内：{self.allow_codes}"
        log_msg = f"[{self.method}] {self.url} 发生异常: [{exception_type}]: {exception_msg}"

        if attempt_number == self.retrys_count:
            logger.error(f"{log_msg} {attempt_number}次重试全部失败")
            raise RequestException()
        else:
            logger.error(f"{log_msg} 第{attempt_number}次重试")

    async def request(self, stream=False) -> Response:
        """
        使用httpx发起请求 带重试机制
        :return: 返回响应
        """
        if self.retrys_count < 1:
            return await self._request()

        # 设置重试的等候时间
        if self.retries_delay:
            wait = wait_fixed(self.retries_delay) + wait_random(0, 2)
        else:
            wait = wait_fixed(0)

        # 异步重试
        r = AsyncRetrying(
            stop=stop_after_attempt(self.retrys_count),
            after=self.retry_handler,
            retry_error_callback=self.retry_handler,
            wait=wait
        )

        resp = await r.wraps(functools.partial(self._request, stream))()
        return resp

    async def _request(self, stream=False) -> Response:
        """
        使用httpx发起请求
        :return: 返回响应
        """
        # 打印一个DEBUG级别的日志
        logger.debug(f"[{self.method}] {self.url}")

        # 锁定信号量
        if self.sem:
            await self.sem.acquire()

        # 进行请求，并获取响应
        request = self.session.build_request(
            self.method,
            self.url,
            json=self.json,
        )
        response = await self.session.send(request, stream=stream)

        # 释放信号量
        if self.sem:
            self.sem.release()

        # 如果状态码不在允许范围当中，则抛出异常
        if response.status_code not in self.allow_codes:
            raise RequestStateException(response.status_code)

        return response
