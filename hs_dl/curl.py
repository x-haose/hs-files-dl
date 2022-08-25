import json
import shlex
from argparse import ArgumentParser

import rich


class CurlParser(object):
    """
    CURL解析器（适用场景有限）
    """

    def __init__(self, curl_string: str):
        """
        初始化方法
        Args:
            curl_string: curl字符串
        """
        # 定义属性
        self.url: str = ""
        self.method: str = ""
        self.headers: dict = {}
        self.data: dict = {}

        # 设置curl字符串并作处理
        self._curl_string = curl_string.replace('\\\n', ' ').strip()

        # 设置参数解析器
        self._parser = ArgumentParser()

        # 初始化参数
        self._init_parser()

        # 判断传进来的是url地址还是curl字符串
        if self._curl_string.startswith("http"):
            self.url = self._curl_string
            self.method = 'GET'
        else:
            self._parse_curl()

    def to_dict(self):
        """
        转成dict类型
        Returns:

        """
        return {
            k: v for k, v in self.__dict__.items() if not k.startswith('_')
        }

    def _init_parser(self):
        """
        初始化解析器
        Returns:

        """
        self._parser.add_argument("curl")
        self._parser.add_argument("url", default='')
        self._parser.add_argument("-X", "--request", default="")
        self._parser.add_argument("-d", "--data", "--data-raw")
        self._parser.add_argument("-F", "--form", "--form-string")
        self._parser.add_argument("-H", "--header", action="append", default=[])
        self._parser.add_argument("--compressed", action="store_true")

    def _parse_curl(self):
        """
        解析CURL字符串
        Returns:

        """
        lex_list = shlex.split(self._curl_string)
        args, _ = self._parser.parse_known_args(lex_list)

        # 设置请求url
        self.url = args.url

        # 设置请求头
        for header in args.header:
            key, value = header.split(":", 1)
            self.headers[key.title()] = value.strip()

        # 设置请求数据
        data = args.data or args.form or '{}'
        self.data = json.loads(data)

        # 设置请求方法
        # 如果没有传递请求方法：存在请求数据则设置为POST请求，否则为GET
        # 如果传递了请求方法：按照传递了的值来设置
        self.method = args.request if args.request else "POST" if data else "GET"


def main():
    req = CurlParser("""
    curl 'https://api.mdnice.com/articles' \
  -H 'Accept: application/json, text/plain, */*' \
  -H 'Accept-Language: zh-CN,zh;q=0.9' \
  -H 'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoidXNlciIsInVzZXJJZCI6Ik1qazVPVEE9Iiwic3ViIjoieGhydHhoQGdtYWlsLmNvbSIsImlzcyI6IjkwYjlhNjNjODFjYzYzNTg4NDg2IiwiaWF0IjoxNjU5MjQyNjc4LCJhdWQiOiJtZG5pY2UtYXBpIiwiZXhwIjoxNjYxODM0Njc4LCJuYmYiOjE2NTkyNDI2Nzh9.vlFaIpnDNKOvOfwoQs6FTYF4aW6GrAmAVXfXWxVW0zA' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/json;charset=UTF-8' \
  -H 'Origin: https://editor.mdnice.com' \
  -H 'Pragma: no-cache' \
  -H 'Referer: https://editor.mdnice.com/' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Site: same-site' \
  -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36' \
  -H 'dnt: 1' \
  -H 'sec-ch-ua: "Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "Windows"' \
  -H 'sec-gpc: 1' \
  --data-raw '{"title":"未命名文章","markdown":"","html":"","isPublic":0}' \
  --compressed
    """)
    rich.print(req.to_dict())


if __name__ == '__main__':
    main()
