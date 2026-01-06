from urllib.parse import urlparse

import vk_api  # type: ignore[import-untyped]
from vk_api.exceptions import ApiError  # type: ignore[import-untyped]

VK_API_VERSION = "5.131"
PAGE_SIZE = 100

# for some reason vk_api doesn't have this
INVALID_TOKEN_CODE = 5

def _is_invalid_token(e: BaseException) -> bool:
    return isinstance(e, ApiError) and e.code == INVALID_TOKEN_CODE



class VKClient():
    def __init__(self, token: str, page_size: int|None = None):
        self.api = vk_api.VkApi(token=token, api_version=VK_API_VERSION).get_api()
        self.page_size = page_size or PAGE_SIZE
        self._check_auth()
        
    def _check_auth(self):
        try:
            self.call(self.api.users.get)
        except ApiError as e:
            if _is_invalid_token(e):
                raise ValueError("Invalid token: authorization failed") from e
            raise
        
    def call(self, fn, **params):
        return fn(**params)
        
    def paginate(self, fn, **params):
        offset = 0
        while True:
            r = self.call(fn, **params, count=PAGE_SIZE, offset=offset)
            items = r.get("items") or []
            if not items:
                return
            yield from items
            
            offset += len(items)
            if offset >= int(r.get("count") or 0):
                return
            
    @staticmethod
    def slug(group_url: str) -> str:
        s = (group_url or "").strip()
        if not s:
            return ""
        if "vk.com/" in s or "vkontakte.ru/" in s:
            s = urlparse(s if "://" in s else "https://" + s).path

        return s.strip("/").split("/", 1)[0]
    
