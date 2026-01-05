from urllib.parse import urlencode

BASE_URL = "https://oauth.vk.com/authorize"
PARAMS = {
    "client_id": "6121396",
    "redirect_uri": "https://oauth.vk.com/blank.html",
    "response_type": "token",
    "scope": "offline",  # for long-lived token, details: https://stackoverflow.com/a/27107324
    "v": "5.131",
}


def prompt_token() -> None:
    print("Access token is required. Open:")
    print(f"{BASE_URL}?{urlencode(PARAMS)}")
    print("After authorization, copy access_token from the address bar (vk1.a...)")


if __name__ == "__main__":
    prompt_token()
