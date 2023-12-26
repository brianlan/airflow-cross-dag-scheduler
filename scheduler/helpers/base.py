import json

import aiofiles

async def async_read_cookie_session(path):
    async with aiofiles.open(path, 'r') as f:
        cookie_session = await f.read()
    return cookie_session.strip()


def read_cookie_session(path):
    with open(path, "r") as f:
        return f.read().strip()


def is_in_df(query_key_values, df):
    query_str = " and ".join([f"{k} == '{v}'" for k, v in query_key_values.items()])
    return len(df.query(query_str)) > 0


def extract_values(input: str) -> list:
    d = json.loads(input)
    assert isinstance(d, list), "input string should be actually a list"
    assert len(d) > 0, "input should not be empty"
    return [list(item.values())[0] if isinstance(item, dict) else item for item in d]

# async def read_cookie_session():
#     async with open('conf/cookie_session', 'r') as f:
#         cookie_session = f.read()
#     return cookie_session.strip()
