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

# async def read_cookie_session():
#     async with open('conf/cookie_session', 'r') as f:
#         cookie_session = f.read()
#     return cookie_session.strip()
