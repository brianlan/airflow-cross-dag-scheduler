import aiofiles

async def async_read_cookie_session(path):
    async with aiofiles.open(path, 'r') as f:
        cookie_session = await f.read()
    return cookie_session.strip()


# async def read_cookie_session():
#     async with open('conf/cookie_session', 'r') as f:
#         cookie_session = f.read()
#     return cookie_session.strip()
