import aiohttp

async def get(url, cookies=None):
    async with aiohttp.ClientSession(cookies=cookies) as session:
        async with session.get(url) as response:
            resp = await response.json()
            return resp
