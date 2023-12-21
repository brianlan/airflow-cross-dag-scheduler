import aiohttp

async def get(url, cookies=None):
    async with aiohttp.ClientSession(cookies=cookies) as session:
        async with session.get(url) as response:
            status = response.status
            json_data = await response.json()
            return status, json_data


async def post(url, data, cookies=None):
    async with aiohttp.ClientSession(cookies=cookies) as session:
        async with session.post(url, data=data) as response:
            status = response.status
            json_data = await response.json()
            return status, json_data
