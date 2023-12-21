import aiohttp
import asyncio

class Non200Response(Exception):
    pass

def async_retry(retries=3, delay=1):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exception = None
            for _ in range(retries):
                try:
                    # response is a tuple of (status_code, json_data) in `get` and `post` defined below
                    response = await func(*args, **kwargs)
                    if response[0] != 200:  # Assuming the first element of the return tuple is status code
                        raise Non200Response(f"Status code {response[0]} received.")
                    return response
                except (aiohttp.ClientError, Non200Response) as e:
                    last_exception = e
                    await asyncio.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


@async_retry(retries=3, delay=1)
async def get(url, cookies=None):
    async with aiohttp.ClientSession(cookies=cookies) as session:
        async with session.get(url) as response:
            status = response.status
            json_data = await response.json()
            return status, json_data


@async_retry(retries=3, delay=1)
async def post(url, data, cookies=None):
    async with aiohttp.ClientSession(cookies=cookies) as session:
        async with session.post(url, data=data) as response:
            status = response.status
            json_data = await response.json()
            return status, json_data
