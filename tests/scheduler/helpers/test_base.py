from scheduler.helpers.base import async_read_cookie_session


async def test_async_read_cookie_session():
    result = await async_read_cookie_session("conf/cookie_session")
    assert result == "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"


# def test_read_cookie_session():
#     assert read_cookie_session() == "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"
