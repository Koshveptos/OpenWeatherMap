import aiohttp
import requests
from loguru import logger

API_BASE = "https://api.openweathermap.org/data/2.5/weather"


def get_current_weather_sync(city: str, api_key: str) -> dict:
    # cинхронный запрос к API.
    params = {"q": city, "appid": api_key, "units": "metric"}
    try:
        response = requests.get(API_BASE, params=params)
        response.raise_for_status()
        data = response.json()
        return {
            "temperature": data["main"]["temp"],
            "description": data["weather"][0]["description"],
            "timestamp": data["dt"],
        }
    except requests.exceptions.HTTPError as e:
        if response.status_code == 401:
            raise ValueError("Invalid API key")
        raise e
    except Exception as e:
        logger.error(f"API error: {e}")
        raise


async def get_current_weather_async(city: str, api_key: str) -> dict:
    # aсинхронный запрос к API.
    params = {"q": city, "appid": api_key, "units": "metric"}
    async with aiohttp.ClientSession() as session:
        async with session.get(API_BASE, params=params) as response:
            if response.status == 401:
                raise ValueError("Invalid API key")
            response.raise_for_status()
            data = await response.json()
            return {
                "temperature": data["main"]["temp"],
                "description": data["weather"][0]["description"],
                "timestamp": data["dt"],
            }
