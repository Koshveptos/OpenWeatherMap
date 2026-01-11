import aiohttp
import requests
from loguru import logger

API_BASE = "https://api.openweathermap.org/data/2.5/weather"


"""
В своем коде я использовал не только паралельность и тд, я использовал
"Метод анализа",
                [
                    "Синхронный",
                    "Параллельный (Joblib)",
                    "Многопоточный",
                    "Многопроцессный",
                    "Асинхронный",
                ],
теперь подробнее о выводах которые получил (значение увидите на странице бенчмарка в приложении)

Синхронный подход использовал как базовый, для небольших объемов данных дает норм результат, +прост в реализации, без излишеств

Joblib (параллелизация на уровне процессов) -
Использование joblib привело к значительному увеличению времени выполнения по сравнению с синхронным вариантом.
Причины:
Высокие накладные расходы на создание и управление процессами.
Сериализация данных между процессами.
Небольшой объём вычислений, при котором параллелизация не успевает себя оправдать.

Многопоточный вариант показал незначительное улучшение по сравнению с синхронным выполнением(он, скорее , среднее между синхронный и асинхронным).
Особенности:
В Python действует GIL (Global Interpreter Lock), который ограничивает эффективность многопоточности для CPU-bound задач.
Тем не менее, часть операций (например, работа с Pandas) может частично освобождать GIL.


Многопроцессный подход оказался самым медленным.
Причины:
Высокие накладные расходы на запуск процессов.
Копирование данных между процессами.
Небольшая вычислительная нагрузка на каждый процесс.

Асинхронная реализация показала лучший результат по времени выполнения.
Причины:
Минимальные накладные расходы на управление задачами.
Эффективное использование событийного цикла.
Хорошо подходит для операций, связанных с ожиданием (например, I/O), и для лёгких вычислений в рамках одного потока.





вообще, путем проб и эксперементов, за первое место среди медленных борятся 2 метода, многопроцессорный и параллелизация на уровне процессов)
на некоторых данных они показывают себя хорошо, а на некоторых очеень плохо
"""


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
