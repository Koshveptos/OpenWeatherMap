import warnings
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go

warnings.filterwarnings("ignore")
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial

from joblib import Parallel, delayed
from loguru import logger
from scipy import stats

from api_utils import get_current_weather_async, get_current_weather_sync


class HistoricalDataAnalyzer:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.df["timestamp"] = pd.to_datetime(self.df["timestamp"])
        self.results = {}
        self.benchmark_times = {}
        self.month_to_season = {  ## можно было и умнее сделать, но больше для удобства решил сделать)
            12: "winter",
            1: "winter",
            2: "winter",
            3: "spring",
            4: "spring",
            5: "spring",
            6: "summer",
            7: "summer",
            8: "summer",
            9: "autumn",
            10: "autumn",
            11: "autumn",
        }

    # базовые показатели
    def calculate_basic_statistics(self, city_data: pd.DataFrame) -> dict:
        return {
            "mean": city_data["temperature"].mean(),
            "std": city_data["temperature"].std(),
            "min": city_data["temperature"].min(),
            "max": city_data["temperature"].max(),
            "median": city_data["temperature"].median(),
            "q1": city_data["temperature"].quantile(0.25),
            "q3": city_data["temperature"].quantile(0.75),
            "count": len(city_data),
        }

    # скользящее
    def calculate_rolling_mean(self, city_data: pd.DataFrame, window_size: int = 30) -> pd.DataFrame:
        city_data = city_data.sort_values("timestamp")
        city_data[f"ma_{window_size}"] = (
            city_data["temperature"].rolling(window=window_size, center=True, min_periods=1).mean()
        )
        return city_data

    # аномалии
    def detect_anomalies(self, city_data: pd.DataFrame, window_size: int = 30, threshold: float = 2.0) -> dict:
        city_data = self.calculate_rolling_mean(city_data, window_size)
        city_data[f"std_{window_size}"] = (
            city_data["temperature"].rolling(window=window_size, center=True, min_periods=1).std()
        )
        ma_col = f"ma_{window_size}"
        std_col = f"std_{window_size}"
        anomalies_mask = (city_data["temperature"] > city_data[ma_col] + threshold * city_data[std_col]) | (
            city_data["temperature"] < city_data[ma_col] - threshold * city_data[std_col]
        )
        anomalies = city_data[anomalies_mask].copy()
        anomalies["deviation"] = anomalies["temperature"] - anomalies[ma_col]
        return {
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "anomaly_percent": (len(anomalies) / len(city_data)) * 100 if len(city_data) > 0 else 0,
        }

    # профиль сезона
    def calculate_seasonal_profile(self, city_data: pd.DataFrame) -> pd.DataFrame:
        seasonal_stats = city_data.groupby("season")["temperature"].agg(["mean", "std", "count"])
        seasonal_stats["lower"] = seasonal_stats["mean"] - seasonal_stats["std"]
        seasonal_stats["upper"] = seasonal_stats["mean"] + seasonal_stats["std"]
        return seasonal_stats

    # тренды
    def calculate_trend(self, city_data: pd.DataFrame) -> dict:
        city_data = city_data.copy()
        city_data["timestamp"] = pd.to_datetime(city_data["timestamp"])  # Фикс: Убедимся в типе
        city_data["days"] = (city_data["timestamp"] - city_data["timestamp"].min()).dt.days
        slope, intercept, r_value, p_value, std_err = stats.linregress(city_data["days"], city_data["temperature"])
        return {
            "slope": slope,
            "intercept": intercept,
            "r_value": r_value,
            "p_value": p_value,
            "trend_description": f"Тренд: {'рост' if slope > 0 else 'падение'} на {abs(slope):.4f}°C в день (R²={r_value**2:.2f})",
        }

    # анализ города
    def analyze_city_sync(self, city: str, window_size: int, threshold: float) -> dict:
        city_data = self.df[self.df["city"] == city]
        stats = self.calculate_basic_statistics(city_data)
        anomalies = self.detect_anomalies(city_data, window_size, threshold)
        seasonal = self.calculate_seasonal_profile(city_data)
        trend = self.calculate_trend(city_data)
        return {
            "city": city,
            "stats": stats,
            "anomalies": anomalies,
            "seasonal": seasonal,
            "trend": trend,
        }

    # паралель
    def analyze_city_parallel(self, cities: list, window_size: int, threshold: float, method: str = "joblib") -> dict:
        if method == "joblib":
            results = Parallel(n_jobs=-1)(
                delayed(self.analyze_city_sync)(city, window_size, threshold) for city in cities
            )
        elif method == "multithread":
            with ThreadPoolExecutor() as executor:
                results = list(
                    executor.map(
                        partial(
                            self.analyze_city_sync,
                            window_size=window_size,
                            threshold=threshold,
                        ),
                        cities,
                    )
                )
        elif method == "multiprocess":
            with ProcessPoolExecutor() as executor:
                results = list(
                    executor.map(
                        partial(
                            self.analyze_city_sync,
                            window_size=window_size,
                            threshold=threshold,
                        ),
                        cities,
                    )
                )
        else:
            raise ValueError("Invalid method")
        return {res["city"]: res for res in results}

    # асинхронщина
    async def analyze_city_async(self, city: str, window_size: int, threshold: float) -> dict:
        return self.analyze_city_sync(city, window_size, threshold)

    # замеры
    def benchmark_methods(self, city: str, window_size: int, threshold: float) -> dict:
        import time

        times = {}
        start = time.time()
        self.analyze_city_sync(city, window_size, threshold)
        times["sync"] = time.time() - start
        start = time.time()
        self.analyze_city_parallel([city], window_size, threshold, "joblib")
        times["joblib"] = time.time() - start
        start = time.time()
        self.analyze_city_parallel([city], window_size, threshold, "multithread")
        times["multithread"] = time.time() - start
        start = time.time()
        self.analyze_city_parallel([city], window_size, threshold, "multiprocess")
        times["multiprocess"] = time.time() - start
        start = time.time()
        asyncio.run(self.analyze_city_async(city, window_size, threshold))
        times["async"] = time.time() - start
        self.benchmark_times = times
        return times

    # для работы с текущей погодой
    def analyze_current_weather(self, city: str, api_key: str, method: str = "sync") -> dict:
        logger.info(f"Analyzing current weather for {city} using {method}")
        if method == "sync":
            current = get_current_weather_sync(city, api_key)
        elif method == "async":
            current = asyncio.run(get_current_weather_async(city, api_key))
        else:
            raise ValueError("Invalid API method")

        logger.debug(f"Current data: {current}")
        city_data = self.df[self.df["city"] == city]
        current_date = datetime.fromtimestamp(current["timestamp"])
        season = self.month_to_season.get(current_date.month, "winter")
        logger.info(f"Determined season: {season} for month {current_date.month}")

        seasonal_stats = self.calculate_seasonal_profile(city_data)
        if season not in seasonal_stats.index:
            logger.error(f"Season '{season}' not in seasonal_stats: {seasonal_stats.index}")
            raise ValueError(f"Сезон '{season}' не найден в исторических данных для {city}")

        seasonal_mean = seasonal_stats.loc[season, "mean"]
        seasonal_std = seasonal_stats.loc[season, "std"]

        deviation = current["temperature"] - seasonal_mean
        # 7.85 > 2 * 5
        """
        вот тут немного затупил, из-за данных, на момент эксперемента аномалий не было, что меня смутило
        но потом, на следующий день увидел одну аномалию, вначале запутался поэтому написал не по условию детект аномалии
        (закоменченая строчка) но потом вроде разобрался
        """
        is_anomaly = abs(deviation) > abs(seasonal_std) * 2
        # is_anomaly = abs(deviation) > (abs(seasonal_std) + abs(seasonal_mean)) # исправил из-за условя, было seasonal_std  * 2

        return {
            "current_temp": current["temperature"],
            "description": current["description"],
            "seasonal_mean": seasonal_mean,
            "seasonal_std": seasonal_std,
            "deviation": deviation,
            "is_anomaly": is_anomaly,
            "anomaly_desc": f"Аномалия: {'Да' if is_anomaly else 'Нет'} (отклонение {deviation:.2f}°C)",
        }

    # графики
    def plot_time_series(self, city_data: pd.DataFrame, window_size: int, threshold: float) -> go.Figure:
        city_data = city_data.copy()
        city_data = self.calculate_rolling_mean(city_data, window_size)
        city_data[f"std_{window_size}"] = (
            city_data["temperature"].rolling(window=window_size, center=True, min_periods=1).std()
        )
        ma_col = f"ma_{window_size}"
        std_col = f"std_{window_size}"
        anomalies_mask = (city_data["temperature"] > city_data[ma_col] + threshold * city_data[std_col]) | (
            city_data["temperature"] < city_data[ma_col] - threshold * city_data[std_col]
        )
        anomalies = city_data[anomalies_mask].copy()
        anomalies["deviation"] = anomalies["temperature"] - anomalies[ma_col]
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=city_data["timestamp"],
                y=city_data["temperature"],
                mode="lines",
                name="Температура",
            )
        )
        fig.add_trace(go.Scatter(x=city_data["timestamp"], y=city_data[ma_col], mode="lines", name="MA"))
        fig.add_trace(
            go.Scatter(
                x=anomalies["timestamp"],
                y=anomalies["temperature"],
                mode="markers",
                name="Аномалии",
                marker=dict(color="red"),
            )
        )
        fig.update_layout(
            title="Временной ряд температуры",
            xaxis_title="Дата",
            yaxis_title="Температура (°C)",
            width=800,
            height=600,
        )
        return fig

    def plot_seasonal_profile(self, seasonal_stats: pd.DataFrame) -> go.Figure:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=seasonal_stats.index, y=seasonal_stats["mean"], name="Среднее"))
        fig.add_trace(
            go.Scatter(
                x=seasonal_stats.index,
                y=seasonal_stats["mean"],
                mode="markers",
                marker=dict(color="rgba(0,0,0,0)", size=0),
                error_y=dict(type="data", array=seasonal_stats["std"], visible=True),
                name="STD",
            )
        )
        fig.update_layout(
            title="Сезонный профиль",
            xaxis_title="Сезон",
            yaxis_title="Температура (°C)",
            width=800,
            height=600,
        )
        return fig

    def plot_heatmap_anomalies(self, anomalies: pd.DataFrame) -> go.Figure:
        if len(anomalies) == 0:
            fig = go.Figure()
            fig.update_layout(title="Тепловая карта аномалий (нет данных)", width=800, height=600)
            return fig
        anomalies["year"] = anomalies["timestamp"].dt.year
        anomalies["month"] = anomalies["timestamp"].dt.month
        heatmap_data = anomalies.groupby(["year", "month"]).size().unstack(fill_value=0)
        fig = go.Figure(
            data=go.Heatmap(
                z=heatmap_data.values,
                x=heatmap_data.columns,
                y=heatmap_data.index,
                colorscale="Reds",
            )
        )
        fig.update_layout(
            title="Тепловая карта аномалий",
            xaxis_title="Месяц",
            yaxis_title="Год",
            width=800,
            height=600,
        )
        return fig

    def plot_trend(self, city_data: pd.DataFrame, trend: dict) -> go.Figure:
        city_data = city_data.copy()
        city_data["timestamp"] = pd.to_datetime(city_data["timestamp"])  # Фикс: Убедимся в типе
        city_data["days"] = (city_data["timestamp"] - city_data["timestamp"].min()).dt.days
        trend_line = trend["intercept"] + trend["slope"] * city_data["days"]
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=city_data["timestamp"],
                y=city_data["temperature"],
                mode="markers",
                name="Данные",
                marker=dict(opacity=0.5),
            )
        )
        fig.add_trace(go.Scatter(x=city_data["timestamp"], y=trend_line, mode="lines", name="Тренд"))
        fig.update_layout(
            title="Долгосрочный тренд",
            xaxis_title="Дата",
            yaxis_title="Температура (°C)",
            width=800,
            height=600,
        )
        return fig

    def plot_seasonal_boxplot(self, city_data: pd.DataFrame) -> go.Figure:
        fig = go.Figure()
        for season in city_data["season"].unique():
            season_data = city_data[city_data["season"] == season]["temperature"]
            fig.add_trace(go.Box(y=season_data, name=season))
        fig.update_layout(
            title="Boxplot температур по сезонам",
            xaxis_title="Сезон",
            yaxis_title="Температура (°C)",
            width=800,
            height=600,
        )
        return fig

    def plot_temperature_scatter(self, city_data: pd.DataFrame, trend: dict) -> go.Figure:
        city_data = city_data.copy()
        city_data["timestamp"] = pd.to_datetime(city_data["timestamp"])
        city_data["days"] = (city_data["timestamp"] - city_data["timestamp"].min()).dt.days
        trend_line = trend["intercept"] + trend["slope"] * city_data["days"]
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=city_data["days"],
                y=city_data["temperature"],
                mode="markers",
                name="Температура",
            )
        )
        fig.add_trace(go.Scatter(x=city_data["days"], y=trend_line, mode="lines", name="Тренд"))
        fig.update_layout(
            title="Scatter температуры vs дней",
            xaxis_title="Дни",
            yaxis_title="Температура (°C)",
            width=800,
            height=600,
        )
        return fig
