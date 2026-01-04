import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')
from joblib import Parallel, delayed
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import asyncio
import aiohttp
from functools import partial


class historycal_data_analizer:
    def __init__(self, df):
        self.df = df.copy()
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.results = {}
        self.benchmark_times = {}
    ##базовый анализ
    def calculate_basic_statistic(self, city_data):
        return {
            'mean': city_data['temperature'].mean(),
            'std': city_data['temperature'].std(),
            'min': city_data['temperature'].min(),
            'max': city_data['temperature'].max(),
            'median': city_data['temperature'].median(),
            'q1': city_data['temperature'].quantile(0.25),
            'q3': city_data['temperature'].quantile(0.75),
            'count': len(city_data)
        }
    def calculate_srednee_scolz_for_one_city(self, city_data, window_size=30):
        #среднее скользящее для города 
        city_data = city_data.sort_values('timestamp')
        city_data[f'ma_{window_size}'] = city_data['temperature'].rolling(
            window=window_size, 
            center=True,
            min_periods=1
        ).mean()
        return city_data
    def detect_anomaly_for_one_city(self, city_data,window_size=30, threshold=2):
        #бнаружение аномалий для одного города
        city_data = self.calculate_srednee_scolz_for_one_city(city_data, window_size)
        city_data[f'std_{window_size}'] = city_data['temperature'].rolling(
            window=window_size, 
            center=True,
            min_periods=1
        ).std()
        ma_col = f'ma_{window_size}'
        std_col = f'std_{window_size}'
        
        # Определение аномалий
        anomalies_mask = (
            (city_data['temperature'] > city_data[ma_col] + threshold * city_data[std_col]) |
            (city_data['temperature'] < city_data[ma_col] - threshold * city_data[std_col])
        )
        
        anomalies = city_data[anomalies_mask].copy()
        anomalies['deviation'] = anomalies['temperature'] - anomalies[ma_col]
        
        return {
            'anomalies': anomalies,
            'anomaly_count': len(anomalies),
            'anomaly_percent': (len(anomalies) / len(city_data)) * 100
        }