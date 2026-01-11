import asyncio

import pandas as pd
import streamlit as st
from loguru import logger

from historycal_analiz import HistoricalDataAnalyzer


def run_analysis():
    st.set_page_config(page_title="Анализ температурных данных", page_icon="🌡️", layout="wide")
    st.title("🌡️ Анализ исторических и текущих температурных данных")

    # решил сделать через  боковое меню, так показалось будет лучше выглядеть
    with st.sidebar:
        st.header("Настройки")
        uploaded_file = st.file_uploader(
            "Загрузите CSV файл",
            type=["csv"],
            help="Формат: city, timestamp, temperature, season",
        )
        api_key_input = st.text_input("API-ключ OpenWeatherMap", type="password", help="Для текущей погоды")
        if api_key_input:
            st.session_state["api_key"] = api_key_input

    # обработчик загрузки
    df = None
    analyzer = None
    cities = []
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            required_columns = ["city", "timestamp", "temperature", "season"]
            missing = [col for col in required_columns if col not in df.columns]
            if missing:
                st.error(f'Отсутствуют колонки: {", ".join(missing)}')
                return
            analyzer = HistoricalDataAnalyzer(df)
            cities = sorted(df["city"].unique())
            st.sidebar.success("Данные загружены")
        except Exception as e:
            st.error(f"Ошибка: {e}")
            return

    # скрытые меню (отбражать после загрузки файла)
    if analyzer:
        with st.sidebar:
            selected_city = st.selectbox("Выберите город", cities)
            window_size = st.slider("Окно скользящего среднего (дни)", 7, 90, 30)
            anomaly_threshold = st.slider("Порог аномалий (σ)", 1.0, 3.0, 2.0, 0.5)

            analysis_method = st.radio(
                "Метод анализа",
                [
                    "Синхронный",
                    "Параллельный (Joblib)",
                    "Многопоточный",
                    "Многопроцессный",
                    "Асинхронный",
                    "Бенчмарк всех методов",
                ],
            )

            api_method_ru = st.radio("Метод API", ["Синхронный", "Асинхронный"], index=0) if api_key_input else None
            if api_method_ru:
                method_map = {"Синхронный": "sync", "Асинхронный": "async"}
                st.session_state["api_method"] = method_map.get(api_method_ru, "sync")

    # текущая погода вверху (только если ключ введён и файл загружен)
    api_key = st.session_state.get("api_key", "")
    api_method = st.session_state.get("api_method", "sync")
    if analyzer and selected_city and api_key:
        st.header("Текущая погода")
        try:
            with st.spinner("Загрузка погоды..."):
                current_analysis = analyzer.analyze_current_weather(selected_city, api_key, api_method)
            st.write(
                f"Для {selected_city}: Температура {current_analysis['current_temp']}°C ({current_analysis['description']})"
            )
            st.write(
                f"Сезонная норма: {current_analysis['seasonal_mean']:.2f} ± {current_analysis['seasonal_std']:.2f}°C"
            )
            st.write(current_analysis["anomaly_desc"])
        except Exception as e:
            st.error(f"Ошибка: {e}")
            logger.error(f"Error in weather display: {e}")

    if analyzer and selected_city:
        st.header(f"Анализ для {selected_city}")

        if analysis_method == "Бенчмарк всех методов":
            with st.spinner("Бенчмарк..."):
                benchmark = analyzer.benchmark_methods(selected_city, window_size, anomaly_threshold)
            st.subheader("Результаты бенчмарка (время, сек)")
            st.table(benchmark)
            results = analyzer.analyze_city_sync(selected_city, window_size, anomaly_threshold)
        elif analysis_method == "Синхронный":
            results = analyzer.analyze_city_sync(selected_city, window_size, anomaly_threshold)
        elif analysis_method == "Параллельный (Joblib)":
            results = analyzer.analyze_city_parallel([selected_city], window_size, anomaly_threshold, "joblib")[
                selected_city
            ]
        elif analysis_method == "Многопоточный":
            results = analyzer.analyze_city_parallel([selected_city], window_size, anomaly_threshold, "multithread")[
                selected_city
            ]
        elif analysis_method == "Многопроцессный":
            results = analyzer.analyze_city_parallel([selected_city], window_size, anomaly_threshold, "multiprocess")[
                selected_city
            ]
        elif analysis_method == "Асинхронный":
            results = asyncio.run(analyzer.analyze_city_async(selected_city, window_size, anomaly_threshold))

        # визуализация резщов
        st.subheader("Базовая статистика")
        st.table(results["stats"])

        st.subheader("Аномалии")
        st.write(
            f"Количество: {results['anomalies']['anomaly_count']} ({results['anomalies']['anomaly_percent']:.2f}%)"
        )
        st.dataframe(results["anomalies"]["anomalies"])

        st.subheader("Сезонный профиль")
        st.table(results["seasonal"])

        st.subheader("Тренд")
        st.write(results["trend"]["trend_description"])

        # графики
        city_data = df[df["city"] == selected_city]
        st.subheader("Графики")

        st.plotly_chart(
            analyzer.plot_time_series(city_data, window_size, anomaly_threshold),
            width="stretch",
        )

        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(analyzer.plot_seasonal_profile(results["seasonal"]), width="stretch")
            st.plotly_chart(analyzer.plot_seasonal_boxplot(city_data), width="stretch")
        with col2:
            st.plotly_chart(
                analyzer.plot_heatmap_anomalies(results["anomalies"]["anomalies"]),
                width="stretch",
            )
            st.plotly_chart(analyzer.plot_trend(city_data, results["trend"]), width="stretch")


# для тестов делал
if __name__ == "__main__":
    run_analysis()
