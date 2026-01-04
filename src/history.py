import pandas as pd
from loguru import logger

def load_data(file_path:str) -> pd.DataFrame:
    logger.info(f'load data from {file_path}')
    try:
        df = pd.read_csv(file_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.sort_values(['city', 'timestamp'], inplace=True)
        logger.debug('Data loaded')
    except Exception as e:
        logger.error(f'sombody error {e}')
        raise

