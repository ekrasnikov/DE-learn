import logging

import pandas as pd
import requests.exceptions

from config.settings import Settings, DATA_DIR
from http_client.coingecko_client import CoingeckoClient


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_coingecko_market_chart(api_key: str) -> dict | None:
    """Работа с получением данных.

    Args:
        api_key - ключ api для авторизации в api coingecko

    Returns:
        Возвращает данные из coingecko api

    """
    client = CoingeckoClient(api_key)
    params = {
        'days': 1,
        'vs_currency': 'usd',
    }

    try:
        return client.fetch_market_chart('bitcoin', params)
    except requests.exceptions.HTTPError as e:
        logging.error(f'Get data failed! :(. {e}')
        return None
    except Exception as e:
        logging.error(f'Catch error: {e}')
        return None


def transform_market_chart_to_df(data: dict) -> pd.DataFrame:
    """Трансформация полученных данных.

    Args:
        data - данные, которые нужно трасформировать

    Returns:
        Возвращает DataFrame  обработанных данных

    """
    df_prices = pd.DataFrame(data['prices'], columns=['datetime', 'price'])
    df_caps = pd.DataFrame(data['market_caps'], columns=['datetime', 'cap'])
    df_volumes = pd.DataFrame(data['total_volumes'], columns=['datetime', 'volume'])

    df = df_prices.merge(df_caps, on='datetime').merge(df_volumes, on='datetime')

    df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
    df = df.set_index('datetime')

    return df.round(3)


def save_dataframe_to_csv(data: pd.DataFrame) -> None:
    """Сохранение обработанных данных в csv файл.


    Args:
        data - обработанные данные

    """
    data.to_csv(
        DATA_DIR / 'market_chart.csv',
        index=True,
        index_label='datetime',
    )


def save_dataframe_to_parquet(data: pd.DataFrame) -> None:
    """Сохранение обработанных данных в parquet файл.


    Args:
        data - обработанные данные

    """
    data.to_parquet(DATA_DIR / 'market_chart.parquet', index=True)



def main():
    settings = Settings()
    data = get_coingecko_market_chart(settings.api_key)

    if not data:
        return

    logging.info('Data got successfully!')

    transformed_data = transform_market_chart_to_df(data)
    logging.info('Data transformed!')

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    save_dataframe_to_csv(transformed_data)
    save_dataframe_to_parquet(transformed_data)
    logging.info('Data saved!')


if __name__ == '__main__':
    main()