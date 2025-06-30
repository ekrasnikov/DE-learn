import logging

import pandas as pd
import requests.exceptions

from config.settings import Settings, DATA_DIR
from http_client.coingecko_client import CoingeckoClient


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_coingecko_data(api_key: str) -> dict | None:
    """Работа с получением данных.

    Attributes:
        api_key - ключ api для авторизации в api coingecko

    """
    client = CoingeckoClient(api_key)

    try:
        return client.fetch_market_chart('bitcoin')
    except requests.exceptions.HTTPError as e:
        logging.error(f'Get data failed! :(. {e}')
        return None
    except Exception as e:
        logging.error(f'Catch error: {e}')


def transform_data(data: dict) -> pd.DataFrame:
    """Трансформация полученных данных.

    Attributes:
        data - данные, которые нужно трасформировать

    """
    prices_data = pd.DataFrame(data['prices'], columns=['datetime', 'price'])
    market_caps = pd.DataFrame(data['market_caps'], columns=['datetime', 'cap'])
    total_volumes = pd.DataFrame(data['total_volumes'], columns=['datetime', 'volume'])

    prices_data['datetime'] = pd.to_datetime(prices_data['datetime'], unit='ms')
    market_caps['datetime'] = pd.to_datetime(market_caps['datetime'], unit='ms')
    total_volumes['datetime'] = pd.to_datetime(total_volumes['datetime'], unit='ms')

    return pd.DataFrame(
        index=[*prices_data['datetime']],
        data={
            'price': [*prices_data['price'].apply(lambda x: round(x, 3))],
            'cap': [*market_caps['cap'].apply(lambda x: round(x, 3))],
            'volume': [*total_volumes['volume'].apply(lambda x: round(x, 3))],
        },
    )



def save_dataframe_to_csv(data: pd.DataFrame) -> None:
    """Сохранение обработанных данных в файл.


    Attributes:
        data - обработанные данные

    """

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    data.to_csv(
        DATA_DIR / 'market_chart.csv',
        index=True,
        index_label='datetime',
    )


def save_dataframe_to_parquet(data: pd.DataFrame) -> None:
    """Сохранение обработанных данных в файл.


    Attributes:
        data - обработанные данные

    """

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    data.to_parquet(DATA_DIR / 'market_chart.parquet', index=True)



def main():
    settings = Settings()
    data = get_coingecko_data(settings.api_key)

    if not data:
        return

    logging.info('Data got successfully!')

    transformed_data = transform_data(data)
    logging.info('Data transformed!')

    save_dataframe_to_csv(transformed_data)
    save_dataframe_to_parquet(transformed_data)
    logging.info('Data saved!')


if __name__ == '__main__':
    main()