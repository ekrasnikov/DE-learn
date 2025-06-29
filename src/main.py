import logging

import pandas as pd
import requests.exceptions

from src.config.settings import Settings
from src.http_client.base import HttpClient


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_coingecko_data(api_key: str) -> dict | None:
    http_client = HttpClient(api_key)

    try:
        return http_client.market_chart('bitcoin')
    except requests.exceptions.HTTPError as e:
        logging.error(f'Get data failed! :(. {e}')
        return None
    except Exception as e:
        logging.error(f'Catch error: {e}')


def transform_data(data: dict) -> pd.DataFrame:
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



def save_data_to_files(data: pd.DataFrame) -> None:
    data.to_csv(
        '../files/market_chart.csv',
        index=True,
        index_label='datetime',
    )
    data.to_parquet('../files/market_chart.parquet', index=True)



def main():
    settings = Settings()
    data = get_coingecko_data(settings.api_key)

    if not data:
        return

    logging.info('Data got successfully!')

    transformed_data = transform_data(data)
    logging.info('Data transformed!')

    save_data_to_files(transformed_data)
    logging.info('Data saved!')


if __name__ == '__main__':
    main()