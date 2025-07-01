import logging

from requests.exceptions import HTTPError
from pandas import DataFrame
from pandas import to_datetime

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
    except HTTPError as e:
        logging.error(f'Get data failed! :(. {e}')
        return None
    except Exception as e:
        logging.error(f'Catch error: {e}')
        return None


def transform_market_chart_to_df(data: dict) -> DataFrame:
    """Трансформация полученных данных.

    Args:
        data - данные, которые нужно трасформировать

    Returns:
        Возвращает DataFrame  обработанных данных

    """
    df_prices = DataFrame(data['prices'], columns=['datetime', 'price'])
    df_caps = DataFrame(data['market_caps'], columns=['datetime', 'cap'])
    df_volumes = DataFrame(data['total_volumes'], columns=['datetime', 'volume'])

    df = df_prices.merge(df_caps, on='datetime').merge(df_volumes, on='datetime')

    df['datetime'] = to_datetime(df['datetime'], unit='ms')
    df.set_index('datetime', inplace=True)

    return df.round(3)


def save_dataframe_to_csv(data: DataFrame) -> None:
    """Сохранение обработанных данных в csv файл.


    Args:
        data - обработанные данные

    """
    data.to_csv(
        DATA_DIR / 'market_chart.csv',
        index=True,
        index_label='datetime',
    )


def save_dataframe_to_parquet(data: DataFrame) -> None:
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

    logging.info('API Data has been received successfully!')

    transformed_data = transform_market_chart_to_df(data)
    logging.info('Data transformed!')

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    save_dataframe_to_csv(transformed_data)
    save_dataframe_to_parquet(transformed_data)
    logging.info('ETL pipeline finished. Data saved successfully to CSV and Parquet files.')


if __name__ == '__main__':
    main()