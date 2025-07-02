import logging

from sqlalchemy import create_engine
from requests.exceptions import HTTPError
from pandas import DataFrame
from pandas import to_datetime

from config.settings import Settings
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
    df.set_index('datetime')

    logging.info('Data has been transformed successfully!')

    return df.round(3)

def save_dataframe_to_db(df: DataFrame, db_url: str, table_name: str) -> None:
    """Сохранение DataFrame в базу данных.

    Args:
        df - DataFrame, который нужно сохранить
        db_url - URL базы данных
        table_name - имя таблицы, в которую нужно сохранить данные

    """

    logging.info('Saving DataFrame to database...')

    engine = create_engine(db_url)
    df.to_sql(table_name, con=engine, if_exists='replace', index=True, index_label='datetime')

    logging.info(f"Data successfully loaded to '{table_name}' table in PostgreSQL.")



def main():
    settings = Settings()
    data = get_coingecko_market_chart(settings.api_key)

    if not data:
        return

    logging.info('API Data has been received successfully!')

    transformed_data = transform_market_chart_to_df(data)
    save_dataframe_to_db(transformed_data, str(settings.database_url), 'market_chart')


if __name__ == '__main__':
    main()