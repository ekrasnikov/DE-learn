from datetime import datetime
import logging
import sys

from sqlalchemy import create_engine
from requests.exceptions import HTTPError
from pandas import DataFrame
from pandas import to_datetime

from config.settings import Settings
from http_client.coingecko_client import CoingeckoClient


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_coingecko_history(api_key: str, execution_date: str) -> dict | None:
    """Работа с получением данных.

    Args:
        api_key - ключ api для авторизации в api coingecko

    Returns:
        Возвращает данные из coingecko api

    """
    client = CoingeckoClient(api_key)

    try:
        return client.fetch_history('bitcoin', date=execution_date)
    except HTTPError as e:
        logging.error(f'Get data failed! :(. {e}')
        return None
    except Exception as e:
        logging.error(f'Catch error: {e}')
        return None


def transform_history_market_data_to_df(data: dict, execution_date: str, currency: str = 'usd') -> DataFrame:
    """Трансформация полученных данных.

    Args:
        data - данные, которые нужно трасформировать

    Returns:
        Возвращает DataFrame  обработанных данных

    """
    market_data = data.get('market_data', {})

    if not market_data:
        logging.error('No market data found in the response.')
        return DataFrame()

    single_day_data = {
        'price': market_data['current_price'][currency],
        'market_cap': market_data['market_cap'][currency],
        'total_volume': market_data['total_volume'][currency],
    }

    index_date = to_datetime(execution_date, format='%d-%m-%Y')
    df = DataFrame([single_day_data], index=[index_date])
    df.index.name = 'datetime'

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
    df.to_sql(table_name, con=engine, if_exists='append', index=True, index_label='datetime')

    logging.info(f"Data successfully loaded to '{table_name}' table in PostgreSQL.")


def main(execution_date: str, currency: str = 'usd') -> None:
    """Основная функция для запуска процесса получения и сохранения данных.
    
    Args:
        execution_date - дата за которую нужно получить данные
        currency - валюта, в которой нужно получить данные (по умолчанию 'usd')
    """
    settings = Settings()
    data = get_coingecko_history(settings.api_key, execution_date=execution_date)

    if not data:
        return

    logging.info('API Data has been received successfully!')

    transformed_data = transform_history_market_data_to_df(data, execution_date, currency)
    save_dataframe_to_db(transformed_data, str(settings.database_url), 'history_market_data')


def run_from_cli() -> None:
    """Запуск скрипта из командной строки."""
    if len(sys.argv) > 1:
        date_str = sys.argv[1]

        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            formatted_execution_date = date_obj.strftime('%d-%m-%Y')
            logging.info(f'Date {date_str} is valid. Starting data processing...')
            main(execution_date=formatted_execution_date)
        except ValueError:
            logging.error(f'Invalid date format: {date_str}. Please use "yyyy-mm-dd".')
            sys.exit(1)

    else:
        logging.error('Execution date is required as a command line argument. Enter date in format "yyyy-mm-dd".')
        sys.exit(1)


if __name__ == '__main__':
    run_from_cli()