import pandas as pd

from src.config.settings import Settings
from src.http_client.base import HttpClient


def get_coingecko_data(api_key: str) -> dict:
    http_client = HttpClient(api_key)
    response = http_client.market_chart('bitcoin')

    return response


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
        '../marekt_caps.csv',
        index=True,
        index_label='datetime',
    )
    data.to_parquet('../marekt_caps.parquet', index=True)



def main():
    settings = Settings()
    data = get_coingecko_data(settings.api_key)
    transformed_data = transform_data(data)
    save_data_to_files(transformed_data)


if __name__ == '__main__':
    main()