import requests
from tenacity import retry, stop_after_attempt, wait_exponential


class HttpClient:
    BASE_URL = 'https://api.coingecko.com/api/v3'
    attempts = 0

    def __init__(self, api_key: str):
        self.headers = {
            "x-cg-demo-api-key": api_key
        }


    def ping(self) -> bool:
        url = f'{self.BASE_URL}/ping'
        response = requests.get(url, headers=self.headers)

        return response.status_code == 200

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=5, max=60)
    )
    def market_chart(self, coin_id: str) -> dict:
        url = f'{self.BASE_URL}/coins/{coin_id}/market_chart'
        query_params = {
            'days': 1,
            'vs_currency': 'usd',
        }
        response = requests.get(url, params=query_params, headers=self.headers)
        response.raise_for_status()

        return response.json()
