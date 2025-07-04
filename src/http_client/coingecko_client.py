import requests
from tenacity import retry, stop_after_attempt, wait_exponential


class CoingeckoClient:
    """Класс для работы с api coingecko."""
    BASE_URL = 'https://api.coingecko.com/api/v3'

    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers["x-cg-demo-api-key"] = api_key


    def ping(self) -> bool:
        """Метод ping для проверки, что запросы летят правильно.

        Returns:
            Возвращет True если status_code == 200, иначе False
        """
        url = f'{self.BASE_URL}/ping'
        response = self.session.get(url)

        return response.status_code == 200

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=5, max=60)
    )
    def fetch_history(self, coin_id: str, date: str) -> dict:
        """Получение иторических данных market_chart.

        Args:
            coin_id - id криптовалюты, данные которой хотим получить
            date - дата снэпшота в формате 'dd-mm-yyyy' или количество дней назад

        Returns:
            Данные от API в виде словаря.

        Raises:
            requests.exceptions.HTTPError: Если API возвращает ошибку после всех попыток.

        """
        url = f'{self.BASE_URL}/coins/{coin_id}/history'
        response = self.session.get(url, params={
            'date': date
        })
        response.raise_for_status()

        return response.json()
