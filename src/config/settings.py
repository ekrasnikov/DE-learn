from pathlib import Path

from pydantic import PostgresDsn, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ROOT_DIR / '.env',
        env_file_encoding='utf-8'
    )

    api_key: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    @computed_field
    @property
    def database_url(self) -> PostgresDsn:
        """Construct the database URL from components."""
        return PostgresDsn.build(
            scheme="postgresql+psycopg2",
            username=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
            path=self.db_name,
        )
