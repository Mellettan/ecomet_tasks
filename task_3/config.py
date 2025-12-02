from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    GITHUB_TOKEN: str = ""
    CLICKHOUSE_URL: str = "http://localhost:8123"
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DB: str = "test"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
