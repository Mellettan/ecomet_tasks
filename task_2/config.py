from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    GITHUB_TOKEN: str = ""
    MAX_CONCURRENT_REQUESTS: int = 5
    MAX_RPS: int = 10

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
