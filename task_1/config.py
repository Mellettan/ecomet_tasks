from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    POSTGRES_DSN: str = "postgresql://user:password@localhost:5432/db_name"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
