from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATA_ROOT: str = "./data"
    OUTPUT_ROOT: str = "./outputs"
    SPOTIFY_CLIENT_ID: str | None = None
    SPOTIFY_CLIENT_SECRET: str | None = None
    SPOTIFY_REDIRECT_URI: str | None = None

    # Pydantic v2 config replaces the old inner Config class
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",  # ignore unexpected env vars
    )


settings = Settings()
