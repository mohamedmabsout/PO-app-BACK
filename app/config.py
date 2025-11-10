from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int

    # This tells pydantic to load the variables from a .env file
    model_config = SettingsConfigDict(env_file=".env")

# Create a single instance that the rest of our app can import
settings = Settings()
