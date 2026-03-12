from pydantic_settings import BaseSettings, SettingsConfigDict
from fastapi_mail import ConnectionConfig
class Settings(BaseSettings):
    DATABASE_URL: str
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_DAYS: int

    # This tells pydantic to load the variables from a .env file
    model_config = SettingsConfigDict(env_file=".env")

# Create a single instance that the rest of our app can import
settings = Settings()
conf = ConnectionConfig(
MAIL_USERNAME="erp.sib.system@gmail.com",
    MAIL_PASSWORD="dolr sopv qwks uxuf",
    MAIL_FROM="erp.sib.system@gmail.com",
    MAIL_PORT=587,
    MAIL_SERVER="smtp.gmail.com",
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True
)