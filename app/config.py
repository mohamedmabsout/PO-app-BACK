from pydantic_settings import BaseSettings, SettingsConfigDict
from fastapi_mail import ConnectionConfig
class Settings(BaseSettings):
    DATABASE_URL: str
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_DAYS: int
    java_api_base_url: str
    java_api_username: str
    java_api_password: str

    # Huawei eSupplier B2B
    huawei_app_id: str = "APP_Z06PRE_B2BTEST"
    huawei_app_key: str = ""
    huawei_env: str = "test"  # "test" or "prod"

    # This tells pydantic to load the variables from a .env file
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

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