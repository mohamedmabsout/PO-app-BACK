from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from fastapi_mail import ConnectionConfig

BASE_DIR = Path(__file__).resolve().parent.parent 

class Settings(BaseSettings):
    DATABASE_URL: str
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_DAYS: int

    INFOBIP_BASE_URL: str = "eejjzn.api.infobip.com"  # SANS https://
    INFOBIP_API_KEY: str = "df307328cf283b40a2095ff97be13d08-57c60b1e-4e3f-4a48-8bb2-a0557005be1d"                         # dans .env
    INFOBIP_SENDER: str = "ServiceSMS"
    OTP_TTL_SECONDS: int = 300
    # This tells pydantic to load the variables from a .env file
    class Config:
        env_file = ".env"
    sms_notifications_enabled: bool = True  # Master switch
    otp_ttl_seconds: int = 300

# Create a single instance that the rest of our app can import
settings = Settings()
conf = ConnectionConfig(
MAIL_USERNAME="mailsib6@gmail.com",
    MAIL_PASSWORD="xfhh jiap ziso jypn",
    MAIL_FROM="mailsib6@gmail.com",
    MAIL_PORT=587,
    MAIL_SERVER="smtp.gmail.com",
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True
)