from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    env: str = Field(default="development", alias="ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    batch_size: int = Field(default=1000, alias="BATCH_SIZE")

    snowflake_account: str = Field(alias="SNOWFLAKE_ACCOUNT")
    snowflake_user: str = Field(alias="SNOWFLAKE_USER")
    snowflake_password: str = Field(alias="SNOWFLAKE_PASSWORD")
    snowflake_database: str = Field(default="UNIVERSITY_DW", alias="SNOWFLAKE_DATABASE")
    snowflake_warehouse: str = Field(default="UNIVERSITY_WH", alias="SNOWFLAKE_WAREHOUSE")
    snowflake_role: str = Field(default="SYSADMIN", alias="SNOWFLAKE_ROLE")
    snowflake_schema: str = Field(default="PROD", alias="SNOWFLAKE_SCHEMA")

    postgres_uri: str = Field(
        default="postgresql+psycopg2://university:university123@localhost:5432/university_staging",
        alias="POSTGRES_URI",
    )

    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_attendance_topic: str = Field(default="attendance-events", alias="KAFKA_ATTENDANCE_TOPIC")
    kafka_group_id: str = Field(default="university-analytics", alias="KAFKA_GROUP_ID")

    jwt_secret_key: str = Field(alias="JWT_SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")
    jwt_expire_minutes: int = Field(default=60, alias="JWT_EXPIRE_MINUTES")
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()