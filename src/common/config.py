# common/config.py

PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "dwh_spark"
PG_SCHEMA = "gold"
PG_USER = "od_api_user"
PG_PASSWORD = "X57tmQ846GYP3Jgb"

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}
