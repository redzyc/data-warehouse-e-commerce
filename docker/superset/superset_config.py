import os
SESSION_COOKIE_NAME = "superset_session"
SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://hive:hive@hive-metastore-postgresql:5432/superset'
CSV_EXTENSIONS = {"csv", "tsv", "txt"}
SECRET_KEY = 'tajni_kljuc112233'
WTF_CSRF_ENABLED = True
ROW_LIMIT = 5000