import os
DB_USERNAME = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
SUPERSET_DATABASE_DB = os.environ.get('SUPERSET_DATABASE_DB')

SESSION_COOKIE_NAME = "superset_session"
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{SUPERSET_DATABASE_DB}"
CSV_EXTENSIONS = {"csv", "tsv", "txt"}
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY')
WTF_CSRF_ENABLED = True
ROW_LIMIT = 5000