
import sys
import os
import random
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
except ImportError:
    pass

# Add script directory to path for imports
script_dir = os.path.join(os.path.dirname(__file__), 'script')
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

# PySpark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, concat, concat_ws, split, trim, substring,
    to_date, to_timestamp, current_timestamp,
    sum as _sum, count, avg, max as _max, min as _min,
    median, stddev, coalesce,
    row_number, rank, dense_rank
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, IntegerType, LongType, FloatType, DoubleType,
    DateType, TimestampType, BooleanType
)

# Import utility modules
from spark_session import create_spark_session_hive, create_spark_session_postgres
from check_db import *
from write_to_db import *

# Load environment variables from .env file
# Try multiple possible paths for flexibility
env_paths = [
    '/opt/spark/.env',  # In Spark containers
    os.path.join(os.path.dirname(__file__), '..', '.env'),  # Relative to src/ in local
]

for env_path in env_paths:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

HDFS_INPUT_PATH = os.getenv("HDFS_INPUT_PATH")
HIVE_TARGET_TABLE = os.getenv("HIVE_TARGET_TABLE")
HIVE_DB = os.getenv("HIVE_DB")
HDFS_OUTPUT_PATH = os.getenv("HDFS_OUTPUT_PATH")
PG_URL = os.getenv("PG_URL")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

__all__ = [
    'sys', 'os', 'datetime', 'timedelta', 'random',
    'SparkSession', 'DataFrame', 'Window',
    'col', 'lit', 'when', 'concat', 'concat_ws', 'split', 'trim', 'substring',
    'to_date', 'to_timestamp', 'current_timestamp',
    '_sum', 'count', 'avg', '_max', '_min', 'median', 'stddev', 'coalesce',
    'row_number', 'rank', 'dense_rank',
    'StringType', 'IntegerType', 'LongType', 'FloatType', 'DoubleType',
    'DateType', 'TimestampType', 'BooleanType',
    'create_spark_session_hive', 'create_spark_session_postgres',
    'HDFS_INPUT_PATH', 'HIVE_TARGET_TABLE', 'HIVE_DB', 'HDFS_OUTPUT_PATH',
    'PG_URL', 'PG_PROPERTIES', 'load_dotenv',
]
