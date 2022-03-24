from enum import Enum

ATTRIBUTE_REGEX = r'{[\w\s\d]+}'
MYSQL_DETAILS = {
    "pool_name": "mysql_pool",
    "pool_size": 10,
    "pool_reset_session": True,
    "host": "127.0.0.1",
    "port": "3306",
    "user": "root",
    "password": "admin@123",
    "database": "spider"
}

REDIS_DETAILS = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 0
}

KAFKA_PRODUCER_DETAILS = {
    "bootstrap_servers": ["kafka01.prod.mist.com:9092"]
}

DATE_FORMAT = "%Y-%m-%dT%H-%M-%S"


class Status(Enum):
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'
    WORKFLOW_STARTED = "WORKFLOW_STARTED"
    WORKFLOW_SUCCESS = "WORKFLOW_SUCCESS"
    WORKFLOW_FAILURE = "WORKFLOW_FAILURE"
    SCRAPING_STARTED = "SCRAPING_STARTED"
    SCRAPING_SUCCESS = "SCRAPING_SUCCESS"
    PARSING_STARTED = "PARSING_STARTED"
    SCRAPING_FAILURE = "SCRAPING_FAILURE"
    PARSING_SUCCESS = "PARSING_SUCCESS"
    PARSING_FAILED = "PARSING_FAILURE"
