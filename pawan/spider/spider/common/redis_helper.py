import redis

from common.constants import REDIS_DETAILS


def get_redis_object():
    return redis.Redis(**REDIS_DETAILS)
