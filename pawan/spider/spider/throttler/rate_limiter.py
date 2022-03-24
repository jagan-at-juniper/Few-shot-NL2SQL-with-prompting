import datetime
import logging


class RateLimiter(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.redis_obj = kwargs.get("redis")
        self.window = ["minute", "hour", "day"]
        self.lock = self.redis_obj.lock("rate_limiter")
        self.logger = logging.getLogger(__name__)

    def __valid_window(self, window):
        return window in self.window

    def __get_value(self, key):
        if self.redis_obj.get(key):
            return self.redis_obj.get(key)
        return 0

    def __get_window_params(self, window_type):
        window_status = self.__valid_window(window_type)
        if not window_status:
            self.logger.error(f"Incorrect window:{self.window}")
            raise Exception(f"Incorrect window:{self.window}")
        current_time = datetime.datetime.now()
        current_timestamp = int(current_time.timestamp())
        windows = {
            'minute': {
                'passed_time': current_time.second,
                'window_value': 60,
                'window_unit': 60
            },
            'hour': {
                'passed_time': (60 * current_time.minute) + current_time.second,
                'window_value': 60 * 60,
                'window_unit': 60
            },
            'day': {
                'passed_time': (60 * 60 * current_time.hour) + (60 * current_time.minute) + current_time.second,
                'window_value': 60 * 60 * 24,
                'window_unit': 24
            }
        }
        window_params = windows.get(window_type)
        params = {
            # how much current window time has passed
            'current_window_fraction': window_params.get('passed_time') / window_params.get('window_unit'),
            # starting timestamp of current window (to be used for naming cache key)
            'current_timestamp': current_timestamp - window_params.get('passed_time')
        }
        # starting timestamp of previous window
        params['previous_timestamp'] = params['current_timestamp'] - window_params.get('window_value')
        # cache expiry timestamp for current window key
        params['expiry_timestamp'] = params['current_timestamp'] + (2 * window_params.get('window_value'))
        return params

    def __get_available_limit(self, node_name, user_id, window_type, limit):
        params = self.__get_window_params(window_type)
        previous_key = f'{node_name}_{user_id}_{params["previous_timestamp"]}'
        current_key = f'{node_name}_{user_id}_{params["current_timestamp"]}'

        prev_window_data = self.__get_value(previous_key)  # get previous window request count
        current_window_data = self.__get_value(current_key)  # get current window request count
        current_request_count = current_window_data + ((1 - params['current_window_fraction']) * prev_window_data)
        return limit - current_request_count, params["current_timestamp"]

    def update_count(self, node_name, user_id, rate_limit):
        try:
            is_blocked = self.lock.acquire(blocking_timeout=self.kwargs.get("blocking_timeout", 60))
            if not is_blocked:
                self.logger.error(f"Acquiring of redis lock failed at {int(datetime.datetime.now().timestamp())}")
                return False
            for window_type, limit in rate_limit.items():
                available_limit, current_timestamp = self.__get_available_limit(node_name, user_id, window_type, limit)
                if int(available_limit) == 0:
                    self.logger.debug(
                        f"Limit exhausted at {current_timestamp} for {f'{node_name}_{user_id}_{current_timestamp}'}")
                    self.lock.release()
                    return False
                else:
                    key = f'{node_name}_{user_id}_{current_timestamp}'
                    key_count = self.__get_value(key, 0)
                    self.redis_obj.set(f'{node_name}_{user_id}_{current_timestamp}', key_count + 1)
                    self.logger.info(f'Count for {key} increased to {key_count + 1}')
            self.lock.release()
            return True
        except Exception as ex:
            self.logger.error(
                f"RateLimiter failed for {node_name}_{user_id}_{int(datetime.datetime.now().timestamp())} with {ex}")
            self.lock.release()
