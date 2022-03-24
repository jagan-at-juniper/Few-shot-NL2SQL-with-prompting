import datetime
import logging

import yaml


class ConfigManager(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def file_load(file_detail):
        return yaml.load(open(file_detail))

    def get_scraper_config(self, file_name, **kwargs):
        scraper_config = {}
        config = self.file_load(file_name)
        current_time = datetime.datetime.now()
        scraper_config["start_time"] = int(current_time.timestamp())
        scraper_config["end_time"] = scraper_config["start_time"] + config.get("frequency", 3600)
        scraper_config["env"] = self.kwargs.get("env")
        scraper_config["provider"] = self.kwargs.get("provider")
        for key, value in config.items():
            if key != 'workflow':
                scraper_config[key] = value
        scraper_config.update(kwargs)
        return scraper_config
