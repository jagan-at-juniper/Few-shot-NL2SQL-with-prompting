import json
import logging

import kafka

from common.constants import KAFKA_PRODUCER_DETAILS


class KafkaProducer(object):

    @staticmethod
    def send_json_data(topic, data):
        try:
            kafka_config = KAFKA_PRODUCER_DETAILS
            kafka_config["topic"] = topic

            if isinstance(data, dict):
                try:
                    data = json.dumps(data).encode('utf-8')
                except Exception as e:
                    logging.error(f"Error in dumping the msg{data}::{ex}")
                    return False
                producer = kafka.KafkaProducer(**kafka_config)
                producer.send(topic, data)
                producer.flush()
        except Exception as ex:
            logging.error(f"Error in producing the msg{data}::{ex}")
            return False
        return True
