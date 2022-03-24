"""
Data Schema:

-id
-slug
-configured_indicators
-status
-created_at
-start_at
-end_at
-total_reports
-total_indicators
-reports
 --status
 --<array>report
  ---indicators_counts
  ---amount
  ---observed_at
-indicators
 --status
 --<array>indicator
   ---slug
   ---id
   ---amount
-providers
 --status
 --<array>provider
   ---name
   ---id
   ---amount
-networks
 --status
 --<array>network
   ---name
   ---id
   ---amount
"""
import logging
from common.constants import Status


class ParseIndicators(object):
    @staticmethod
    def execute(response, scraper_config):
        try:
            result = {}
            total_indicators = 0
            json_response = response.json()
            result["indicators"] = {
                "status": Status.SUCCESS.value,
                "indicator": []
            }
            for element in json_response:
                current_indicator = {
                    "slug": element["indicator"]["slug"],
                    "amount": element["amount"],
                    "id": element["indicator"]["id"],
                }
                total_indicators = total_indicators + current_indicator["amount"]
                result["indicators"]["indicator"].append(current_indicator)
            result["total_indicators"] = total_indicators
            return True, result, ''
        except Exception as ex:
            logging.error(f"Parsing of Indicators API of downdetector failed with::{ex}")
            return False, {
                "status": Status.FAILURE.value,
                "indicators": {
                    "status": Status.FAILURE.value,
                    "indicator": []
                },
                "total_indicators": 0
            }, str(ex)
