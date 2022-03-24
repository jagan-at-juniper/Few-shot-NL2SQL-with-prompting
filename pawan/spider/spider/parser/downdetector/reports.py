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


class ParseReports(object):
    @staticmethod
    def execute(response, scraper_config):
        try:
            result = {}
            total_reports = 0
            json_response = response.json()
            result["start_at"] = scraper_config["start_time"]
            result["created_at"] = scraper_config["start_time"]
            result["end_at"] = scraper_config["end_time"]
            result["reports"] = {
                "status": Status.SUCCESS.value,
                "report": []
            }
            for element in json_response:
                current_report = {
                    "total_reports": element["total"],
                    "indicators_counts": element["indicators"],
                    "others": element["others"],
                    "observed_at": element["point_in_time"]
                }
                total_reports = total_reports + current_report["total_reports"]
                result["reports"]["report"].append(current_report)
            result["total_reports"] = total_reports
            return True, result, ''
        except Exception as ex:
            logging.error(f"Parsing of reports API of downdetector failed with::{ex}")
            return False, {
                "status": Status.FAILURE.value,
                "start_at": scraper_config["start_time"],
                "created_at": scraper_config["start_time"],
                "end_at": scraper_config["end_time"],
                "reports": {
                    "status": Status.FAILURE.value,
                    "report": []
                },
                "total_reports": 0
            }, str(ex)
