import logging
import os
import time

from common.utils import Utils
from scraper.workflow_manager import WorkflowManager


class DemandGenerator(object):
    def __init__(self, **kwargs):
        self.demand_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'demand.csv')
        self.workflow_manager = WorkflowManager(**kwargs)
        self.logger = logging.getLogger(__name__)

    def execute_demand(self):
        demands = Utils.get_csv_file_data(self.demand_file)
        while True:
            for demand in demands:
                for _ in demand[-1]:
                    file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), demand[3])
                    self.workflow_manager.execute_workflow(demand[0], file_path)
            time.sleep(1)
