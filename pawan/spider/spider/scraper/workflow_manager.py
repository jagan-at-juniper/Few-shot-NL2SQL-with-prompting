import logging

from common.constants import Status
from common.utils import Utils
from scraper.config_manager import ConfigManager
from scraper.request_processor import RequestProcessor


class WorkflowManager(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.logger = logging.getLogger(__name__)

    def execute_workflow(self, demand_id, workflow_file, **kwargs):
        try:
            scraper_config = ConfigManager(**kwargs).get_scraper_config(workflow_file)
            workflows = ConfigManager.file_load(workflow_file).get("workflow", {})
            for workflow in workflows:
                status, result, workflow_id = Utils.create_workflow(self.kwargs.get("mysql_obj"), workflow, demand_id)
                if not status:
                    Utils.update_workflow_status(self.kwargs.get("mysql_obj"), workflow_id,
                                                 Status.WORKFLOW_FAILURE.value, result)
                    self.logger.error(f"Creation of workflow{workflow} for {demand_id} failed:: {result}")
                    continue
                try:
                    RequestProcessor(workflow_id, scraper_config, workflows.get(workflow, {}), **kwargs).execute()
                except Exception as ex:
                    self.logger.error(f"Workflow {workflow_id} for {demand_id} failed:: {ex}")
                    Utils.update_workflow_status(self.kwargs.get("mysql_obj"), workflow_id,
                                                 Status.WORKFLOW_FAILURE.value, ex)
        except Exception as ex:
            self.logger.error(f"Workflow for {demand_id} got exception:: {ex}")
