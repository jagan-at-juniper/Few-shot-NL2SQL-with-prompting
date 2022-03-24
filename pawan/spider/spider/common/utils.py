import datetime
import hashlib
import uuid
import csv

from common.constants import Status, DATE_FORMAT


class Utils(object):
    @staticmethod
    def get_uuid(name, epoch, reference_id=None):
        if reference_id:
            return str(uuid.UUID(hex=hashlib.md5(f'{name}_{epoch}-{reference_id}'.encode('utf-8')).hexdigest(),
                                 is_safe=uuid.SafeUUID.safe))
        return str(
            uuid.UUID(hex=hashlib.md5(f'{name}_{epoch}'.encode('utf-8')).hexdigest(), is_safe=uuid.SafeUUID.safe))

    @staticmethod
    def update_task_status(mysql_object, task_id, status, error_msg=''):
        dt_now = datetime.datetime.now()
        modified_date = dt_now.strftime(DATE_FORMAT)
        if error_msg:
            return mysql_object.execute_query(
                f"""
                UPDATE task 
                SET status = {status}, modified_time = {modified_date}, error = {error_msg}
                where id = {task_id};
                """
            )
        return mysql_object.execute_query(
            f"""
                        UPDATE task 
                        SET status = {status}, modified_time = {modified_date}
                        where id = {task_id};
                        """
        )

    @staticmethod
    def create_task(mysql_object, task_name, workflow_id):
        dt_now = datetime.datetime.now()
        dt_iso_format = dt_now.strftime(DATE_FORMAT)
        current_epoch = int(dt_now.timestamp())
        task_id = Utils.get_uuid(name=task_name, epoch=current_epoch, reference_id=workflow_id)
        return mysql_object.execute_query(
            f"""
            INSERT INTO task
                VALUES ({task_id},{workflow_id},{dt_iso_format},{dt_iso_format},{Status.SCRAPING_STARTED.value},);
            """
        ), task_id

    @staticmethod
    def update_workflow_status(mysql_object, workflow_id, status, error_msg=''):
        dt_now = datetime.datetime.now()
        modified_date = dt_now.strftime(DATE_FORMAT)
        if error_msg:
            return mysql_object.execute_query(
                f"""
                        UPDATE task 
                        SET status = {status}, modified_time = {modified_date}, error = {error_msg}
                        where id = {workflow_id};
                        """
            )
        return mysql_object.execute_query(
            f"""
            UPDATE task 
            SET status = {status}, modified_time = {modified_date}
            where id = {workflow_id};
            """
        )

    @staticmethod
    def create_workflow(mysql_object, workflow_name, demand_id):
        dt_now = datetime.datetime.now()
        dt_iso_format = dt_now.strftime(DATE_FORMAT)
        current_epoch = int(dt_now.timestamp())
        workflow_id = Utils.get_uuid(name=workflow_name, epoch=current_epoch, reference_id=demand_id)
        return mysql_object.execute_query(
            f"""
            INSERT INTO task
                VALUES ({workflow_id},{demand_id},{dt_iso_format},{dt_iso_format},{Status.WORKFLOW_STARTED.value},);
            """
        ), workflow_id

    @staticmethod
    def get_csv_file_data(csv_file_name):
        result = []
        with open(csv_file_name) as f:
            reader = csv.reader(csv_file_name)
            for row in reader:
                if not row[0].startswith("#"):
                    result.append([element.strip() for element in row])
        return result
