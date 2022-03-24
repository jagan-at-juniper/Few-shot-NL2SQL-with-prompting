import logging

from mysql.connector import pooling

from common.constants import MYSQL_DETAILS


class MysqlConnector(object):
    def __init__(self):
        self.pool = pooling.MySQLConnectionPool(**MYSQL_DETAILS)
        self.logger = logging.getLogger(__name__)

    def get_connection(self):
        return self.pool.get_connection()

    def execute_query(self, sql_query):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            try:
                self.logger.info(f"Executing the query::{sql_query}")
                cursor.execute(sql_query)
                rows = cursor.fetchall()
                connection.commit()
                cursor.close()
                connection.close()
                return True, rows
            except Exception as ex:
                self.logger.error(f"Execution of query{sql_query} failed with::{ex}")
                cursor.close()
                connection.close()
                return False, f"Execution of query{sql_query} failed with::{ex}"
        except Exception as ex:
            self.logger.error(f"Connection to MYSQL failed::{ex}")
            return False, f"Connection to MYSQL failed::{ex}"

    def execute_many_query(self, sql_query):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            try:
                self.logger.info(f"Executing the query::{sql_query}")
                cursor.executemany(sql_query)
                rows = cursor.fetchall()
                connection.commit()
                cursor.close()
                connection.close()
                return True, rows
            except Exception as ex:
                self.logger.error(f"Execution of query{sql_query} failed with::{ex}")
                cursor.close()
                connection.close()
                return False, f"Execution of query{sql_query} failed with::{ex}"
        except Exception as ex:
            self.logger.error(f"Connection to MYSQL failed::{ex}")
            return False, f"Connection to MYSQL failed::{ex}"

