import logging
import pymysql.cursors
from .config import Config


class RdsManager(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config = Config()

    def connect(self):
        connection = pymysql.connect(host=self.config.db_host,
                             user=self.config.db_user,
                             password=self.config.db_password,
                             db=self.config.db_name,
                             charset='utf8mb4',
                             use_unicode=True,
                             cursorclass=pymysql.cursors.DictCursor)
        return connection

    def get(self, sql, params=None):
        self.logger.debug("Get Operation:%s", sql)
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                result = cursor.fetchone()
                return result
            conn.commit()
        finally:
            conn.close()

    def find(self, sql, params=None):
        self.logger.debug("Find Operation:%s", sql)
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                result = cursor.fetchall()
                return result
            conn.commit()
        finally:
            conn.close()

    def execute(self, sql, params=None):
        self.logger.info("Execute Operation:%s", sql)
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
            conn.commit()
        finally:
            conn.close()
