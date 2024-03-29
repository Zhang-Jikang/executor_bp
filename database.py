# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# File       : database.py
# Time       ：2024/3/29 16:10
# Author     ：zhang ji kang
# version    ：python 3.10
# Description：
"""
import pymysql
from loguru import logger

from configs import MYSQL_CONFIG


class Database:
    def __init__(self, host, user, password, dbname, port=3306):
        self.db = pymysql.connect(host=host, user=user, password=password, db=dbname, port=port)
        self.cursor = self.db.cursor()

    def insert(self, sql, params=None):
        """
        插入
        :param sql:
        :param params:
        :return:
        """
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            data_id = self.cursor.lastrowid
            self.db.commit()
            return data_id
        except Exception as e:
            self.db.rollback()
            logger.error(f"Insert error: {e}")
            return

    def update(self, sql, params=None):
        """
        更新
        :param sql:
        :param params:
        :return:
        """
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logger.error(f"Update error: {e}")

    def delete(self, sql, params=None):
        """
        删除
        :param sql:
        :param params:
        :return:
        """
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            self.db.commit()
        except Exception as err:
            logger.error(f"Delete error: {err}")
            self.db.rollback()

    def query(self, sql, params=None):
        """
        查询
        :param sql:
        :param params:
        :return:
        """
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            results = self.cursor.fetchall()
            return results
        except Exception as err:
            logger.error(f"error: {err}")

    def replace(self, sql, params=None):
        """
        替换
        :param sql:
        :param params:
        :return:
        """
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            self.db.commit()
        except Exception as err:
            self.db.rollback()
            logger.error(f"Replace error: {err}")

    def create_table(self, sql):
        """
        建表
        :param sql:
        :return:
        """
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as err:
            self.db.rollback()
            logger.error(f"Create error: {err}")

    def close(self):
        self.cursor.close()
        self.db.close()


def create_database():
    """
    创建数据库连接
    :return:
    """
    data_base = Database(MYSQL_CONFIG['host'],
                         MYSQL_CONFIG['user'],
                         MYSQL_CONFIG['password'],
                         MYSQL_CONFIG['database'])

    return data_base


db = create_database()

async_tasks_result_info_sql = """
CREATE TABLE IF NOT EXISTS async_tasks (
    task_id VARCHAR(36) PRIMARY KEY,
    status VARCHAR(10),
    result TEXT
);
"""
db.create_table(async_tasks_result_info_sql)
logger.info(f'create table success')
