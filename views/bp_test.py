# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# File       : bp_test.py
# Time       ：2024/3/29 14:02
# Author     ：zhang ji kang
# version    ：python 3.10
# Description：
"""
import time
import uuid
from loguru import logger

from database import create_database

from flask import Blueprint, jsonify, request
from executor import executor

bp = Blueprint('task_async', __name__)


def update_task_status(task_id, status, result=None):
    """
    更新任务状态到数据库
    :param task_id:
    :param status:
    :param result:
    :return:
    """
    replace_sql = "REPLACE INTO async_tasks (task_id, status, result) VALUES (%s, %s, %s)"
    db = create_database()
    try:
        db.replace(replace_sql, (task_id, status, result))
        logger.info(f'update status success :{(task_id, status, result)}')
    except Exception as err:
        logger.error(f'Error: {str(err)}')
    finally:
        db.close()


def task_process(task_id):
    """
    任务执行程序
    :param task_id:
    :return:
    """
    try:
        time.sleep(30)
        update_task_status(task_id, 'success', 'Task processing.')
        time.sleep(30)
        update_task_status(task_id, 'success', 'Task processing completed successfully.')
    except Exception as err:
        update_task_status(task_id, 'error', str(err))


@bp.route('/process', methods=['POST'])
def process():
    try:
        data = request.json
        params = data.get('params')

        task_id = str(uuid.uuid4())
        update_task_status(task_id, 'pending')

        executor.submit(task_process, task_id)
        return jsonify({'task_id': task_id, 'status': 'Task started.'})
    except Exception as err:
        return jsonify({'task_id': '', 'status': str(err)}), 404


@bp.route('/status/<task_id>', methods=['GET'])
def get_status(task_id):
    """

    :param task_id:
    :return:
    """
    query_sql = "SELECT status, result FROM async_tasks WHERE task_id = %s"
    db = create_database()
    try:
        task = db.query(query_sql, (task_id,))[0]
        logger.info(f'task:{task}')
        if task:
            return jsonify({'status': task[0], 'message': task[1]})
        else:
            return jsonify({'status': 'error', 'message': 'Invalid task ID'}), 404
    except Exception as err:
        return jsonify({'status': 'error', 'message': str(err)}), 404
    finally:
        db.close()
