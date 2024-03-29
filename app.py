# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# File       : app.py
# Time       ：2024/3/29 16:25
# Author     ：zhang ji kang
# version    ：python 3.10
# Description：
"""
import os
from flask import Flask
from executor import executor
from views import bp


def create_app():
    cur_app = Flask(__name__)
    cur_app.config.from_mapping(
        JSON_AS_ASCII=False,
        SECRET_KEY=os.urandom(24),
        EXECUTOR_PUSH_APP_CONTEXT=True
    )
    executor.init_app(cur_app)

    cur_app.register_blueprint(bp, url_prefix='/task_async')
    return cur_app


app = create_app()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=6010, debug=True)
