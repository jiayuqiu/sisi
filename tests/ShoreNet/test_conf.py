# -*- encoding: utf-8 -*-
'''
@File    :   test_conf.py
@Time    :   2025/03/02 00:38:14
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

import unittest
from sqlalchemy.engine import Engine

from core.ShoreNet.conf import connect_mysql, connect_sqlite
from core.ShoreNet.definitions.variables import ShoreNetVariablesManager


class TestEvents(unittest.TestCase):
    stage_env = "dummy"
    vars = ShoreNetVariablesManager(stage_env)

    def test_connect_mysql(self):
        engine = connect_mysql(self.stage_env)
        self.assertIsInstance(engine, Engine)