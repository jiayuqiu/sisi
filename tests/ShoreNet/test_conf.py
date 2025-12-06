# -*- encoding: utf-8 -*-
'''
@File    :   test_conf.py
@Time    :   2025/03/02 00:38:14
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

import os
import unittest
from dotenv import load_dotenv
from sqlalchemy.engine import Engine

from sisi_ops.ShoreNet.conf import connect_mysql, connect_sqlite
from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager


class TestEvents(unittest.TestCase):
    load_dotenv(".env")
    stage_env = os.environ["TEST_STAGE_ENV"]  # TODO: deduplicate stage_env loading code.
    db_path = os.environ["DB_PATH"]
    # vars = ShoreNetVariablesManager(stage_env)

    @unittest.skip("Skipping MySQL")
    def test_connect_mysql(self):
        engine = connect_mysql(self.stage_env)
        self.assertIsInstance(engine, Engine)
    
    def test_connect_sqlite(self):
        engine = connect_sqlite(self.db_path)
        self.assertIsInstance(engine, Engine)