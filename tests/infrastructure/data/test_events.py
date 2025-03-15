# -*- encoding: utf-8 -*-
"""
@File    :   test_events.py
@Time    :   2025/03/02 15:07:25
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
"""

import unittest

import pandas as pd

from core.infrastructure.data.events import EventsDataProcessor
from core.ShoreNet.definitions.variables import ShoreNetVariablesManager as Vm

class TestEvents(unittest.TestCase):
    stage_env = "test"
    vars = Vm(stage_env)

    def test_events(self,):
        pass
