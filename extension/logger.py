# -*- coding: utf-8 -*-

import os
import json
import logging.config

class logger(object):
    def __init__(self, log_cfg='etc/logging.json', cfg_module='default'):
        logging.config.dictConfig(json.load(open(log_cfg, 'r')))
        try:
            self.logger = logging.getLogger(cfg_module)
            fd = open(log_cfg, 'r')
            config = fd.read()
            fd.close()

            logging.config.dictConfig(eval(json.dumps(config)))
        except Exception as e:
            pass
        finally:
            pass

    @property
    def getlogger(self):
        return self.logger

