# -*- coding: utf-8 -*-

import json

class zbxcfg(object):
    def __init__(self, cfg='etc/config.json'):
        self.zbxcfg = {}
        try:
            with open(cfg, 'r') as f:
                self.zbxcfg = json.loads(f.read()) 
            f.close()
        except Exception as e:
           print(e)

    def config(self):
        return self.zbxcfg

