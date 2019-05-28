#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import json
import argparse

os.chdir(os.path.split(os.path.abspath(__file__))[0])
sys.path.append('extension')

from logger import logger
from postgresql import postgresql
from pgbouncer import pgbouncer
from sendproto import sendproto


def run(args):
    log = logger().getlogger

    pg = postgresql(args)
    pgb = pgbouncer(args)

    if args.sendtrap:
        info = {}
        log.info("sendtrap start")

        pgInfo = pg.info()
        info.update(pgInfo)

        pgbInfo = pgb.info()
        info.update(pgbInfo)
        log.info("sendtrap result: {}".format(info))

        zbx = sendproto(pg.config['zbx_server'], pg.config['zbx_port'])
        resp = zbx.send(info)

        log.info("send status: {}".format(resp))

    elif args.discover:
        info = {}
        log.info("discover start")

        pgInfo = pg.discover()
        pgbInfo = pgb.discover()

        info['data'] = pgInfo + pgbInfo

        log.info("discover result: {}".format(json.dumps(info, sort_keys=True, indent=4, separators=(',', ':'))))
        print (json.dumps(info, indent=4))

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--discover', help='discover monitor item', action='store_true')
    parser.add_argument('--check', help='check monitor item', action='store_true')
    parser.add_argument('--sendtrap', help='send to zabbix server', action='store_true')
    parser.add_argument('--debug', help='set log level to debug', action='store_true')
    parser.add_argument('--settings', help='user settings', type=str)
    args = parser.parse_args()

    run(args)
