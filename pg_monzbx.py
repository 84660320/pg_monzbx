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


def run(sendtrap=True, discover=False):
    log = logger().getlogger

    pg = postgresql(host='localhost', port=5432, dbname='postgres', dbuser='postgres', password='postgres')
    pgb = pgbouncer(host='localhost', port=6432, dbname='pgbouncer', dbuser='postgres', password='postgres')

    log.info("parameter discover = {} sendtrap = {}".format(discover, sendtrap))

    if sendtrap:
        info = {}
        log.info("sendtrap start")

        pgInfo = pg.info()
        info.update(pgInfo)

        pgbInfo = pgb.info()
        info.update(pgbInfo)
        log.info("sendtrap result: {}".format(info) )

        zbx = sendproto('127.0.0.1', 10051)
        resp = zbx.send(info)
        log.info("send status: {}".format(resp) )

    elif discover:
        info = {}
        log.info("discover start")

        pgInfo = pg.discover()
        #info.update(pgInfo)

        pgbInfo = pgb.discover()
        #info.update(pgbInfo)
        info['data'] = pgInfo + pgbInfo

        log.info("discover result: {}".format(info) )
        print (json.dumps(info, indent=4))

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--discover', help='discover monitor item', action='store_true')
    parser.add_argument('--check', help='check monitor item', action='store_true')
    parser.add_argument('--sendtrap', help='send to zabbix server', action='store_true')
    parser.add_argument('--debug', help='set log level to debug', action='store_true')
    args = parser.parse_args()

    run(args.sendtrap, args.discover)


