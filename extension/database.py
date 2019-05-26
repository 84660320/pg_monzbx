# -*- coding:utf-8 -*-

import os
import re
import psycopg2
import time

from logger import logger
from zbxcfg import zbxcfg
from zbx_query import *

log = logger().getlogger

class database(object):
    conn = ''
    cur = ''

    def __init__(self, host='localhost', port=5432, dbname='postgres', dbuser='postgres', password='postgres'):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.dbuser = dbuser
        self.password = password
        self.config = zbxcfg().config()


    def dbconnect(self):
        url = "host={} port={} user={} dbname={} password={}".format(self.host, self.port, self.dbuser, self.dbname, self.password)
        try:
            self.conn = psycopg2.connect(url)
            self.conn.set_session(autocommit=True)
            try:
                self.cur = self.conn.cursor()
            except Exception as e:
                log.error("open cursor failed")
                log.error(e)
                self.dbdisconnect()
        except Exception as e:
                log.error("connect database %s faled" % (url))
                log.error(e)

        log.info(self.conn)

 
    def disconnect(self):
        if self.cur:
            self.cur.close()
            self.cur = ''
        if self.conn:
            self.conn.close()
            self.conn = ''


    def execute_sql(self, sql_string, fetchall = False):
        rows = ''
        try:
            log.debug(sql_string)
            self.cur.execute(sql_string)
            if fetchall:
                rows = self.cur.fetchall()
            else:
                rows = self.cur.fetchone()

            log.debug("results: {}".format(rows))
        except Exception as e:
            log.error("{} Failed, {} ".format(sql_string, self.conn))
            log.error(e)

        return rows
