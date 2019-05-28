# -*- coding:utf-8 -*-

import os
import re

from logger import logger
from database import database

log = logger().getlogger

class pgbouncer(database):

    def __init__(self, args):
        database.__init__(self, type='pgb')

    def show_databases(self):
        dblist = []
        sql_string = 'show databases;'
        rows = self.execute_sql(sql_string, True)
        for row in rows:
            dbname = row[0]
            if dbname in dblist:
                continue
            dblist.append(dbname)

        return dblist

    def show_pools(self, dblist):
        dict = {}
        sql_sring = 'show pools'
        rows = self.execute_sql(sql_sring, True)
        for row in rows:
            database, user, cl_active, cl_waiting, sv_active, sv_idle, sv_used, sv_tested, sv_login, maxwait = row[0:10]
            if database in self.config['db_black_list'] or database not in dblist:
                continue

            dict['pgb.[{}.{}.cl_waiting]'.format(self.port, database)] = int(cl_waiting)
            dict['pgb.[{}.{}.cl_active]'.format(self.port, database)] = int(cl_active)

        return dict

    def show_stats(self, dblist):
        dict = {}
        sql_sring = 'show stats'
        rows = self.execute_sql(sql_sring, True)
        for row in rows:
            (database, total_requests, total_received, total_sent, total_query_time, avg_req, avg_recv, avg_sent, avg_query) = row[0:9]
            if database in self.config['db_black_list'] or database not in dblist:
                continue

            dict['pgb.[{}.{}.avg_req]'.format(self.port, database)] = int(avg_req)
            dict['pgb.[{}.{}.avg_sent]'.format(self.port, database)] = int(avg_sent)
            dict['pgb.[{}.{}.avg_recv]'.format(self.port, database)] = int(avg_recv)
            dict['pgb.[{}.{}.avg_query]'.format(self.port, database)] = int(avg_query)

        return dict

    def info(self):
        pgbouncer_info = {}


        # find all instance listen port
        for filename in os.listdir(self.unix_socket_directory):
            match = re.match('^.s.PGSQL.(\d+)$', filename)
            if match and not os.path.isfile('{}/{}.lock'.format(self.unix_socket_directory, filename)):
                self.port = match.group(1)
                log.debug("find pgbouncer listen port {} unix_socket_file {}/{}".format(self.port, self.unix_socket_directory, filename))

                self.dbconnect()

                dblist = self.show_databases()
                pgbouncer_info.update(self.show_pools(dblist))
                pgbouncer_info.update(self.show_stats(dblist))

                self.disconnect()
        return pgbouncer_info


    def discover(self):
        pgbouncer_list = []

        path_prefix = self.config['unix_socket_directory']
        # find all instance listen port
        for filename in os.listdir(path_prefix):
            match = re.match('^.s.PGSQL.(\d+)$', filename)
            if match and not os.path.isfile('{}/{}.lock'.format(self.unix_socket_directory, filename)):
                self.port = match.group(1)

                self.dbconnect()
                sql_string = 'show databases;'
                rows = self.execute_sql(sql_string, True)
                for row in rows:
                    tmp = {}
                    dbname = row[0]
                    if dbname not in ('postgres', 'template0', 'template1','pgbouncer'):
                        tmp['{#PGB.DB}'] = dbname
                        tmp['{#PGB.PORT}'] = self.port
                        pgbouncer_list.append(tmp)

                self.disconnect()

        return pgbouncer_list
