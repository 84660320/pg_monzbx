# -*- coding:utf-8 -*-

import os
import re
import argparse

from logger import logger
from zbxcfg import zbxcfg
import zbx_query
from database import database


log = logger().getlogger

class postgresql(database):
    def __init__(self, args):
        database.__init__(self, type='pg')
        if args.settings:
            for paire in args.settings.split(','):
                key, value = paire.split('=')
                self.config[key] = value

    def show_slow_query(self):
        dict = {}
        slow_query_detail = []

        sql_string = zbx_query.get_slow_query(self.db_version, self.config['slow_query_sec'])
        rows = self.execute_sql(sql_string, True)
        dict["pg.[{}.slow_query.cnt]".format(self.port)] = len(rows)
        for row in rows:
            (pid, query, state, datname,usename, backend_conn_time, query_run_time, ip, waiting, wait_event_type, wait_event, appname) = row
            append_str = "\n\n\nip: {}" \
                             "\nwaiting: {}" \
                             "\nwait_event_type: {}" \
                             "\nwait_event: {}" \
                             "\ndatname: {}" \
                             "\nusename: {}" \
                             "\npid: {}" \
                             "\nappname: {}" \
                             "\nbackend_conn_time: {}" \
                             "\nstate: {}" \
                             "\nrun_time: {}" \
                             "\nquery: [{}]" \
                             "".format(ip, waiting, wait_event_type, wait_event, datname, usename, pid, appname,
                                       backend_conn_time, state, query_run_time, query)
            slow_query_detail.append(append_str)

        dict['pg.[{}.slow_query.detail]'.format(self.port)] = ";".join(slow_query_detail)

        return dict


    def show_lock_query(self):
        dict = {}
        lock_detail = []

        sql_string = zbx_query.get_lock_query(self.db_version)
        rows = self.execute_sql(sql_string, True)

        dict["pg.[{}.locks.cnt]".format(self.port)] = len(rows)

        for row in rows:
            (pid, state, transactionid, virtualxid, locktype, usename,
                 application_name, client_addr, waiting, wait_event_type,
                 wait_event, query_start, query_runtime, query, waitfor_pid, waitfor_state,
                 waitfor_transactionid, waitfor_virtualxid, waitfor_locktype,
                 waitfor_usename, waitfor_client_addr, waitfor_application_name,
                 waitfor_waiting, waitfor_wait_event_type, waitfor_wait_event,
                 waitfor_query_start, waitfor_query_runtime, waitfor_query) = row
            append_str = "\n\n\npid: {}\nstate: {}\ntransactionid:{}\nvirtualxid:{}\nlocktype:{}\nusename:{}\n" \
                             "application_name: {}\nclient_addr: {}\nwaiting: {}\nwait_event_type: {}\n" \
                             "wait_event: {}\nquery_start: {}\nquery_runtime: {}\nquery: {}\nwaitfor_pid: {}\nwaitfor_state: {}\n" \
                             "waitfor_transactionid: {}\nwaitfor_virtualxid: {}\nwaitfor_locktype: {}\n" \
                             "waitfor_usename: {}\nwaitfor_client_addr: {}\nwaitfor_application_name: {}\n" \
                             "waitfor_waiting: {}\nwaitfor_wait_event_type: {}\nwaitfor_wait_event: {}\n" \
                             "waitfor_query_start: {}\nwaitfor_query_runtime: {}\nwaitfor_query: {}" \
                             "".format(pid, state, transactionid, virtualxid, locktype, usename,
                                       application_name, client_addr, waiting, wait_event_type,
                                       wait_event, query_start, query_runtime, query, waitfor_pid, waitfor_state,
                                       waitfor_transactionid, waitfor_virtualxid, waitfor_locktype,
                                       waitfor_usename, waitfor_client_addr, waitfor_application_name,
                                       waitfor_waiting, waitfor_wait_event_type, waitfor_wait_event,
                                       waitfor_query_start,waitfor_query_runtime, waitfor_query)
            lock_detail.append(append_str)

        dict['pg.[{}.locks.detail]'.format(self.port)] = ";".join(lock_detail)

        return dict

    def show_streaming_query(self):

        dict = {}

        sql_string = "select pg_is_in_recovery();"
        rows = self.execute_sql(sql_string)
        if len(rows) < 1:
            return {}

        isMaster =  not rows[0]
        sql_string = zbx_query.get_streaming_query(self.db_version, isMaster)
        rows = self.execute_sql(sql_string, True)

        length = len(rows)

        if isMaster:
            dict = {'pg.[{}.master.slave_cnt]'.format(self.port): 0 , 'pg.[{}.master.slave_detail]'.format(self.port): ''}
            if length > 0:
                dict['pg.[{}.master.slave_cnt]'.format(self.port)] = length
                for row in rows:
                    application_name, wal_diff = row
                    dict['pg.[{}.master.app_{}_size_diff]'.format(self.port, application_name)] = int(wal_diff)
                    dict['pg.[{}.master.slave_detail]'.format(self.port)] = dict['pg.[{}.master.slave_detail]'.format(self.port)] + application_name
        else:
            dict['pg.[{}.slave.master_detail]'.format(self.port)] = ''
            if length > 0:
                for row in rows:
                    now, replay_time, wal_diff, master = row
                    dict['pg.[{}.slave.master_detail]'.format(self.port)] = "wal diff is: {}, master is: {}".format(int(wal_diff), master)

        return dict


    def show_simple_info(self):
        dict = {}

        sql_string = "select setting from pg_settings where name = 'max_connections';"
        max_connections = self.execute_sql(sql_string)[0]
        dict['pg.[{}.conn.max_cnt]'.format(self.port)] = int(max_connections)

        sql_string = "select count(1) from pg_stat_activity;"
        current_connections = self.execute_sql(sql_string)[0]
        dict['pg.[{}.conn.cnt]'.format(self.port)] = int(current_connections)

        return dict


    def info(self):
        postgres_info = {}

        # find all instance listen port
        for filename in os.listdir(self.unix_socket_directory):
            match = re.match('^.s.PGSQL.(\d+).lock$', filename)
            if match:
                self.port = match.group(1)
                log.debug("find postgresql listen port {} unix_socket_file {}/{}".format(self.port, self.unix_socket_directory, filename))

                self.dbconnect()
                # select database server_version
                sql_string = "select replace(setting, '.', ', ') from pg_settings where name = 'server_version';"
                self.db_version = tuple(self.execute_sql(sql_string)[0])

                postgres_info.update(self.show_slow_query())
                postgres_info.update(self.show_lock_query())
                postgres_info.update(self.show_streaming_query())
                postgres_info.update(self.show_simple_info())

                self.disconnect()

        return postgres_info


    def discover(self):
        postgres_list = []

        path_prefix = self.config['unix_socket_directory']
        # find all instance listen port
        for filename in os.listdir(path_prefix):
            tmp = {}
            match = re.match('^.s.PGSQL.(\d+).lock$', filename)
            if match:
                self.port = match.group(1)
                tmp['{#PG.MON}'] = 1
                tmp['{#PG.PORT}'] = self.port
                postgres_list.append(tmp)

                self.dbconnect()
                sql_string = "select pg_is_in_recovery()"
                isSlave = int(self.execute_sql(sql_string)[0])

                if not isSlave:
                    sql_string = "select application_name from pg_stat_replication"
                    rows = self.execute_sql(sql_string, True)
                    for row in rows:
                        app = {}
                        application_name = row[0]
                        app['{#PG.MASTER.APP}'] = application_name
                        app['{#PG.PORT}'] = self.port
                        postgres_list.append(app)

                self.disconnect()

        return postgres_list
