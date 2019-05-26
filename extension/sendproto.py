# -*- coding:utf-8 -*-

import socket
import json
import struct
from logger import logger


log = logger().getlogger

class sendproto(object):
    def __init__(self, server, port):
        #self.server_ip = socket.gethostbyname(server)
        self.server_ip = server
        self.server_port = port
        self.hostname = socket.gethostname()

    def send(self, data):
        zbx_data = {
            'request': 'sender data',
            'data': []
        }

        for key in data:
            zbx_data['data'].append({
                'host': self.hostname,
                'key': key,
                'value': data[key]
            })

        #log.info(json.dumps(zbx_data, indent=4))
        log.info("send zabbix data: {} ".format(zbx_data))

        zbx_data_json = json.dumps(zbx_data)
        send_data = struct.pack("<4sBq{}s".format(len(zbx_data_json)), "ZBXD", 1, len(zbx_data_json), zbx_data_json)
        so = socket.socket()
        so.connect((self.server_ip, self.server_port))
        wobj = so.makefile('wb')
        wobj.write(send_data)
        wobj.close()

        robj = so.makefile('rb')
        recv_data = robj.read()
        robj.close()
        so.close()

        tmp_data = struct.unpack("<4sBq{}s".format(len(recv_data) - struct.calcsize("<4sBq")), recv_data)
        recv_json = json.loads(tmp_data[3])
        return recv_json

