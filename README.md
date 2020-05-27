# pg_monzbx
use zabbix for monitor postgresql

########zabbix_agentd.conf

Include=/opt/zabbix/etc/zabbix_agentd.conf.d/pg.conf


# install
apt install python

apt install python-psycopg2

apt install python-jinja2
