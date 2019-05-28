from jinja2 import Template

SLOW_QUERY_SQL = '''
{%- if ver <= (9, 1) -%}
    {%- set query = 'current_query' -%}
    {%- set pid = 'procpid' -%}
{%- else -%}
    {%- set query = 'query' -%}
    {%- set pid = 'pid' -%}
{%- endif -%}
select
    {{pid}},
    {{query}},
  {% if ver <= (9,1) -%}
    'active' as state,
  {% else -%}
    state,
  {% endif -%}
    datname,
    usename,
    now() - backend_start as backend_conn_time,
    now() - query_start as query_run_time,
    client_addr,
  {% if ver <= (9,5) -%}
    waiting,
    'NULL' as wait_event_type,
    'NULL' as wait_event,
  {%- else -%}
    'NULL' as waiting,
    wait_event_type,
    wait_event,
  {%- endif -%}
    application_name
from
    pg_stat_activity
where
    client_port is not null
    and {{query}} !~* '^vacuum'
    and {{query}} !~* '^analyze|^analyse'
    and {{query}} !~* 'pg_start_backup|pg_stop_backup'
    and application_name <> 'pg_reorg'
    and ( application_name = 'psql' and {{query}} ~* '^COPY' ) = false
    and ( application_name = 'pg_dump' and {{query}} ~* '^COPY' and usename = 'pgdba' ) = false
    and {{query}} !~* 'INDEX +CONCURRENTLY'
    and {{query}} !~* 'pg_sleep'
    and {{query}} !~* 'refresh +materialized +view'
  {%- if ver <= (9,1) -%}
    and {{query}} not in ('<IDLE>', '<IDLE> in transaction', '<IDLE> in transaction (aborted)' )
  {% else -%}
    and state <> 'idle'
  {%- endif -%}
    and now() - query_start > '{{sec}} sec'
'''


LOCK_SQL = '''
select
      wait.pid,
      wait.state,
      (select string_agg(distinct transactionid::text, ',') from pg_locks where pid = wait.pid and locktype = 'transactionid' and transactionid::text <> wait.transactionid::text),
      wait.virtualxid,
      wait.locktype,
      wait.usename,
      wait.application_name,
      wait.client_addr,
      wait.waiting,
      wait.wait_event_type,
      wait.wait_event,
      wait.query_start,
      extract(epoch from now() - wait.query_start) as wait_query_run_time,
      wait.query,

      granted.pid              as waitfor_pid,
      granted.state            as waitfor_state,
      granted.transactionid    as waitfor_transactionid,
      granted.virtualxid       as waitfor_virtualxid,
      granted.locktype         as waitfor_locktype,
      granted.usename          as waitfor_usename,
      granted.client_addr      as waitfor_client_addr,
      granted.application_name as waitfor_application_name,
      granted.waiting          as waitfor_waiting,
      granted.wait_event_type  as waitfor_wait_event_type,
      granted.wait_event       as waitfor_wait_event,
      granted.query_start      as waitfor_query_start,
      extract(epoch from now() - granted.query_start) as waitfor_query_run_time,
      granted.query            as waitfor_query
from
    (select
       {% if ver <= (9, 1) -%}
          a.procpid as pid,
          'NULL' as state,
       {% else -%}
          a.pid,
          a.state,
       {% endif -%}
          b.transactionid,
          b.virtualxid,
          b.locktype,
          b.relation,
          b.page,
          b.tuple,
          a.usename,
          a.application_name,
          a.client_addr,
       {% if ver <= (9, 5)-%}
          a.waiting,
          'NULL' as wait_event_type,
          'NULl' as wait_event,
       {% else -%}
          'NULL' as waiting,
          a.wait_event_type,
          a.wait_event,
       {% endif -%}
          a.query_start,
       {% if ver <= (9,1) -%}
          a.current_query as query
       {% else -%}
          a.query
       {% endif -%}
     from
          pg_stat_activity a,
          pg_locks b
     where
       {% if ver <= (9,1) -%}
          a.procpid = b.pid
       {% else -%}
          a.pid = b.pid
       {% endif-%}
          and granted = 'f'
       {% if ver <= (9,5) -%}
          and a.waiting = 't'
       {% else -%}
          and a.wait_event_type is not null
       {% endif -%}
    ) wait
join
    (select
       {% if ver <= (9, 1) -%}
          b.procpid as pid,
          'NULL' as state,
       {% else -%}
          a.pid,
          b.state,
       {% endif -%}
          a.transactionid,
          a.virtualxid,
          a.locktype,
          a.relation,
          a.page,
          a.tuple,
          b.usename,
          b.application_name,
          b.client_addr,
       {% if ver <= (9, 5)-%}
          b.waiting,
          'NULL' as wait_event_type,
          'NULl' as wait_event,
       {% else -%}
          'NULL' as waiting,
          b.wait_event_type,
          b.wait_event,
       {% endif -%}
          b.query_start,
       {% if ver <= (9,1) -%}
          b.current_query as query
       {% else -%}
          b.query
       {% endif -%}
    from
        pg_locks a,
        pg_stat_activity b
    where
       {% if ver <= (9, 1) -%}
        a.pid = b.procpid
       {% else -%}
        a.pid = b.pid
       {% endif -%}
        and a.granted = 't'
    ) granted
on (
    ( wait.locktype = 'transactionid'
    and granted.locktype = 'transactionid'
    and wait.transactionid = granted.transactionid )
    or
    ( wait.locktype = 'relation'
    and granted.locktype = 'relation'
    and wait.relation = granted.relation
    )
    or
    ( wait.locktype = 'virtualxid'
    and granted.locktype = 'virtualxid'
    and wait.virtualxid = granted.virtualxid )
    or
    ( wait.locktype = 'tuple'
    and granted.locktype = 'tuple'
    and wait.relation = granted.relation
    and wait.page = granted.page
    and wait.tuple = granted.tuple )
)
where  wait.query !~ '^autovacuum'
and granted.query !~ '^autovacuum'
and (wait.query ~* '^VACUUM' and wait.usename = 'pgdba') = false
order by
granted.query_start
'''


STREAMING_SQL = '''
SELECT
    application_name,
    {% if ver < (10, 0) -%}
    pg_xlog_location_diff(pg_current_xlog_location(), replay_location) as diff
    {% else -%}
    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as diff
    {% endif -%}
FROM
    pg_stat_replication
'''


STREAMING_SLAVE_SQL = '''
SELECT
    now() AS now,
    coalesce(pg_last_xact_replay_timestamp(), now()) replay,
    extract(EPOCH FROM (now() - coalesce(pg_last_xact_replay_timestamp(), now()))) AS diff,
(SELECT regexp_replace(a,
                  E'primary_conninfo.+(host.*=.*).*(port=\\\\d+).+',
                  E'\\\\1 \\\\2', 'ig') AS master
FROM
    regexp_split_to_table(pg_read_file('recovery.conf'), E'\\\\n') t(a)
WHERE
    a ~ '^ *primary_conninfo')
'''

def get_slow_query(db_version, secs):
    template = Template(SLOW_QUERY_SQL)
    sql = template.render(ver=db_version, sec=secs)
    return sql

def get_lock_query(db_version):
    template = Template(LOCK_SQL)
    sql = template.render(ver=db_version)
    return sql

def get_streaming_query(db_version, isMaster=True):
    if isMaster:
        template = Template(STREAMING_SQL)
        sql = template.render(ver=db_version)
        return sql
    else:
        return STREAMING_SLAVE_SQL
