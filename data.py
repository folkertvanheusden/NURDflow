#! /usr/bin/python3

import psycopg2

def get_db_parameters():
    return 'dbname=ipfixer user=ipfixer host=localhost password=ipfixer'

conn = psycopg2.connect(get_db_parameters())

# ---

def get_record(group_by, sum_):
    what = 'SUM(n_bytes)' if sum_ else 'COUNT(*) AS n'
    with conn.cursor() as cur:
        cur.execute('SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, ' + what + ' FROM records GROUP BY dt ORDER BY dt ASC')
        return cur.fetchall()

def get_unique_ip_count(group_by, sum_):
    assert sum_ == False
    with conn.cursor() as cur:
        cur.execute("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(source_address->'sourceIPv4Address')) AS IPv4, COUNT(DISTINCT(source_address->'sourceIPv6Address')) AS IPv6 FROM records GROUP BY dt ORDER BY dt ASC")
        return cur.fetchall()

def get_count_per(group_by, column, items, where, sum_bytes):
    values = tuple([v[0] for v in items])
    w = 'AND ' + where if where != None else ''
    if sum_bytes:
        qs = 'SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, SUM(n_bytes) AS n, ' + column + ' FROM records WHERE ' + column + ' IN %s GROUP BY dt, ' + column + ' UNION ALL SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, SUM(n_bytes) AS n, -1 AS ' + column + ' FROM records WHERE ' + column + ' NOT IN %s ' + w + ' GROUP BY dt ORDER BY dt ASC'
    else:
        qs = 'SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, COUNT(*) AS n, ' + column + ' FROM records WHERE ' + column + ' IN %s GROUP BY dt, ' + column + ' UNION ALL SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, COUNT(*) AS n, -1 AS ' + column + ' FROM records WHERE ' + column + ' NOT IN %s ' + w + ' GROUP BY dt ORDER BY dt ASC'
    with conn.cursor() as cur:
        cur.execute(qs, tuple([values, values]))
        return cur.fetchall()

def get_count_per_src_dst_address(group_by, sum_):
    assert sum_ == False
    with conn.cursor() as cur:
        cur.execute("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(concat(source_address->'sourceIPv4Address', destination_address->'destinationIPv4Address'))) AS n4, COUNT(DISTINCT(concat(source_address->'sourceIPv6Address', destination_address->'destinationIPv6Address'))) FROM records GROUP BY dt ORDER BY dt ASC")
        return cur.fetchall()

def get_count_per_src_dst_mac_address(group_by, sum_):
    assert sum_ == False
    with conn.cursor() as cur:
        cur.execute("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(miscellaneous->'sourceMacAddress', destination_address->'postDestinationMacAddress')) AS n FROM records GROUP BY dt ORDER BY dt ASC")
        return cur.fetchall()

def get_heatmap_data(gb_y, gb_x, sum_):
    with conn.cursor() as cur:
        if sum_:
            cur.execute("SELECT EXTRACT(" + gb_y + " FROM ts) AS dty, EXTRACT(" + gb_x + " FROM ts) AS dtx, SUM(n_bytes) AS n FROM records GROUP BY dty, dtx ORDER BY dty, dtx ASC")
        else:
            cur.execute("SELECT EXTRACT(" + gb_y + " FROM ts) AS dty, EXTRACT(" + gb_x + " FROM ts) AS dtx, COUNT(*) AS n FROM records GROUP BY dty, dtx ORDER BY dty, dtx ASC")
        return cur.fetchall()

def get_flow_duration_groups(n_values):
    with conn.cursor() as cur:
        cur.execute('SELECT MAX(flow_end_time - flow_start_time) AS max FROM records WHERE ip_protocol=6')
        row = cur.fetchone()
        divider = row[0] / n_values
        cur.execute(f'SELECT ROUND(AVG(flow_end_time - flow_start_time) / 1000) AS duration, COUNT(*) AS n FROM records WHERE ip_protocol=6 GROUP BY FLOOR((flow_end_time - flow_start_time) / {divider}) ORDER BY duration ASC')
        return cur.fetchall()
