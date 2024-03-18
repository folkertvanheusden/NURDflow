#! /usr/bin/python3

import psycopg2

def get_db_parameters():
    return 'dbname=ipfixer user=ipfixer host=localhost password=ipfixer'

conn = psycopg2.connect(get_db_parameters())

# ---

def get_record_count(group_by):
    with conn.cursor() as cur:
        cur.execute('SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, COUNT(*) AS n FROM records GROUP BY dt ORDER BY dt ASC')
        return cur.fetchall()

def get_unique_ip_count(group_by):
    with conn.cursor() as cur:
        cur.execute("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(source_address->'sourceIPv4Address')) AS IPv4, COUNT(DISTINCT(source_address->'sourceIPv6Address')) AS IPv6 FROM records GROUP BY dt ORDER BY dt ASC")
        return cur.fetchall()

def get_count_per(group_by, column, items, where):
    values = tuple([v[0] for v in items])
    w = 'AND ' + where if where != None else ''
    qs = 'SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, COUNT(*) AS n, ' + column + ' FROM records WHERE ' + column + ' IN %s GROUP BY dt, ' + column + ' UNION ALL SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, COUNT(*) AS n, -1 AS ' + column + ' FROM records WHERE ' + column + ' NOT IN %s ' + w + ' GROUP BY dt ORDER BY dt ASC'
    with conn.cursor() as cur:
        cur.execute(qs, tuple([values, values]))
        return cur.fetchall()

def get_count_per_src_dst_address(group_by):
    with conn.cursor() as cur:
        cur.execute("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(concat(source_address->'sourceIPv4Address', destination_address->'destinationIPv4Address'))) AS n4, COUNT(DISTINCT(concat(source_address->'sourceIPv6Address', destination_address->'destinationIPv6Address'))) FROM records GROUP BY dt ORDER BY dt ASC")
        return cur.fetchall()

def get_count_per_src_dst_mac_address(group_by):
    with conn.cursor() as cur:
        cur.execute("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(miscellaneous->'sourceMacAddress', destination_address->'postDestinationMacAddress')) AS n FROM records GROUP BY dt ORDER BY dt ASC")
        return cur.fetchall()
