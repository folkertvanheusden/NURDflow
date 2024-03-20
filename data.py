#! /usr/bin/python3

import asyncio
import asyncpg
import time


connection = None
async def init_db():
    global connection
    connection = await asyncpg.create_pool(database='ipfixer', user='ipfixer', password='ipfixer')

def get_db():
    global connection
    return connection

async def get_record(group_by, sum_):
    ts = time.time()
    pool = get_db()
    async with pool.acquire() as conn:
        t = time.time()
        what = 'SUM(n_bytes)' if sum_ else 'COUNT(*) AS n'
        values = await conn.fetch('SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, ' + what + ' FROM records GROUP BY dt ORDER BY dt ASC')
    print(time.time(), 'get_record', time.time() - t, t - ts)
    return values

async def get_unique_ip_count(group_by, sum_):
    assert sum_ == False
    ts = time.time()
    pool = get_db()
    async with pool.acquire() as conn:
        t = time.time()
        values = await conn.fetch("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(source_address->'sourceIPv4Address')) AS IPv4, COUNT(DISTINCT(source_address->'sourceIPv6Address')) AS IPv6 FROM records GROUP BY dt ORDER BY dt ASC")
    print(time.time(), 'get_unique_ip_count', time.time() - t, t - ts)
    return values

async def get_count_per(group_by, column, items, where, sum_bytes):
    values = tuple([v[0] for v in items])
    w = 'AND ' + where if where != None else ''
    ts = time.time()
    pool = get_db()
    async with pool.acquire() as conn:
        t = time.time()
        if sum_bytes:
            values = await conn.fetch('SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, SUM(n_bytes) AS n, ' + column + ' FROM records WHERE ' + column + ' = ANY($1) GROUP BY dt, ' + column + ' UNION ALL SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, SUM(n_bytes) AS n, -1 AS ' + column + ' FROM records WHERE NOT ' + column + '= ANY($2) ' + w + ' GROUP BY dt ORDER BY dt ASC', values, values)
        else:
            values = await conn.fetch('SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, COUNT(*) AS n, ' + column + ' FROM records WHERE ' + column + ' = ANY($1) GROUP BY dt, ' + column + ' UNION ALL SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, COUNT(*) AS n, -1 AS ' + column + ' FROM records WHERE NOT ' + column + ' = ANY($2) ' + w + ' GROUP BY dt ORDER BY dt ASC', values, values)
    print(time.time(), 'get_count_per', time.time() - t, t - ts)
    return values

async def get_count_per_src_dst_address(group_by, sum_):
    assert sum_ == False
    ts = time.time()
    pool = get_db()
    async with pool.acquire() as conn:
        t = time.time()
        values = await conn.fetch("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(concat(source_address->'sourceIPv4Address', destination_address->'destinationIPv4Address'))) AS n4, COUNT(DISTINCT(concat(source_address->'sourceIPv6Address', destination_address->'destinationIPv6Address'))) FROM records GROUP BY dt ORDER BY dt ASC")
    print(time.time(), 'get_count_per_src_dst_address', time.time() - t, t - ts)
    return values

async def get_count_per_src_dst_mac_address(group_by, sum_):
    assert sum_ == False
    ts = time.time()
    pool = get_db()
    async with pool.acquire() as conn:
        t = time.time()
        values = await conn.fetch("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(miscellaneous->'sourceMacAddress', destination_address->'postDestinationMacAddress')) AS n FROM records GROUP BY dt ORDER BY dt ASC")
    print(time.time(), 'get_count_per_src_dst_mac_address', time.time() - t, t - ts)
    return values

async def get_heatmap_data(gb_y, gb_x, sum_):
    ts = time.time()
    pool = get_db()
    async with pool.acquire() as conn:
        t = time.time()
        if sum_:
            values = await conn.fetch("SELECT EXTRACT(" + gb_y + " FROM ts) AS dty, EXTRACT(" + gb_x + " FROM ts) AS dtx, SUM(n_bytes) AS n FROM records GROUP BY dty, dtx ORDER BY dty, dtx ASC")
        else:
            values = await conn.fetch("SELECT EXTRACT(" + gb_y + " FROM ts) AS dty, EXTRACT(" + gb_x + " FROM ts) AS dtx, COUNT(*) AS n FROM records GROUP BY dty, dtx ORDER BY dty, dtx ASC")
    print(time.time(), 'get_heatmap_data', time.time() - t, t - ts)
    return values

async def get_flow_duration_groups(n_values):
    ts = time.time()
    pool = get_db()
    resolution = 1  # ms
    async with pool.acquire() as conn:
        t = time.time()
        max_value = await conn.fetchval('select percentile_cont(0.5) WITHIN GROUP (ORDER BY flow_end_time - flow_start_time) from records where ip_protocol=6')
        divider = max_value / n_values
        values = await conn.fetch(f'SELECT AVG(flow_end_time - flow_start_time) AS duration, COUNT(*) AS n FROM records WHERE ip_protocol=6 GROUP BY FLOOR((flow_end_time - flow_start_time) / {divider}) ORDER BY duration ASC')
        values = [v for v in values if v[0] <= max_value]
    print(time.time(), 'get_flow_duration_groups', time.time() - t, t - ts)
    return values
