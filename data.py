#! /usr/bin/python3

import psycopg2

def get_db_parameters():
    return 'dbname=ipfixer user=ipfixer host=localhost password=ipfixer'

# ---

def get_record_count(group_by):
    print(group_by)
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute('SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, COUNT(*) AS n FROM records GROUP BY dt ORDER BY dt ASC')
             return cur.fetchall()

def get_unique_ip_count(group_by):
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(source_address->'sourceIPv4Address')) AS IPv4, COUNT(DISTINCT(source_address->'sourceIPv6Address')) AS IPv6 FROM records GROUP BY dt ORDER BY dt ASC")
             return cur.fetchall()

def get_count_per(group_by, column, items, where):
    values = tuple([v[0] for v in items])
    w = 'AND ' + where if where != None else ''
    qs = 'SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, COUNT(*) AS n, ' + column + ' FROM records WHERE ' + column + ' IN %s GROUP BY dt, ' + column + ' UNION ALL SELECT EXTRACT(' + group_by + ' FROM ts) AS dt, COUNT(*) AS n, -1 AS ' + column + ' FROM records WHERE ' + column + ' NOT IN %s ' + w + ' GROUP BY dt ORDER BY dt ASC'
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute(qs, tuple([values, values]))
            return cur.fetchall()

def get_count_per_src_dst_address(group_by):
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(concat(source_address->'sourceIPv4Address', destination_address->'destinationIPv4Address'))) AS n4, COUNT(DISTINCT(concat(source_address->'sourceIPv6Address', destination_address->'destinationIPv6Address'))) FROM records GROUP BY dt ORDER BY dt ASC")
            return cur.fetchall()

def get_count_per_src_dst_mac_address(group_by):
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXTRACT(" + group_by + " FROM ts) AS dt, COUNT(DISTINCT(miscellaneous->'sourceMacAddress', destination_address->'postDestinationMacAddress')) AS n FROM records GROUP BY dt ORDER BY dt ASC")
            return cur.fetchall()

# ---

def get_ip_protocol_count_per_hour(protocols):
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute('''SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(*) AS n, ip_protocol FROM records WHERE ip_protocol IN %s GROUP BY h, ip_protocol UNION ALL
             SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(*) AS n, -1 AS ip_protocol FROM records WHERE ip_protocol NOT IN %s GROUP BY h ORDER BY h ASC''', [protocols, protocols])
             return cur.fetchall()

def get_ip_protocol_count_per_day_of_week(protocols):
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute('''SELECT EXTRACT(ISODOW FROM ts) AS dow, COUNT(*) AS n, ip_protocol FROM records WHERE ip_protocol IN %s GROUP BY dow, ip_protocol UNION ALL
             SELECT EXTRACT(ISODOW FROM ts) AS dow, COUNT(*) AS n, -1 AS ip_protocol FROM records WHERE ip_protocol NOT IN %s GROUP BY dow ORDER BY dow ASC''', [protocols, protocols])
             return cur.fetchall()

def get_ip_protocol_count_per_month_day(protocols):
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute('''SELECT EXTRACT(DAY FROM ts) AS d, COUNT(*) AS n, ip_protocol FROM records WHERE ip_protocol IN %s GROUP BY d, ip_protocol UNION ALL
             SELECT EXTRACT(DAY FROM ts) AS d, COUNT(*) AS n, -1 AS ip_protocol FROM records WHERE ip_protocol NOT IN %s GROUP BY d ORDER BY d ASC''', [protocols, protocols])
             return cur.fetchall()

def get_ip_protocol_count_per_month(protocols):
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute('''SELECT EXTRACT(MONTH FROM ts) AS m, COUNT(*) AS n, ip_protocol FROM records WHERE ip_protocol IN %s GROUP BY m, ip_protocol UNION ALL
             SELECT EXTRACT(MONTH FROM ts) AS m, COUNT(*) AS n, -1 AS ip_protocol FROM records WHERE ip_protocol NOT IN %s GROUP BY m ORDER BY m ASC''', [protocols, protocols])
             return cur.fetchall()

if __name__ == "__main__":
    print(get_record_count_per_date())
    print(get_record_count_per_hour())
    print(get_record_count_per_day_of_week())
    print(get_record_count_per_month())

    print(get_unique_ip_count_per_hour())
    print(get_unique_ip_count_per_day_of_week())
    print(get_unique_ip_count_per_month())

    print(get_count_per_port_per_hour((80, 443, 5900)))
    print(get_count_per_port_per_day_of_week((80, 443, 5900)))
    print(get_count_per_port_per_month_day((80, 443, 5900)))
    print(get_count_per_port_per_month((80, 443, 5900)))

    print(get_count_per_src_dst_address_per_hour())
    print(get_count_per_src_dst_address_per_day_of_week())
    print(get_count_per_src_dst_address_per_month_day())
    print(get_count_per_src_dst_address_per_month())

    print(get_count_per_src_dst_mac_address_per_hour())
    print(get_count_per_src_dst_mac_address_per_day_of_week())
    print(get_count_per_src_dst_mac_address_per_month_day())
    print(get_count_per_src_dst_mac_address_per_month())

    print(get_ip_protocol_count_per_hour((1, 6, 17)))
    print(get_ip_protocol_count_per_day_of_week((1, 6, 17)))
    print(get_ip_protocol_count_per_month_day((1, 6, 17)))
    print(get_ip_protocol_count_per_month((1, 6, 17)))
