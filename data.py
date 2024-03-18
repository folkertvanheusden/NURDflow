#! /usr/bin/python3

import psycopg2

def get_db_parameters():
    return 'dbname=ipfixer user=ipfixer host=localhost password=ipfixer'

# ---

def get_record_count_per_date():
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute('SELECT DATE(ts) AS d, COUNT(*) AS n FROM records GROUP BY d ORDER BY d ASC')
             return cur.fetchall()

def get_record_count_per_hour():
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute('SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(*) AS n FROM records GROUP BY h ORDER BY h ASC')
             return cur.fetchall()

def get_record_count_per_day_of_week():
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute('SELECT EXTRACT(ISODOW FROM ts) AS dow, COUNT(*) AS n FROM records GROUP BY dow ORDER BY dow ASC')
             return cur.fetchall()

def get_record_count_per_month_day():
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute('SELECT EXTRACT(DAY FROM ts) AS md, COUNT(*) AS n FROM records GROUP BY md ORDER BY md ASC')
             return cur.fetchall()

def get_record_count_per_month():
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute('SELECT EXTRACT(MONTH FROM ts) AS m, COUNT(*) AS n FROM records GROUP BY m ORDER BY m ASC')
             return cur.fetchall()

# ---

def get_unique_ip_count_per_hour():
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute("SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(DISTINCT(source_address->'sourceIPv4Address')) AS n4, COUNT(DISTINCT(source_address->'sourceIPv6Address')) AS n6 FROM records GROUP BY h ORDER BY h ASC")
             return cur.fetchall()

def get_unique_ip_count_per_day_of_week():
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute("SELECT EXTRACT(ISODOW FROM ts) AS dow, COUNT(DISTINCT(source_address->'sourceIPv4Address')) AS n4, COUNT(DISTINCT(source_address->'sourceIPv6Address')) AS n6 FROM records GROUP BY dow ORDER BY dow ASC")
             return cur.fetchall()

def get_unique_ip_count_per_month_day():
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute("SELECT EXTRACT(DAY FROM ts) AS md, COUNT(DISTINCT(source_address->'sourceIPv4Address')) AS n4, COUNT(DISTINCT(source_address->'sourceIPv6Address')) AS n6 FROM records GROUP BY md ORDER BY md ASC")
             return cur.fetchall()

def get_unique_ip_count_per_month():
    with psycopg2.connect(get_db_parameters()) as conn:
         with conn.cursor() as cur:
             cur.execute("SELECT EXTRACT(MONTH FROM ts) AS m, COUNT(DISTINCT(source_address->'sourceIPv4Address')) AS n4, COUNT(DISTINCT(source_address->'sourceIPv6Address')) AS n6 FROM records GROUP BY m ORDER BY m ASC")
             return cur.fetchall()

# ---

def get_count_per_port_per_hour(ports):
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(*) AS n, dst_port AS d FROM records WHERE dst_port IN %s GROUP BY h, dst_port ORDER BY h ASC', [ports])
            return cur.fetchall()

def get_count_per_port_per_day_of_week(ports):
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT EXTRACT(ISODOW FROM ts) AS dow, COUNT(*) AS n, dst_port AS d FROM records WHERE dst_port IN %s GROUP BY dow, dst_port ORDER BY dow ASC', [ports])
            return cur.fetchall()

def get_count_per_port_per_month_day(ports):
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT EXTRACT(DAY FROM ts) AS md, COUNT(*) AS n, dst_port AS d FROM records WHERE dst_port IN %s GROUP BY md, dst_port ORDER BY md ASC', [ports])
            return cur.fetchall()

def get_count_per_port_per_month(ports):
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT EXTRACT(MONTH FROM ts) AS m, COUNT(*) AS n, dst_port AS d FROM records WHERE dst_port IN %s GROUP BY m, dst_port ORDER BY m ASC', [ports])
            return cur.fetchall()

# ---

def get_count_per_src_dst_address_per_hour():
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(DISTINCT(concat(source_address->'sourceIPv4Address', destination_address->'destinationIPv4Address'))) AS n4, COUNT(DISTINCT(concat(source_address->'sourceIPv6Address', destination_address->'destinationIPv6Address'))) FROM records GROUP BY h ORDER BY h ASC")
            return cur.fetchall()

def get_count_per_src_dst_address_per_day_of_week():
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXTRACT(ISODOW FROM ts) AS dow, COUNT(DISTINCT(concat(source_address->'sourceIPv4Address', destination_address->'destinationIPv4Address'))) AS n4, COUNT(DISTINCT(concat(source_address->'sourceIPv6Address', destination_address->'destinationIPv6Address'))) FROM records GROUP BY dow ORDER BY dow ASC")
            return cur.fetchall()

def get_count_per_src_dst_address_per_month_day():
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXTRACT(DAY FROM ts) AS md, COUNT(DISTINCT(concat(source_address->'sourceIPv4Address', destination_address->'destinationIPv4Address'))) AS n4, COUNT(DISTINCT(concat(source_address->'sourceIPv6Address', destination_address->'destinationIPv6Address'))) FROM records GROUP BY md ORDER BY md ASC")
            return cur.fetchall()

def get_count_per_src_dst_address_per_month():
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXTRACT(MONTH FROM ts) AS m, COUNT(DISTINCT(concat(source_address->'sourceIPv4Address', destination_address->'destinationIPv4Address'))) AS n4, COUNT(DISTINCT(concat(source_address->'sourceIPv6Address', destination_address->'destinationIPv6Address'))) FROM records GROUP BY m ORDER BY m ASC")
            return cur.fetchall()

# ---

def get_count_per_src_dst_mac_address_per_hour():
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(DISTINCT(miscellaneous->'sourceMacAddress', destination_address->'postDestinationMacAddress')) AS n FROM records GROUP BY h ORDER BY h ASC")
            return cur.fetchall()

def get_count_per_src_dst_mac_address_per_day_of_week():
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXTRACT(ISODOW FROM ts) AS dow, COUNT(DISTINCT(miscellaneous->'sourceMacAddress', destination_address->'postDestinationMacAddress')) AS n FROM records GROUP BY dow ORDER BY dow ASC")
            return cur.fetchall()

def get_count_per_src_dst_mac_address_per_month_day():
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXTRACT(DAY FROM ts) AS md, COUNT(DISTINCT(miscellaneous->'sourceMacAddress', destination_address->'postDestinationMacAddress')) AS n FROM records GROUP BY md ORDER BY md ASC")
            return cur.fetchall()

def get_count_per_src_dst_mac_address_per_month():
    with psycopg2.connect(get_db_parameters()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXTRACT(MONTH FROM ts) AS m, COUNT(DISTINCT(miscellaneous->'sourceMacAddress', destination_address->'postDestinationMacAddress')) AS n FROM records GROUP BY m ORDER BY m ASC")
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
