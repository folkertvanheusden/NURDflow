#! /usr/bin/python3

import asyncio
import configparser
from motor.motor_asyncio import AsyncIOMotorClient
import time

collection = None
database = None
db_url = None

def get_collection():
    return collection

async def init_db():
    global collection
    global database
    global db_url
    config = configparser.ConfigParser()
    config.read('nurdflow.ini')
    db_url = config['database']['url']
    database = config['database']['database']
    collection = config['database']['collection']

def get_db():
    return AsyncIOMotorClient(db_url)[database]

async def get_field_grouped(field):
    ts = time.time()
    db = get_db()
    t = time.time()
    values = await db[collection].aggregate([
          { '$match': { field: { '$exists': True } } },
          {
            '$group': {
                '_id': { 'grouped_time': { '$dateToString': { 'format': '%Y-%m-%dT%H:%M', 'date': '$export_time' } } },
                'ts': { '$first': '$export_time' },
                'sum': { '$sum': '$' + field },
                'count': { '$sum': 1 },
              }
          },
          { '$sort': { '_id': 1 } }
        ]).to_list(None)
    print(time.time(), 'get_record', time.time() - t, t - ts)
    return values


async def get_record(group_by):
    ts = time.time()
    db = get_db()
    t = time.time()
    if group_by in ('isoDayOfWeek'):
        values = await db[collection].aggregate([
                {
                    '$group': {
                      '_id': { '$' + group_by: '$export_time' },
                      'sum': { '$sum': '$data.octetDeltaCount' },
                      'count': { '$sum': 1 },
                    },
                },
                { '$sort': { '_id': 1 } }
            ]
            ).to_list(None)
    else:
        values = await db[collection].aggregate([
              {
                '$group': {
                  '_id': { '$dateTrunc': { 'date': '$export_time', 'unit': group_by } },
                  'sum': { '$sum': '$data.octetDeltaCount' },
                  'count': { '$sum': 1 },
                },
              },
              { '$sort': { '_id': 1 } }
            ]).to_list(None)
    print(values)
    print(time.time(), 'get_record', time.time() - t, t - ts)
    return values

async def get_unique_address_count(field, group_by):
    ts = time.time()
    db = get_db()
    t = time.time()
    values = []
    if group_by in ('dayOfWeek'):
        n = await db[collection].aggregate([
	            { '$match': { field: { '$exists': True } } },
                { '$group': { '_id' : { 'ts_component': {'$' + group_by: '$export_time'}, 'address': '$' + field } } },
                { '$group': { '_id' : '$_id.isodow', 'count' : { '$sum' : 1 } }
  	            }
	       ]).to_list(None)
    else:
        n = await db[collection].aggregate([
	            { '$match': { field: { '$exists': True } } },
                { '$group': { '_id' : { 'ts_component': { '$dateTrunc': { 'date': '$export_time', 'unit': group_by } }, 'address': '$' + field } } },
                { '$group': { '_id' : '$_id.ts_component', 'count' : { '$sum' : 1 } }
  	            }
	       ]).to_list(None)
    for record in n:
        values.append((record['_id'], record['count']))
    print(time.time(), 'get_unique_ip_address_count', time.time() - t, t - ts)
    return values

async def get_COUNT_PER(field, field_values, group_by):
    ts = time.time()
    db = get_db()
    t = time.time()
    field_compare = { '$' + group_by: '$export_time' } if group_by in ('isoDayOfWeek') else { '$' + group_by: '$export_time' }
    field_values = [v[0] for v in field_values]

    cursor1 = db[collection].aggregate([
            {
                '$match': { field: { '$exists': True } },
                '$match': { field: { '$in': field_values } },
            },
            {
                '$group': {
                    '_id': { 'ts_component' : field_compare, 'field': '$' + field },
                    'sum': { '$sum': '$data.octetDeltaCount' },
                    'count': { '$sum': 1 },
                },
            },
            { '$sort': { '_id.ts_component':1, field: 1 } },
        ])

    cursor2 = db[collection].aggregate([
            {
                '$match': { field: { '$exists': True } },
                '$match': { field: { '$nin': field_values } },
            },
            {
                '$group': {
                    '_id': { 'ts_component' : field_compare },
                    'sum': { '$sum': '$data.octetDeltaCount' },
                    'count': { '$sum': 1 },
                },
            },
            { '$sort': { '_id.ts_component':1 } },
        ])

    values = await cursor1.to_list(None)

    for record in await cursor2.to_list(None):
        record['_id']['field'] = 'other'
        values.append(record)

    print(time.time(), 'get_unique_ip_count', time.time() - t, t - ts)
    return values

async def get_heatmap(group_by_1, group_by_2):
    ts = time.time()
    db = get_db()
    t = time.time()
    field_1_compare = { '$' + group_by_1: '$export_time' } if group_by_1 in ('isoDayOfWeek') else { '$' + group_by_1: '$export_time' }
    field_2_compare = { '$' + group_by_2: '$export_time' } if group_by_2 in ('isoDayOfWeek') else { '$' + group_by_2: '$export_time' }
    cursor = db[collection].aggregate([
            {
                '$group': {
                    '_id': { 'field_1': field_1_compare, 'field_2': field_2_compare },
                    'sum': { '$sum': '$data.octetDeltaCount' },
                    'count': { '$sum': 1 },
                },
            },
            { '$sort': { 'sum': -1, 'count': -1 } },
        ])
    values = await cursor.to_list(None)
    print(time.time(), 'get_unique_ip_count', time.time() - t, t - ts)
    return values

if __name__ == '__main__':
    asyncio.run(init_db())
    asyncio.run(get_field_grouped('data.octetDeltaCount'))
    #
    asyncio.run(get_record('hour'))
    asyncio.run(get_unique_address_count('data.sourceIPv4Address', 'hour'))
    asyncio.run(get_COUNT_PER('data.destinationTransportPort', [ 22, 80, 123, 443 ], 'hour'))
    asyncio.run(get_heatmap('hour', 'isoDayOfWeek'))
    print()
    asyncio.run(get_COUNT_PER('data.destinationTransportPort', [ 22, 80 ], 'isoDayOfWeek'))
