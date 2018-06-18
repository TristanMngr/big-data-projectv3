import pymongo
import time

from pymongo import MongoClient
from datetime import datetime

client = MongoClient('localhost', 27017)
db = client.big_data_project

FIVE_MINUTE_TIMESTAMP = 60 * 5
ONE_WEEK_TIMESTAMP = 60 * 60 * 24 *7

def sum_load_curve_industry(choosen_time):
    start = time.time()
    industry = list(db.industries.aggregate([
        {'$unwind': '$ENERGIES'},
        {'$match': { 'ENERGIES.timestamp': { '$mod': [choosen_time, 0]} }},
        {'$project': {'_id': 0, 'total_energy': {'$sum': '$ENERGIES.value'}}},
        {'$group': {'_id': '', 'total': { '$sum': '$total_energy'}}}
    ]))
    end = time.time()
    print "[TIME] the query took " + str(end-start)[:5] + "s to execute"
    return industry[0]['total'] if len(industry) >= 1 else "no industry found"

def sum_load_curve_industryV2(choosen_time):
    start = time.time()
    query = db.industries.aggregate([
        {'$unwind': '$ENERGIES'},
        {'$match': { 'ENERGIES.timestamp': { '$mod': [choosen_time, 0]} }},
        {'$group': {'_id': '$ENERGIES.timestamp', 'total_energy': { '$sum': '$ENERGIES.value'}}},
        {'$sort': {'_id': 1}}]
    );
    end = time.time()
    print "[TIME] the query took " + str(end-start)[:5] + "s to execute"
    return list(query)

def avg_load_curve_industry(choosen_time):
    start = time.time()
    query = db.industries.aggregate([
        {'$unwind': '$ENERGIES'},
        {'$match': { 'ENERGIES.timestamp': { '$mod': [choosen_time, 0]} }},
        {'$group': {'_id': '$SUB_INDUSTRY', 'average': { '$avg': '$ENERGIES.value'}}}
    ]);
    end = time.time()
    print "[TIME] the query took " + str(end-start)[:5] + "s to execute"

    return list(query)

def print_avg(choosen_time):
    print "query started, wait..."
    list = avg_load_curve_industry(choosen_time)
    for item in list:
        print str("Energy used by " + item['_id']) + '(s) : ' + str(item['average'])

def print_sum(choosen_time):
    print "query started, wait..."
    print "result : " + str(sum_load_curve_industry(choosen_time))

def print_sum_v2(choosen_time):
    print "query started, wait..."
    list = sum_load_curve_industryV2(choosen_time)
    for item in list:
        print "DATETIME : " + datetime.fromtimestamp(item['_id']).strftime('%Y-%m-%d %H:%M:%S')
        print 'timestamp : ' + str(item['_id']) + ', total energy ' + str(item['total_energy'])

# SIMPLE QUERIES :

def count_energies_by_industry():
    print "query started, wait..."
    query = db.industries.aggregate([
        {'$project' : {"site_id" : "$SITE_ID", "count_energies" : {"$size" : '$ENERGIES'}}}
    ])
    array = list(query)
    for item in array:
        print "site with id " + str(item["site_id"]) + " has " + str(item["count_energies"]) + " energies"

def count_energies():
    print "query started, wait..."
    query = db.industries.aggregate([
        {'$project' : {"count" : {"$sum" : {"$size" : '$ENERGIES'}}}},
        {'$group' : {'_id': '', "count" : {"$sum" : '$count'}}}
    ])
    array = list(query)
    print array[0]["count"]

def industries_order_by_site_id_desc():
    print "query started, wait..."
    query = db.industries.aggregate([
        {'$project' : { "ENERGIES" : 0} },
        { '$sort' : {"SITE_ID" : -1}}
    ])
    array = list(query)
    for item in array:
        print item["SITE_ID"]

def get_all_commercial_properties():
    print "query started, wait..."
    query = db.industries.find({"INDUSTRY" : "Commercial Property"})
    array = list(query)
    for item in array:
        print str(item["SITE_ID"])

def count_energies_by_sub_industry():
    print "query started, wait..."
    query = db.industries.aggregate([
        {'$project' : {"SUB_INDUSTRY" : '$SUB_INDUSTRY', "count" : {"$sum" : {"$size" : '$ENERGIES'}}}},
        {'$group' : {'_id': '$SUB_INDUSTRY', "count" : {"$sum" : '$count'}}}
    ])
    array = list(query)
    for item in array:
        print item["_id"] + " has " + str(item["count"]) + " energies"


print '=====> SIMPLE QUERIES :'
print '\n'

print 'number of energies by industry'
print '_____'
count_energies_by_industry()
print '\n'

print 'total number of energies'
print '_____'
count_energies()
print '\n'


print 'get industries without energies, order by SITE_ID descendant (only the SITE_ID will be printed, but everything is retrieved)'
print '_____'
industries_order_by_site_id_desc()
print '\n'

print 'get all industries that are Commercial Properties (only the SITE_ID will be printed, but everything is retrieved, including energies)'
print '_____'
get_all_commercial_properties()
print '\n'

print 'get number of energies, group by sub industry'
print '_____'
count_energies_by_sub_industry()

print '\n'

print '=====> OTHER QUERIES : '
print '\n'

print 'Sum five minute timestamp'
print '_____'
print_sum(FIVE_MINUTE_TIMESTAMP)
print '_____'
print_sum_v2(FIVE_MINUTE_TIMESTAMP)
print '\n'

print 'Sum one week timestamp'
print '_____'
print_sum(ONE_WEEK_TIMESTAMP)
print '_____'
print_sum_v2(ONE_WEEK_TIMESTAMP)
print '\n'

print 'Average sub industry five minute timestamp'
print '_____'
print_avg(FIVE_MINUTE_TIMESTAMP)
print '\n'

print 'Average sub industry one week timestamp'
print '_____'
print_avg(ONE_WEEK_TIMESTAMP)
