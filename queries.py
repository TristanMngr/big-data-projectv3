import pymongo
import time

from pymongo import MongoClient

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




print 'Sum five minute timestamp'
print '_____'
print_sum(FIVE_MINUTE_TIMESTAMP)
print '\n'

print 'Sum one week timestamp'
print '_____'
print_sum(ONE_WEEK_TIMESTAMP)
print '\n'

print 'Average sub industry five minute timestamp'
print '_____'
print_avg(FIVE_MINUTE_TIMESTAMP)
print '\n'

print 'Average sub industry one week timestamp'
print '_____'
print_avg(ONE_WEEK_TIMESTAMP)
