import connection
import datetime
import arrow
from flask import jsonify
from bson.json_util import dumps as mongo_dumps
import simplejson as json



def get_bin_route(vid, y, m, d):
    bin_route_connection = connection.connect_mongo_garbage_bin_route()
    
    query_date = arrow.get(datetime.datetime(y,m,d), 'Asia/Bangkok').datetime
    start = query_date
    end = query_date + datetime.timedelta(days=1)
    
#    bin_routes = []
#    for bin in bin_route_connection.find({'vehicle_id': vid, 
#                                          'route_start': {'$gte': start, '$lt': end}}):
#        bin_routes += [bin]
##        print(bin['vehicle_id'], bin['route_start'])
#    return bin_routes
    return json.loads(mongo_dumps(bin_route_connection.find(
            {'vehicle_id': vid, 'route_start': {'$gte': start, '$lt': end}})))


def get_lastest_bin_route(vid):
    bin_route_connection = connection.connect_mongo_garbage_bin_route()
    
#    for bin in bin_route_connection.find({'vehicle_id': vid}).sort([('route_start', -1)]).limit(1):
##        print(bin['vehicle_id'], bin['route_start'])
#        bin_route = bin
#    return bin_route

    return json.loads(mongo_dumps(bin_route_connection.find(
            {'vehicle_id': vid}).sort([('route_start', -1)]).limit(1)))


@app.route('/get_bin_route/<vid>/<y>/<m>/<d>/', methods=['GET'])
def get_bin_route_json(vid, y, m, d):
    return jsonify(get_bin_route(vid, int(y), int(m), int(d)) )

@app.route('/get_lastest_bin_route/<vid>/', methods=['GET'])
def get_lastest_bin_route_json(vid):
    return jsonify(get_bin_route(vid))


#b = get_bin_route(359486060261458, 2017, 7, 24)
get_lastest_bin_route('359486060261458')





