import connection
from bson.json_util import dumps as mongo_dumps
import simplejson as json
import datetime
import pandas as pd


truck_state_collection = connection.connect_mongo_truck_state()


#truck_state_collection


json.loads(mongo_dumps(truck_state_collection.find()))


c = 0
for state in truck_state_collection.find():
    print(c)
    c += 1


start_date = datetime.datetime(2017, 7, 25, 0, 0, 0, 0)
end_date = datetime.datetime(2017, 7, 25, 23, 59, 59, 0)

truck_array = []
for truck_dict in truck_state_collection.find({'state_begin': {'$gte': start_date, '$lt': end_date}}):
    truck_array += [truck_dict]

truck_df = pd.DataFrame(truck_array)




















