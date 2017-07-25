import requests
import pandas as pd
import numpy as np
import datetime
import connection

def fetch_bin_data(bin_type):
    BIN_TYPE_LIST = ['all', 'metal', 'plastic', 'auto']
    
    if bin_type == 'all':
        url_insert_type = ''
    elif bin_type in BIN_TYPE_LIST:
        url_insert_type = 'type={}&'.format(bin_type)
    else:
        return 'Incorrect bin_type!'

    url = 'https://api.traffy.xyz/v0/trash/?{}limit=5000'.format(url_insert_type)
    
    response = requests.get(url).json()
    return response['results']

def extract_bin_info(bins_dict):
    ids = []
    lat = []
    lon = []
    name = []
    bin_type = []
    for bin in bins_dict:
        ids += [bin['id']]
        lat += [float(bin['coords'][1])]
        lon += [float(bin['coords'][0])]
        name += [bin['name']]
        bin_type += [bin['type']]
    
    bins = pd.DataFrame({'id': ids, 'lat': lat, 'lon': lon, 'name': name, 
                         'type': bin_type})
    
    return bins

def fetch_extract_bin_data(bin_type):
    """ bin_type = ['all', 'plastic', 'metal', 'auto'] """
    
    BIN_TYPE_LIST = ['all', 'metal', 'plastic', 'auto']
    if bin_type not in BIN_TYPE_LIST:
        return 'Incorrect bin type'
    bins_dict = fetch_bin_data(bin_type)
    return extract_bin_info(bins_dict)

def save_bin(bin_type):
    bin_data = fetch_extract_bin_data(bin_type)
    bin_data.to_csv('data/{}_bin.csv'.format(bin_type), index=False)

def save_all_bins():
    bin_data = fetch_extract_bin_data('all')
    bin_data.to_csv('data/all_bin.csv', index=False)

#save_all_bins()
#save_bin('auto')
#bin_data = fetch_extract_bin_data('all')
#
#save_bin('metal')
#save_bin('plastic')

GARBAGE_TRUCK_PATH = 'data/garbage_truck/'

def get_all_garbage_truck_vid():
    url = 'https://api.traffy.xyz/vehicle/?line=phuket-garbage-truck'
    response = requests.get(url).json()
    garbage_trucks = response['results']
    garbage_truck_ids = []
    
    for truck in garbage_trucks:
        garbage_truck_ids += [truck['vehicle_id']]
        
    return garbage_truck_ids

#get_all_garbage_truck_vid()

## Query Truck Data from MongoDB
def fetch_garbage_truck_data_to_csv(query_vid, start, end):
#    start = datetime.datetime(2017, 7, 18, 0, 0, 0, 0)
#    end = datetime.datetime(2017, 7, 24, 23, 0, 0, 0)
#    query_vid = '359486060261458'
    
    import pprint
    vehicle_log = connection.connect_mongo()
    print('mongoDB connected')
    
    vid = []
    timestamp = []
    lat = []
    lon = []
    speed = []
    
    for v in vehicle_log.find({'vehicle_id': query_vid, 'gps_timestamp': {'$gte': start, '$lt': end}}):
        pprint.pprint(v['gps_timestamp'])
        vid += [v['vehicle_id']]
        timestamp += [v['log_timestamp']]
        lat += [v['latitude']]
        lon += [v['longitude']]
        speed += [v['speed']]
        
    truck_info = pd.DataFrame({'vid': vid, 'timestamp':timestamp, 'lat':lat, 
                               'lon':lon, 'speed': speed})
    
    truck_info.to_csv(GARBAGE_TRUCK_PATH + query_vid + '.csv', index=False)
    print('Garbage Truck Data saved!')
    
    return truck_info

#truck = fetch_garbage_truck_data()
#truck.to_csv('data/truck_7days.csv', index=False)

def fetch_all_garbage_truck_data_to_csv(start_date, end_date):
    
    truck_vids = get_all_garbage_truck_vid()
    
    for vid in truck_vids:
        fetch_garbage_truck_data_to_csv(vid, start_date, end_date)

#['359486060261458',
#'359486060261466',
# '359486060261516',
# '359486060261649',
# '359486060261672',
# '359486060261680',
# '359486060261706',
# '359486060261722',
# '359486060261730',
# '359486060261789',
# '359486060261805',
# '359486060261813',
# '359486060261821',
# '359486060261839',
# '359486060261847']

#start_date = datetime.datetime(2017, 7, 18, 0, 0, 0, 0)
#end_date = datetime.datetime(2017, 7, 24, 23, 0, 0, 0)
#fetch_all_garbage_truck_data_to_csv(start_date, end_date)


## Query actual Bin Answers, surveying manually from Phuket
def fetch_bin_ans():
    url = 'http://api2.traffy.in.th/api/smartphuket/garbage/get_auto_bin_ans'
    return requests.get(url).json()

def fetch_extract_bin_ans():
    response = fetch_bin_ans()
    
    ids = []
    lat = []
    lon = []
    name = []
    bin_type = []
    verified = []
    is_correct = []
    for bin in response:
        ids += [bin['bin_id']]
        lat += [float(bin['coords'][1])]
        lon += [float(bin['coords'][0])]
        name += [bin['name_th']]
        bin_type += [bin['type']]
        verified += [bin['verified']]
        is_correct += [bin['is_correct'] if(bin['verified']) else 'undefined']
    
    bins = pd.DataFrame({'id': ids, 'lat': lat, 'lon': lon, 'name': name, 
                         'type': bin_type, 'verified': verified,
                         'is_correct': is_correct})

    return bins

def save_bin_ans():
    verified_bin = fetch_extract_bin_ans()
    
#    verified_bin = verified_bin[verified_bin['is_correct'] == True]
    verified_bin.to_csv('data/bin_ans.csv', index=False)

#save_bin_ans()



























