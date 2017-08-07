import pandas as pd
import numpy as np
GARBAGE_DATA_PATH = 'data/garbage_truck/'
import connection
import requests
import arrow


truck_state_adapter = connection.connect_mongo_truck_state()

truck_states = []
for state in truck_state_adapter.find({'vid': '359486060261821', 'state_begin': {'$gte': arrow.get('2017-08-06').datetime}} ):
    truck_states += [state]
    
    
truck_states_pd = pd.DataFrame(truck_states)

#truck_state_adapter.find_one({'state_begin': {'$gte': arrow.get('2017-08-06').datetime}})



#def extract_bin_info(bins_dict):
#    ids = []
#    lat = []
#    lon = []
#    name = []
#    bin_type = []
#    for bin in bins_dict:
##        print(bin['id'])
#        ids += [bin['id']]
#        lat += [float(bin['coords'][1])]
#        lon += [float(bin['coords'][0])]
#        name += [bin['name']]
#        bin_type += [bin['type']]
#    
#    bins = pd.DataFrame({'id': ids, 'lat': lat, 'lon': lon, 'name': name, 
#                         'type': bin_type})
#    
#    return bins
#
#rep_auto_bin_adapter = connection.connect_mongo_representative_auto_detected_bin()
#
#auto_bins = []
#for bin in rep_auto_bin_adapter.find():
#    auto_bins += [bin]
#
#auto_bins = pd.DataFrame(auto_bins)
#
#
#res = requests.get('https://api.traffy.xyz/v0/trash/?type=auto&limit=5000').json()['results']
#
#auto_bins_api = extract_bin_info(res)








#def fetch_bin_data():
#    auto_bin = pd.read_csv('data/representative_bin.csv')
#    plastic_bin = pd.read_csv('data/plastic_bin.csv')
#    metal_bin = pd.read_csv('data/metal_bin.csv')
#    
#    all_bin = pd.concat([auto_bin, plastic_bin, metal_bin]).reset_index()
#    
#    return all_bin

#def fetch_truck_data(vid):
#    truck = pd.read_csv(GARBAGE_DATA_PATH + vid + '.csv')
#    truck = truck.iloc[::4, :]
#    return truck
#
#print('loading data')
#all_bin = fetch_bin_data()
#vid = '359486060261458'
#truck = fetch_truck_data(vid)
#print('data loaded')
#
##result = []
##for index, bin in all_bin.iterrows():
##    
##    lat1, lon1 = np.array([bin['lat']] * truck.shape[0]), np.array([bin['lon']] * truck.shape[0])
##    lat2, lon2 = truck['lat'], truck['lon']
##    
##    p = 0.017453292519943295
##    a = 0.5 - np.cos((lat2-lat1)*p)/2 + np.cos(lat1*p)*np.cos(lat2*p) * (1-np.cos((lon2-lon1)*p)) / 2
##    
##    result += [12742 * np.arcsin(np.sqrt(a))]
#
#
##result = []
#
#lat1, lon1 = np.array([all_bin['lat']] * truck.shape[0]), np.array([all_bin['lon']] * truck.shape[0])
#lat2, lon2 = truck['lat'] * all_bin.shape[0], truck['lon']
#
#p = 0.017453292519943295
#a = 0.5 - np.cos((lat2-lat1)*p)/2 + np.cos(lat1*p)*np.cos(lat2*p) * (1-np.cos((lon2-lon1)*p)) / 2
#    
#result2 = 12742 * np.arcsin(np.sqrt(a))
#    
#result2 = np.array(result2)
#
#min_distances = result2.min(axis=0)
#closest_bin = result2.argmin(axis=0)
    
