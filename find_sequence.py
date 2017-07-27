import pandas as pd
import numpy as np
import datetime
from geopy.distance import vincenty

#import matplotlib.pyplot as plt
#from mpl_toolkits.basemap import Basemap

GARBAGE_DATA_PATH = 'data/garbage_truck/'

## Dumpsite and Terminal lat,long
DUMPSITE_RADIUS=500 # metre
DUMPSITE_LAT=7.863259
DUMPSITE_LON=98.395629
DUMPSITE_LOCATION = (DUMPSITE_LAT, DUMPSITE_LON)
TERM_RADIUS=300 # metre
TERM1_LAT=7.887007
TERM1_LON=98.2964797
TERM1_LOCATION = (TERM1_LAT, TERM1_LAT)
TERM2_LAT=7.895780
TERM2_LON=98.336451
TERM2_LOCATION = (TERM2_LAT, TERM2_LAT)



def fetch_bin_data():
    auto_bin = pd.read_csv('data/representative_bin.csv')
    plastic_bin = pd.read_csv('data/plastic_bin.csv')
    metal_bin = pd.read_csv('data/metal_bin.csv')
    
    all_bin = pd.concat([auto_bin, plastic_bin, metal_bin]).reset_index()
    
    return all_bin

def fetch_truck_data(vid):
    truck = pd.read_csv(GARBAGE_DATA_PATH + vid + '.csv')
    truck = truck.iloc[::3, :]
    truck = truck[truck['speed'] < 1]
    return truck


def find_parking_points(all_bin, truck):
    
    def find_closest_bin_and_distance(all_bin, truck):
        result = []
        for index, bin in all_bin.iterrows():
            
            lat1, lon1 = np.array([bin['lat']] * truck.shape[0]), np.array([bin['lon']] * truck.shape[0])
            lat2, lon2 = truck['lat'], truck['lon']
            
            p = 0.017453292519943295
            a = 0.5 - np.cos((lat2-lat1)*p)/2 + np.cos(lat1*p)*np.cos(lat2*p) * (1-np.cos((lon2-lon1)*p)) / 2
            
            result += [12742 * np.arcsin(np.sqrt(a))]
            
        result = np.array(result)
        
        min_distances = result.min(axis=0)
        closest_bin = result.argmin(axis=0)
        
        return closest_bin, min_distances
    
    closest_bin, min_distances = find_closest_bin_and_distance(all_bin, truck)
    
    get_bin_id = np.vectorize(lambda i: all_bin.loc[i, 'id'])
    
    truck['distance_to_closest_bin'] = min_distances
    truck['closest_bin_id'] = get_bin_id(closest_bin)
    
    #pick_up_garbage = truck[truck['distance_to_closest_bin'] < 0.050]
    #pick_up_garbage['closest_bin_id'].unique()
    
    
    
    
    def find_stage_picking_id(truck):
#        def classify_stage(point):
#            truck_lat_lon = (point['lat'], point['lon'])
#            if vincenty(DUMPSITE_LOCATION, truck_lat_lon).meters < DUMPSITE_RADIUS:
#                return 'dumpsite'
#            elif vincenty(TERM1_LOCATION, truck_lat_lon).meters < TERM_RADIUS:
#                return 'terminal1'
#            elif vincenty(TERM2_LOCATION, truck_lat_lon).meters < TERM_RADIUS:
#                return 'terminal2'
#            elif point['speed'] < 0.8:
#                return 'park'
#            elif point['speed'] < 7:
#                return 'slow'
#            else:
#                return 'drive'
#    
#        truck['stage'] = truck.apply(classify_stage, axis=1)
        
#        stage_id = []
#        stage_counter = 0
#        last_stage = ''
        picking_id = []
        picking_counter = 0
        last_bin_id = ''
        for i, point in truck.iterrows():
#            if(last_stage != point['stage']):
#                last_stage = point['stage']
#                stage_counter += 1
#            stage_id += [stage_counter]
            
            if(last_bin_id != point['closest_bin_id']):
                last_bin_id = point['closest_bin_id']
                picking_counter += 1
            picking_id += [picking_counter]
            
        return picking_id #, stage_id
    
#    stage_id, picking_id = find_stage_picking_id(truck)
    picking_id = find_stage_picking_id(truck)
    
#    truck['stage_id'] = stage_id
    truck['picking_id'] = picking_id
    
    
#    parking_truck = truck[truck['stage'] == 'park']
    
    
    
    def find_representative_points(parking_truck):
    
#        group = parking_truck.groupby('stage_id')
#        parking_on_road_truck = group.filter(lambda g: (g['vid'].count() < 5000) & (g['vid'].count() > 2) )
#        
#        
#        group = parking_on_road_truck.groupby('stage_id').idxmin()['distance_to_closest_bin']
#        min_parking_on_road_truck = parking_on_road_truck.loc[group]
        
        
        group = parking_truck.groupby('picking_id').idxmin()['distance_to_closest_bin']
        min_parking_on_road_truck = parking_truck.loc[group]
    
        return min_parking_on_road_truck
    
    
    
    min_parking_on_road_truck = find_representative_points(truck)
#    min_parking_on_road_truck = min_parking_on_road_truck[min_parking_on_road_truck['distance_to_closest_bin'] < 0.050]
    
    min_parking_on_road_truck.sort_values('timestamp', inplace=True)
    min_parking_on_road_truck['timestamp'] = pd.to_datetime(min_parking_on_road_truck['timestamp'])
    
    return min_parking_on_road_truck



## FInd sequence
def extract_sequence(truck_data):
    from collections import OrderedDict
    MAX_PICKUP_DISTANCE = 0.04
    
    truck_data = truck_data[truck_data['distance_to_closest_bin'] < MAX_PICKUP_DISTANCE]

    
    temp_truck = truck_data.copy().reset_index(drop=True)
    bin_sequences = []
    
    for i, point in temp_truck.iterrows():
        
        if (i == 0):
            sequence = []
        elif (point['timestamp'] - temp_truck.loc[i-1, 'timestamp'] 
            > datetime.timedelta(hours=1.5)):
            bin_sequences += [sequence]
            sequence = []
    
        sequence += [point['closest_bin_id']]
    bin_sequences += [sequence]

    unique_bin_sequence = [list(OrderedDict.fromkeys(sequence)) for sequence in bin_sequences]
    return pd.DataFrame(unique_bin_sequence).T
#    return unique_bin_sequence




all_bin = fetch_bin_data()
vid = '359486060261458'
truck = fetch_truck_data(vid)

min_parking_on_road_truck = find_parking_points(all_bin, truck)


bin_sequence = extract_sequence(min_parking_on_road_truck)


#truck[truck['stage'] == 'dumpsite']
#truck[truck['stage'] == 'terminal1']
#truck[truck['stage'] == 'terminal2']


from difflib import SequenceMatcher

SequenceMatcher(None, bin_sequence[3], bin_sequence[7]).ratio()



#len(set(bin_sequence[8]).intersection(bin_sequence[11]))
#
#len(set(bin_sequence[0]).union(bin_sequence[5]))




def merge_seqs(*seqs):
    '''Merge sequences that share a hidden order.'''
    order_map = defaultdict(set)
    for s in seqs:
        for i, elem in enumerate(s):
            order_map[elem].update(s[:i])
    return toposort_flatten(dict(order_map))













#def find_distance(truck):
#    p2 = (truck['lat'], truck['lon'])
#    return vincenty(DUMPSITE_LOCATION, p2).meters

#truck['vincenty'] = truck.apply(find_distance, axis=1)



#from math import radians, cos, sin, asin, sqrt
#def haversine(truck):
#    lat1, lon1 = DUMPSITE_LOCATION
#    lat2, lon2 = truck['lat'], truck['lon']
#    # convert decimal degrees to radians 
#    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
#    # haversine formula 
#    dlon = lon2 - lon1 
#    dlat = lat2 - lat1 
#    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
#    c = 2 * asin(sqrt(a)) 
#    km = 6367 * c
#    return km
#
#truck['haversine'] = truck.apply(haversine, axis=1)