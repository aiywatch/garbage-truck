import pandas as pd
import numpy as np
import datetime
from geopy.distance import vincenty


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
#    truck = truck[truck['speed'] < 1]
    return truck


all_bin = fetch_bin_data()
vid = '359486060261458'
truck = fetch_truck_data(vid)


#stop_truck = truck[truck['speed'] < 0.8]

def get_stages(truck):
    print('getting stages ...')
    truck['stage'] = truck.apply(lambda r: 'stop' if r['speed'] < 0.8 else 'drive', axis=1)
    
    stage_id = []
    stage_counter = 0
    last_stage = ''
    for i, point in truck.iterrows():
        if(last_stage != point['stage']):
            last_stage = point['stage']
            stage_counter += 1
        stage_id += [stage_counter]
    
    truck['stage_id'] = stage_id
    
    group = truck.groupby('stage_id')
    parking_truck = group.filter(lambda g: (g['vid'].count() > 4000))
    truck.loc[parking_truck.index, 'stage'] = 'long_stop'
    print('stages done...')


get_stages(truck)

def get_trip_id():
    print('getting trip id ...')
    trip_id = []
    id_counter = 0
    last_trip = ''
    for i, point in truck.iterrows():
        if((last_trip == 'long_stop') & (point['stage'] != 'long_stop') ):
            id_counter += 1
        last_trip = point['stage']
        trip_id += [id_counter]
    
    truck['trip_id'] = trip_id
    print('trip id done ...')

get_trip_id()

short_stop_truck = truck[truck['stage'] == 'stop']

def find_closest_bin_and_distance(all_bin, truck):
    print('finding closest bin ...')
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
    
#    return closest_bin, min_distances

#    closest_bin, short_stop_truck['distance_to_closest_bin'] = find_closest_bin_and_distance(all_bin, short_stop_truck)
    
    get_bin_id = np.vectorize(lambda i: all_bin.loc[i, 'id'])
    
    truck['distance_to_closest_bin'] = min_distances
    short_stop_truck['closest_bin_id'] = get_bin_id(closest_bin)
    print('closest bin done ...')

find_closest_bin_and_distance(all_bin, short_stop_truck)



def find_representative_points(parking_truck):
    print('finding representative point ...')
    group = parking_truck.groupby('stage_id').idxmin()['distance_to_closest_bin']
    min_parking_on_road_truck = parking_truck.loc[group]

    group = min_parking_on_road_truck.groupby(['trip_id', 'closest_bin_id']).idxmin()['distance_to_closest_bin']
    min_truck = min_parking_on_road_truck.loc[group]
    
    print('representative done ...')
    return min_truck



min_truck = find_representative_points(short_stop_truck)
#    min_parking_on_road_truck = min_parking_on_road_truck[min_parking_on_road_truck['distance_to_closest_bin'] < 0.050]

min_truck.sort_values('timestamp', inplace=True)
min_truck['timestamp'] = pd.to_datetime(min_truck['timestamp'])


MAX_PICKUP_DISTANCE = 0.04
truck_data = min_truck[min_truck['distance_to_closest_bin'] < MAX_PICKUP_DISTANCE]
bin_sequence = truck_data.groupby('trip_id')['closest_bin_id'].apply(list)

def select_sequence(bin_sequence):
    selected_sequence = bin_sequence.iloc[1]
    selected_sequence_trip_id = bin_sequence.index[1]
    selected_route = truck[truck['trip_id'] == selected_sequence_trip_id]
    selected_route = selected_route[selected_route['stage'] != 'long_stop'][::40]
    
    all_bin_id = all_bin.set_index('id')
    selected_sequence_with_detail = all_bin_id.loc[selected_sequence, :].drop('index', axis=1)

    return selected_sequence_with_detail, selected_route

selected_sequence, selected_route = select_sequence(bin_sequence)
































