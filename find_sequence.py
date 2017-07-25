import pandas as pd
import numpy as np
import datetime

#import matplotlib.pyplot as plt
#from mpl_toolkits.basemap import Basemap



def fetch_bin_data():
    auto_bin = pd.read_csv('data/representative_bin.csv')
    plastic_bin = pd.read_csv('data/plastic_bin.csv')
    metal_bin = pd.read_csv('data/metal_bin.csv')
    
    all_bin = pd.concat([auto_bin, plastic_bin, metal_bin]).reset_index()
    
    return all_bin

def fetch_truck_data():
    truck = pd.read_csv('data/truck_7days.csv')
    truck = truck.iloc[::4, :]
    return truck


def find_parking_points(all_bin, truck):

    all_bin = fetch_bin_data()
    truck = fetch_truck_data()
    
    def find_closest_bin_and_distance(all_bin, truck):
        result = []
        for index, bin in all_bin.iterrows():
    #        lat_bin, lon_bin = bin['lat'], bin['lon']
    #        bin_id = bin['id']
            
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
        def classify_stage(point):
            if point['speed'] < 0.8:
                return 'park'
            elif point['speed'] < 7:
                return 'slow'
            else:
                return 'drive'
    
        truck['stage'] = truck.apply(classify_stage, axis=1)
        
        stage_id = []
        stage_counter = 0
        last_stage = ''
        picking_id = []
        picking_counter = 0
        last_bin_id = ''
        for i, point in truck.iterrows():
            if(last_stage != point['stage']):
                last_stage = point['stage']
                stage_counter += 1
            stage_id += [stage_counter]
            
            if(last_bin_id != point['closest_bin_id']):
                last_bin_id = point['closest_bin_id']
                picking_counter += 1
            picking_id += [picking_counter]
            
        return stage_id, picking_id
    
    stage_id, picking_id = find_stage_picking_id(truck)
    
    truck['stage_id'] = stage_id
    truck['picking_id'] = picking_id
    
    
    parking_truck = truck[truck['stage'] == 'park']
    
    
    
    def find_representative_points(parking_truck):
    
        group = parking_truck.groupby('stage_id')
        parking_on_road_truck = group.filter(lambda g: (g['vid'].count() < 5000) & (g['vid'].count() > 10) )
        
        
        group = parking_on_road_truck.groupby('stage_id').idxmin()['distance_to_closest_bin']
        min_parking_on_road_truck = parking_on_road_truck.loc[group]
        
        
        group = min_parking_on_road_truck.groupby('picking_id').idxmin()['distance_to_closest_bin']
        min_parking_on_road_truck = min_parking_on_road_truck.loc[group]
    
        return min_parking_on_road_truck
    
    
    
    min_parking_on_road_truck = find_representative_points(parking_truck)
#    min_parking_on_road_truck = min_parking_on_road_truck[min_parking_on_road_truck['distance_to_closest_bin'] < 0.050]
    
    min_parking_on_road_truck.sort_values('timestamp', inplace=True)
    min_parking_on_road_truck['timestamp'] = pd.to_datetime(min_parking_on_road_truck['timestamp'])
    
    return min_parking_on_road_truck



## FInd sequence
def extract_sequence(truck_data):
    from collections import OrderedDict
    MAX_PICKUP_DISTANCE = 0.03
    
    truck_data = truck_data[truck_data['distance_to_closest_bin'] < MAX_PICKUP_DISTANCE]

    
    temp_truck = truck_data.copy().reset_index(drop=True)
    bin_sequences = []
    
    for i, point in temp_truck.iterrows():
        
        if (i == 0):
            sequence = []
        elif (point['timestamp'] - temp_truck.loc[i-1, 'timestamp'] 
            > datetime.timedelta(hours=1)):
            bin_sequences += [sequence]
    
        sequence += [point['closest_bin_id']]
    bin_sequences += [sequence]

    unique_bin_sequence = [list(OrderedDict.fromkeys(sequence)) for sequence in bin_sequences]
    return pd.DataFrame(unique_bin_sequence).T




all_bin = fetch_bin_data()
truck = fetch_truck_data()

min_parking_on_road_truck = find_parking_points(all_bin, truck)


bin_sequence = extract_sequence(min_parking_on_road_truck)










