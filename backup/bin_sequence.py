import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

response = requests.get('https://api.traffy.xyz/v0/trash/?type=metal&limit=100').json()

metal_bin_dict = response['results']


response = requests.get('https://api.traffy.xyz/v0/trash/?type=plastic&limit=100').json()

plastic_bin_dict = response['results']



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


metal_bins = extract_bin_info(metal_bin_dict)
plastic_bins = extract_bin_info(plastic_bin_dict)

garbage_bins = pd.concat([metal_bins, plastic_bins]).reset_index(drop=True)

garbage_bins.to_csv('data/garbage_bin.csv', index=False)


truck = pd.read_csv('data/truck.csv')

truck = truck.iloc[::500, :]

#import geopandas as gpd
#
#
#from geopy.geocoders import Nominatim
#geolocator = Nominatim()
#
#
#
#location = geolocator.reverse("7.8992151, 98.3050894")
#
from geopy.distance import vincenty
#newport_ri = (7.8992151, 98.3050894)
#cleveland_oh = (7.90147116189, 98.3080067334)
#print(vincenty(newport_ri, cleveland_oh).meters)



#usr_lon = 98.3663
#usr_lat = 7.89635

map = Basemap(projection='mill', llcrnrlat=7.85, urcrnrlat=7.92, llcrnrlon=98.25, 
              urcrnrlon=98.35, resolution='h')

map.drawcoastlines()
map.drawcountries(linewidth=0.25)
map.fillcontinents()

map.drawmapboundary()

#x,y = map(usr_lon, usr_lat)
#map.plot(x,y, 'ro')

for index, bin in metal_bins.iterrows():
    x,y = map(bin['lon'], bin['lat'])
    map.plot(x,y, 'ro')

for index, bin in plastic_bins.iterrows():
    x,y = map(bin['lon'], bin['lat'])
    map.plot(x,y, 'go')

for index, truck_p in truck.iterrows():
    x,y = map(truck_p['lon'], truck_p['lat'])
    map.plot(x,y, 'bo')

plt.title('Phuket')
plt.show()



def distance(bin_point):
    lat1, lon1 = truck_point['lat'], truck_point['lon']
    lat2, lon2 = bin_point['lat'], bin_point['lon']
    
    p = 0.017453292519943295
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    return 12742 * asin(sqrt(a))

from math import cos, asin, sqrt


def closest_bin(truck_point):
    distance = 10000000
    bin_id = 0
    
    lat_truck, lon_truck = truck_point['lat'], truck_point['lon']
    
#    def distance_to_bin(bin_point):
#        lat_bin, lon_bin = bin_point['lat'], bin_point['lon']
#        return vincenty((lat_truck, lon_truck), (lat_bin, lon_bin)).meters

#    def distance(bin_point):
#        lat1, lon1 = truck_point['lat'], truck_point['lon']
#        lat2, lon2 = bin_point['lat'], bin_point['lon']
#        
#        p = 0.017453292519943295
#        a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
#        return 12742 * asin(sqrt(a))
#    
#    distances = garbage_bins.apply(distance, axis=1)
    
    
    for index, bin in garbage_bins.iterrows():
        lat_bin, lon_bin = bin['lat'], bin['lon']
        d = vincenty((lat_truck, lon_truck), (lat_bin, lon_bin)).meters
        if(d < distance):
            distance = d
            bin_id = bin['id']
    
#    print(truck_point)
    return distance, bin_id


#for index, truck_p in truck.iterrows():
#    x,y = map(truck_p['lon'], truck_p['lat'])
#    map.plot(x,y, 'bo')



closest_bin(truck.iloc[0,:])


#distances = []
#bin_ids = []
#for index, truck_point in truck.iterrows():
#    distance, bin_id = closest_bin(truck_point)
#    distances = [distance]
#    bin_ids = [bin_id]
#    print(index)



#truck.apply(closest_bin, axis=1)




#truck['distance_to_closet_bin'] = 10000000
#truck['closest_bin_id'] = -1

result = []
for index, bin in garbage_bins.iterrows():
    lat_bin, lon_bin = bin['lat'], bin['lon']
    bin_id = bin['id']
    
#    def distance(bin_point):
#        lat1, lon1 = truck_point['lat'], truck_point['lon']
#        lat2, lon2 = bin_point['lat'], bin_point['lon']
#        
#        p = 0.017453292519943295
#        a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
#        return 12742 * asin(sqrt(a))
    
    lat1, lon1 = np.array([bin['lat']] * truck.shape[0]), np.array([bin['lon']] * truck.shape[0])
    lat2, lon2 = truck['lat'], truck['lon']
    
    p = 0.017453292519943295
    a = 0.5 - np.cos((lat2-lat1)*p)/2 + np.cos(lat1*p)*np.cos(lat2*p) * (1-np.cos((lon2-lon1)*p)) / 2
    
#    print(index)
    result += [12742 * np.arcsin(np.sqrt(a))]
#    distances = truck.apply(distance, axis=1)

    
result1 = result[10]
    
result = np.array(result)

min_distances = result.min(axis=0)
closest_bin = result.argmin(axis=0)

get_bin_id = np.vectorize(lambda i: garbage_bins.loc[i, 'id'])

#closest_bin.vectorize(lambda i: garbage_bins.loc[i, 'id'])

truck['distance_to_closest_bin'] = min_distances
truck['closest_bin_id'] = get_bin_id(closest_bin)

pick_up_garbage = truck[truck['distance_to_closest_bin'] < 0.050]
pick_up_garbage['closest_bin_id'].unique()












