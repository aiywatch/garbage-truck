import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

truck = pd.read_csv('data/truck.csv')
garbage_bin = pd.read_csv('data/garbage_bin.csv')

truck = truck[::6]

truck['timestamp'] = pd.to_datetime(truck['timestamp'])

def classify_stage(point):
    if point['speed'] < 0.8:
        return 'park'
    elif point['speed'] < 7:
        return 'slow'
    else:
        return 'drive'

truck['stage'] = truck.apply(classify_stage, axis=1)

shown_truck = truck[truck['stage'] == 'park']


map = Basemap(projection='mill', llcrnrlat=7.85, urcrnrlat=7.92, llcrnrlon=98.25, 
              urcrnrlon=98.35, resolution='i')

map.drawcoastlines()
map.drawcountries(linewidth=0.25)
map.fillcontinents()

map.drawmapboundary()

for index, bin in shown_truck[::10].iterrows():
    x,y = map(bin['lon'], bin['lat'])
    map.plot(x,y, 'bo')

for index, bin in garbage_bin[garbage_bin['type'] == 'metal'].iterrows():
    x,y = map(bin['lon'], bin['lat'])
    map.plot(x,y, 'ro')

for index, bin in garbage_bin[garbage_bin['type'] == 'plastic'].iterrows():
    x,y = map(bin['lon'], bin['lat'])
    map.plot(x,y, 'go')





plt.title('Phuket')
plt.show()


#truck['lat2'] = truck['lat'].shift(1)
#truck['lon2'] = truck['lon'].shift(1)
#truck['speed2'] = truck['speed'].shift(1)
#truck['time2'] = (truck['timestamp'].shift(1) - truck['timestamp']).dt.seconds
#
#truck['lat3'] = truck['lat'].shift(2)
#truck['lon3'] = truck['lon'].shift(2)
#truck['speed3'] = truck['speed'].shift(2)
#truck['time3'] = (truck['timestamp'].shift(2) - truck['timestamp']).dt.seconds
#
#truck['lat4'] = truck['lat'].shift(3)
#truck['lon4'] = truck['lon'].shift(3)
#truck['speed4'] = truck['speed'].shift(3)
#truck['time4'] = (truck['timestamp'].shift(3) - truck['timestamp']).dt.seconds

#truck = truck.dropna()
#
#
#X = truck[['lat', 'lon', 'speed', 'lat2', 'lon2', 'speed2', 'time2'
#           ,'lat3', 'lon3', 'speed3', 'time3', 'lat4', 'lon4', 'speed4', 'time4']]


#
#from sklearn.cluster import KMeans
#kmeans = KMeans(n_clusters=4, init='k-means++', max_iter=1000, n_init=10)
#y_pred = kmeans.fit_predict(X)
























