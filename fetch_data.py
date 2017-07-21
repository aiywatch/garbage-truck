import requests
import pandas as pd
import numpy as np

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



































