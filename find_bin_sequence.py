import pandas as pd
import numpy as np
import connection
import arrow
import datetime
import fetch_data
import pymongo

MIN_BINS_IN_A_SEQUENCE = 3


def fetch_bin_state():
    truck_state_adapter = connection.connect_mongo_truck_state()
    
    start_date = arrow.get(datetime.datetime(2017,8,5), 'Asia/Bangkok').datetime
    end_date = arrow.utcnow().datetime
    vids = ['359486060261821', '359486060261458']
    
    truck_states = []
    for state in truck_state_adapter.find({'vid': {"$in": vids}, 'state_begin': 
        {'$gte': start_date, '$lte': end_date}} ).sort([['vid', pymongo.ASCENDING], ['state_begin', pymongo.ASCENDING]]):
        truck_states += [state]
    
    truck_states = pd.DataFrame(truck_states)
    return truck_states


truck_states = fetch_bin_state()

def get_bin_sequences(truck_states):
    trip_id = 0
    trip_ids = []
    for i, point in truck_states.iterrows():
        trip_ids += [trip_id]
        if(point['state'] == 'at_term'):
            trip_id += 1
    
    truck_states['trip_id'] = trip_ids
    
    
    collecting_truck = truck_states[truck_states['state'] == 'collecting']
    
    bin_sequences = []
    last_trip = collecting_truck.iloc[0]['trip_id']
    sequence = []
    last_bins = []
    for i, point in collecting_truck.iterrows():
        if(point['trip_id'] != last_trip):
            last_trip = point['trip_id']
            if len(sequence) > MIN_BINS_IN_A_SEQUENCE:
                bin_sequences += [sequence]
            sequence = []
        if(point['collecting_bin_ids'] != last_bins):
            sequence += [point['collecting_bin_ids']]
            last_bins = point['collecting_bin_ids']
    
    if len(sequence) > MIN_BINS_IN_A_SEQUENCE:
        bin_sequences += [sequence]
    return bin_sequences

bin_sequences = get_bin_sequences(truck_states)

bin_sequences_df = pd.DataFrame(bin_sequences).T

def smooth_sequence(bin_sequences):
    bin_sequence_smooth = []
    for bin_row in bin_sequences:
        row = []
        last_bin = -1
        for bins in bin_row:
            for bin in bins:
                if bin != last_bin:
                    row += [bin]
                    last_bin = bin
        bin_sequence_smooth += [row]
    return bin_sequence_smooth

bin_sequence_smooth = smooth_sequence(bin_sequences)


def get_id_latlon_mapper():

    rep_bin_adapter = connection.connect_mongo_representative_auto_detected_bin()
    
    metal_bin = fetch_data.fetch_extract_bin_data('metal')
    plastic_bin = fetch_data.fetch_extract_bin_data('plastic')
    
    mapper = {}
    for rbin in rep_bin_adapter.find():
        mapper[rbin['bin_id']] = [rbin['lat'], rbin['lon']]
    
    for i, rbin in metal_bin.iterrows():
        mapper[rbin['id']] = [rbin['lat'], rbin['lon']]
    
    for i, rbin in plastic_bin.iterrows():
        mapper[rbin['id']] = [rbin['lat'], rbin['lon']]
        
    return mapper

latlon_mapper = get_id_latlon_mapper()

def id_to_latlon(row):
    latlon_row = []
    for item in row:
        if(item != None):
           latlon_row += [latlon_mapper[int(bid)] for bid in item]
#           print([latlon_mapper[int(bid)] for bid in item])
    return latlon_row

sequence_latlon = bin_sequences_df.apply(id_to_latlon)

#sequence_latlon.to_csv('bin_sequence.csv')






























#data = pd.DataFrame(columns = ['2_last_bin', '1_last_bin', 
#                    'current_bin', 'next_bin'])
#
#for sequence in bin_sequence_smooth:
#    current_bin = sequence + ['-1', '-1']
#    last_bin_1 = ['-1'] + sequence + ['-1'] 
#    last_bin_2 = ['-1', '-1'] + sequence
#    next_bin = sequence[1:] + ['-1', '-1', '-1']
#
#    data_temp = pd.DataFrame({'2_last_bin': last_bin_2, '1_last_bin': last_bin_1, 
#                         'current_bin': current_bin, 'next_bin': next_bin})
#    data = data.append(data_temp, ignore_index=True)



#data = data[data['current_bin'] != '-1']
#
#X = data[['current_bin']].values
#y = data['next_bin']
#
#
#
#
#from keras.models import Sequential
#from keras.layers import Dense, LSTM
#from keras.regularizers import l2
#from keras.utils.np_utils import to_categorical
#
#X_cat = X.reshape(1,1,1034)
#y_cat = to_categorical(y, num_classes=None)
#
#
#
#model = Sequential()
##model.add(LSTM(100, input_shape=(1,100), return_sequences=True))
##model.add(Dense(100))
##model.compile(loss='mean_absolute_error', optimizer='adam', metrics=['accuracy'])
##model.fit(data, target, nb_epoch=10000, batch_size=1, verbose=2, 
##          validation_data=(X_test,y_test))
#
#model.add(LSTM(10, input_shape=(1,1034), activation='relu',
#               kernel_regularizer=l2(0.01), return_sequences=True))
#model.add(Dense(10, activation='relu',
#               kernel_regularizer=l2(0.01)))
#model.add(Dense(2795, activation='softmax'))
#
#
#model.compile(optimizer='adam', loss='categorical_crossentropy',
#                   metrics=['accuracy'])
#
#model.fit(X_cat, y_cat, batch_size=32, epochs=500)
#
#predict = model.predict(X)
#
#predict_class = predict.argmax(axis=1)
































            
            



























