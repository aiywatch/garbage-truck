#import numpy as np
#import pandas as pd
#from sklearn.preprocessing import StandardScaler
#
#from keras.models import Sequential
#from keras.layers import Dense, LSTM
#from keras.utils import np_utils

import find_bin_sequence

bin_sequences = find_bin_sequence.bin_sequence_smooth


class Garbage_bin:
    def __init__(self, bin_id):
        self.bin_id = bin_id
        self.next_bin_ids = {}
        self.prev_next_bin_ids = {}
        self.prev2_next_bin_ids = {}
    
    def add_next_bin(self, next_id):
        if next_id not in self.next_bin_ids:
            self.next_bin_ids[next_id] = 1
        else:
            self.next_bin_ids[next_id] += 1
            
    def add_prev_next_bin(self, prev_id, next_id):
        if prev_id not in self.prev_next_bin_ids:
            self.prev_next_bin_ids[prev_id] = {}
        if next_id not in self.prev_next_bin_ids[prev_id]:
            self.prev_next_bin_ids[prev_id][next_id] = 1
        else:
            self.prev_next_bin_ids[prev_id][next_id] += 1
            
    def add_prev2_next_bin(self, prev2_id, prev_id, next_id):
        prev_ids = (prev2_id, prev_id)
        if prev_ids not in self.prev2_next_bin_ids:
            self.prev2_next_bin_ids[prev_ids] = {}
        if next_id not in self.prev2_next_bin_ids[prev_ids]:
            self.prev2_next_bin_ids[prev_ids][next_id] = 1
        else:
            self.prev2_next_bin_ids[prev_ids][next_id] += 1
            
    def get_all_next_bins(self, prev_id=None, prev2_id=None):
        lv2 = {}
        lv3 = {}
        if prev_id in self.prev_next_bin_ids:
            lv2 = self.prev_next_bin_ids[prev_id]
        if (prev2_id, prev_id) in self.prev2_next_bin_ids:
            lv3 = self.prev2_next_bin_ids[(prev2_id, prev_id)]
            
        return {'lv1': self.next_bin_ids, 'lv2': lv2, 'lv3': lv3}
    
    def next_bins_with_rank(self, prev_id=None, prev2_id=None):
        bin_dict = self.get_all_next_bins(prev_id)
        sorted_lv1 = sorted(bin_dict['lv1'].items(), key=lambda x: x[1], reverse=True)
        sorted_lv2 = sorted(bin_dict['lv2'].items(), key=lambda x: x[1], reverse=True)
        sorted_lv3 = sorted(bin_dict['lv3'].items(), key=lambda x: x[1], reverse=True)
        
        sorted_all = sorted_lv3 + sorted_lv2 + sorted_lv1
        
        result = []
        for bin_id, c in sorted_all:
            if bin_id not in result:
                result += [bin_id]
        
        return result
            
class Bin_collection:
    def __init__(self, bin_sequences):
        self.bin_info = {}
        self.bin_collection = []
            
        for sequence in bin_sequences:
            for i, bin_id in enumerate(sequence):
                self.add_bin(bin_id)
                if i in range(2, len(sequence)-1):
                    self.add_prev2_next_bin(bin_id, sequence[i-2], sequence[i-1], sequence[i+1])
                elif(i == 0):
                    self.add_prev2_next_bin(bin_id, 'term', 'term', sequence[i+1])
                elif(i == 1):
                    self.add_prev2_next_bin(bin_id, 'term', sequence[i-1], sequence[i+1])
                else:
                    self.add_prev2_next_bin(bin_id, sequence[i-2], sequence[i-1], 'term')
    
    def add_bin(self, bin_id):
        if bin_id not in self.bin_info:
            garbage_bin = Garbage_bin(bin_id)
            self.bin_info[bin_id] = {'count': 1, 'index': len(self.bin_collection)}
            self.bin_collection += [garbage_bin]
        else:
            self.bin_info[bin_id]['count'] += 1
    
    def get_bin(self, bin_id):
        bin_index = self.bin_info[bin_id]['index']
        return self.bin_collection[bin_index]
    
    def add_next_bin(self, bin_id, next_id):
        garbage_bin = self.get_bin(bin_id)
        garbage_bin.add_next_bin(next_id)
        
    def add_prev_next_bin(self, bin_id, prev_id, next_id):
        garbage_bin = self.get_bin(bin_id)
        garbage_bin.add_prev_next_bin(prev_id, next_id)
        
        self.add_next_bin(bin_id, next_id)
        
    def add_prev2_next_bin(self, bin_id, prev2_id, prev_id, next_id):
        garbage_bin = self.get_bin(bin_id)
        garbage_bin.add_next_bin(next_id)
        garbage_bin.add_prev_next_bin(prev_id, next_id)
        garbage_bin.add_prev2_next_bin(prev2_id, prev_id, next_id)



bin_collection = Bin_collection(bin_sequences)

bin7 = bin_collection.get_bin('2064')
bin7.prev_next_bin_ids
bin_dict = bin7.get_all_next_bins('1948', '2174')

bin7.next_bins_with_rank('1984')




#bin_data = [bin_id for sequence in bin_sequences for bin_id in ['term'] + sequence + ['term']]
#
#bin_to_int = {}
#int_to_bin = []
#index_counter = 0



"""

bin_to_int = {'term': 0}
int_to_bin = ['term']
index_counter = 1

bin_data = []

for sequence in bin_sequences:
#    new_sequence = [0]*SEQ_LENGTH
#    new_sequence = []
    bin_data += [0]*SEQ_LENGTH
    for bin_id in sequence:
        if bin_id not in int_to_bin:
            bin_to_int[bin_id] = index_counter
            int_to_bin += [bin_id]
            index_counter += 1
        bin_data += [bin_to_int[bin_id]]
    bin_data += [0]
#    new_sequence += [0]*SEQ_LENGTH
#    bin_data += [new_sequence]

dataX = []
dataY = []
for i in range(0, len(bin_data) - SEQ_LENGTH):
	seq_in = bin_data[i:i + SEQ_LENGTH]
	seq_out = bin_data[i + SEQ_LENGTH]
	dataX.append([bin_int for bin_int in seq_in])
	dataY.append(seq_out)
	print(seq_in, '->', seq_out)


## reshape X to be [samples, time steps, features]
#X = np.reshape(dataX, (len(dataX), SEQ_LENGTH, 1))
## normalize
#X = X / float(len(bin_data))
## one hot encode the output variable
#y = np_utils.to_categorical(dataY)
## create and fit the model
#model = Sequential()
#model.add(LSTM(32, input_shape=(X.shape[1], X.shape[2])))
#model.add(Dense(y.shape[1], activation='softmax'))
#model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
#model.fit(X, y, epochs=500, batch_size=1, verbose=2)
## summarize performance of the model
#scores = model.evaluate(X, y, verbose=0)
#print("Model Accuracy: %.2f%%" % (scores[1]*100))






X = bin_data[:-1]
y = bin_data[1:]

X_cat = np_utils.to_categorical(X)
y_cat = np_utils.to_categorical(y)

X_cat = X_cat.reshape(X_cat.shape[0], 1, X_cat.shape[1])


model = Sequential()
model.add(LSTM(100, batch_input_shape=(1,X_cat.shape[1],X_cat.shape[2]), stateful=True))
model.add(Dense(y_cat.shape[1], activation='softmax'))
model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
model.fit(X_cat, y_cat, epochs=50, batch_size=1, verbose=2)


predict = model.predict(X_cat)

predict_class = predict.argmax(axis=1)

"""


"""



### Analytics
count_dict = {}
for sequence in bin_sequences:
    seq_tem = sequence + ['term']
    for i, bin_id in enumerate(seq_tem):
        if bin_id is not 'term':
            if bin_id not in count_dict:
                count_dict[bin_id] = {}
            if seq_tem[i+1] not in count_dict[bin_id]:
                count_dict[bin_id][seq_tem[i+1]] = 1
            else:
                count_dict[bin_id][seq_tem[i+1]] += 1

y_bin = [int_to_bin[yy] for yy in y]

single_des = {i: des for i, des in count_dict.items() if len(des)==1}
double_des = {i: des for i, des in count_dict.items() if len(des)==2}





"""
"""
#bin_data_np = np.array(bin_data)

bin_data_df = pd.DataFrame(bin_data).fillna(0)

df_shape = bin_data_df.shape
data_X = bin_data_df.iloc[:, :-1].values#.reshape(df_shape[0],df_shape[1]-1,1)
data_y = bin_data_df.iloc[:, 1:].values#.reshape(df_shape[0],df_shape[1]-1,1)

X = X.reshape(df_shape[0],df_shape[1]-1,1)
y = data_y #.reshape(df_shape[0],df_shape[1]-1,1)
y2 = np_utils.to_categorical(data_y)
y2 = y2.reshape(df_shape[0],df_shape[1]-1,-1)


model = Sequential()
model.add(LSTM(32, input_shape=X.shape[1:]))
model.add(Dense(y2.shape[2], activation='softmax'))
model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
model.fit(X, y2, epochs=500, batch_size=1, verbose=2)

"""
