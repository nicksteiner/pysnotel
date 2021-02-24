__author__ = 'nsteiner'

import pandas as pd

import snotel


DATA_STORE = './snotel_dataframe_store.h5'
ALASKA_SET = [962, 958, 968, 2212, 957, 1177, 1175, 2210, 2211, 2065, 2081,
              950, 963, 1094, 1089, 967, 2080, 1233]


def freeze_stations(station_list):
    h5_store = pd.HDFStore(DATA_STORE, append=True)
    try:
        for station in station_list:
            print('Freezing --> {}'.format(station))
            data_frame = station.data_frame
            add_dataframe(h5_store, station.StationTriplet, data_frame)
    except:
        h5_store.close()

def add_dataframe(hdf_store, station_id, data_frame):
    index_ = index_fromtriplet(station_id)
    try:
        del hdf_store[index_]
    except: pass
    hdf_store[index_] = data_frame


def unfreeze_stations(station_list):
    h5_store = pd.HDFStore(DATA_STORE)
    try:
        for station in station_list:
            station.data_frame = h5_store[station.StationTriplet]
    except:
        h5_store.close()
    return station_list


def freeze_alaska_stations():
    station_list = [snotel.find_station_byid(id_, 'AK') for id_ in ALASKA_SET]
    freeze_stations(station_list)


def unfreeze_alaska_stations():
    station_list = [snotel.find_station_byid(id_, 'AK') for id_ in ALASKA_SET]
    return unfreeze_stations(station_list)

def index_fromtriplet(triplet_string):
    return triplet_string.replace(':', '_')

def test_unfreeze_alaska_stations():
    station_list = unfreeze_alaska_stations()

if __name__ == '__main__':
    test_unfreeze_alaska_stations()
    #freeze_alaska_stations()