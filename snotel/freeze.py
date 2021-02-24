__author__ = 'nsteiner'

import os
import pandas as pd

import snotel
import socket

host_ = socket.gethostname()

_PATH = os.path.dirname(os.path.realpath(__file__))
DATA_STORE = os.path.join(_PATH, './snotel_dataframe_store.h5')

ALASKA_SET = [962, 958, 968, 2212, 957, 1177, 1175, 2210, 2211, 2065, 2081,
              950, 963, 1094, 1089, 967, 2080, 1233]

NEW_SET = [957]

#ALASKA_SET = [2212, 957, 1177, 1175, 2210, 2211, 2065, 2081,
#              950, 963, 1094, 1089, 967, 2080, 1233]


def freeze_stations(station_list, metric='mean', data_store=DATA_STORE):
    for station in station_list:
        print('Freezing --> {}'.format(station))
        data_frame = station.get_data_frame(frequency='D', apply_filter=True, how=metric)
        _set_dataframe(station.StationTriplet, data_frame, metric=metric, data_store=data_store)


def _set_dataframe(station_id, data_frame, metric='mean', data_store=DATA_STORE):
    if not metric.lower() == 'mean':
        metric_end = '_' + metric
    else:
        metric_end = ''
    index_ = _index_fromtriplet(station_id) + metric_end
    data_frame.to_hdf(data_store, index_, mode='a')


def _get_dataframe(station_id, metric='mean', data_store=DATA_STORE):
    if not metric.lower() == 'mean':
        metric_end = '_' + metric
    else:
        metric_end = ''
    index_ = _index_fromtriplet(station_id) + metric_end
    return pd.read_hdf(data_store, index_)


def unfreeze_stations(station_list, metric='mean', data_store=DATA_STORE):
    for station in station_list:
        data_frame = _get_dataframe(station.StationTriplet, metric=metric, data_store=data_store)
        station.data_frame = data_frame
    return station_list

def unfreeze_station(station_triplet, metric='mean', data_store=DATA_STORE):
    station = snotel.get_station(station_triplet)
    data_frame = _get_dataframe(station.StationTriplet, metric=metric, data_store=data_store)
    station.data_frame = data_frame
    return station


def freeze_alaska_stations(metric='mean', data_store=DATA_STORE):
    station_list = _get_station_list(ALASKA_SET)
    freeze_stations(station_list, metric=metric, data_store=data_store)


def unfreeze_alaska_stations(metric='mean', data_store=DATA_STORE):
    station_list = _get_station_list(ALASKA_SET)
    return unfreeze_stations(station_list, metric=metric, data_store=data_store)


def _get_station_list(station_id_list):
    station_list = []
    for station_id in station_id_list:
        station = snotel.find_station_byid(station_id, 'AK')
        if station is not None:
            station_list.append(station)
    return station_list


def _index_fromtriplet(triplet_string):
    return triplet_string.replace(':', '_')


def _test_unfreeze_alaska_stations():
    station_list = unfreeze_alaska_stations()
    return station_list


if __name__ == '__main__':
    freeze_alaska_stations(metric='min')
    freeze_alaska_stations(metric='max')
    #print(_test_unfreeze_alaska_stations())
