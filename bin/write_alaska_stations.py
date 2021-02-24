__author__ = 'nsteiner'

from snotel import *
import copy

attributes = ['Name', 'StationTriplet', 'CountyName', 'Latitude',
              'Longitude', 'Elevation', 'StationDataTimeZone']

_PATH = os.path.dirname(os.path.realpath(__file__))
START = datetime.datetime(2007, 1, 1)
END = datetime.datetime(2011, 1, 1)

def parse_element_triplet(element_triplet):
    el_list = element_triplet.split(':')
    st_id, state, network, elementcd, _, depth = el_list
    return str(elementcd), str(depth)

def filter_keys(frame):
    frame_keys = frame.columns
    elementcd = [('TOBS', 'None'), ('SMS', '-2.0'),
                 ('STO', '-2.0'), ('SMS', '-4.0'),
                 ('STO', '-4.0')]
    element_depth = [parse_element_triplet(key_) for key_ in frame_keys]
    return frame[[key_ for key_, el_dp in zip(frame_keys, element_depth) if el_dp in elementcd]]

def filter_flags(frame):
    flags_ = [key for key in frame if key.endswith('_flag')]
    value_flag = [[(key, flag) for key in frame if key == flag.rstrip('_flag')][0] for flag in flags_]
    frame_dict = dict([(value, frame[value][frame[flag] == 'V']) for value, flag in value_flag])
    return pd.DataFrame(frame_dict)

def write_data_frame(data_frame, file):
    data_frame.to_csv(file)


def file_name(station):
    return station.StationTriplet.replace(':', '_') + '.csv'


def trim_dataframe(data_frame):
    start_ = data_frame.index.searchsorted(START)
    end_ = data_frame.index.searchsorted(END)
    try:
        frame_slice = data_frame.iloc[start_:end_]
    except:
        frame_slice = None
    return frame_slice


def aggregate_df(data_frame):
    df_ = None
    for how, name in [(np.nanmean, 'mean'), (np.nanstd, 'std')]:
        agg_frame = data_frame.resample('D', how=how)
        agg_frame.columns = ['{}_{}'.format(col, name) for col in agg_frame.columns]
        if not df_:
            df_ = agg_frame
        else:
            df_ = df_.join(agg_frame)
    return df_

def write_header(station, file):
    file.write('--HEADERSTART--\n')
    for meta in attributes:
        file.write('{} = {}\n'.format(meta, getattr(station, meta, None)))
    file.write('--HEADEREND--\n')

def write_dataframe(station, frame):
    frame.index.name = 'date'
    file_path = os.path.join(_PATH, 'out', file_name(station))
    with open(file_path, 'w') as file:
        write_header(station, file)
        write_data_frame(frame, file)

if __name__ == '__main__':
    filter_alaska = {'FipsStateNumber': '02'}
    alaska_station_list = get_station_objects(filter_alaska)
    for station_temp in alaska_station_list:
        station = copy.deepcopy(station_temp)
        print(station)
        if station.raw_data_frame:
            data_frame = station.raw_data_frame
            data_frame = filter_flags(data_frame).resample('H')
            data_frame = trim_dataframe(data_frame)
            data_frame = filter_keys(data_frame)
            frame = aggregate_df(data_frame)
            if frame:
                write_dataframe(station, frame)
        del station