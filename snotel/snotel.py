#!/usr/bin/env python
''' Snotel Library
    ~~~~~~~~~~~~~~
    Use to keep a local/searchable copy of snotel data.
'''

__author__ = 'Nick Steiner'
__email__ = 'nick@nicksteiner.com'
__version__ = '0r9'

import os
import sys
import copy
import logging
import datetime
import argparse
import socket
import itertools as it
from multiprocessing import Pool
from contextlib import contextmanager

import pytz
import numpy as np
import pandas as pd
import sqlite3 as sql

from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import and_, or_, types, Column, create_engine, distinct, desc, asc

from .elementrecord import elementcd_toload, duration_toload
import pathlib
from suds.client import Client

'''
Database Engines
~~~~~~~~~~~~~~~~
'''
_PATH = pathlib.Path(__file__).parent

_SQL_PATH = _PATH / 'snotel.sqlite'

engine_str = 'sqlite:///{}'.format(_SQL_PATH.absolute())

ee = engine.create_engine(engine_str)

metadata = MetaData(bind=ee)
Session = sessionmaker(bind=ee)
Base = declarative_base(metadata=metadata)

# in process of refactoring
collection_engine = ee # reate_engine('sqlite:///{}'.format(os.path.join(_PATH, 'collection.sqlite')))


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


# LOGGING
# =======
logging.basicConfig(level=logging.INFO,
                    filename='snotel.log',
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')

# output logging to stdout
VERBOSE = True

# SOAP Client Configuration
# =========================
# paste this at the start of code
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

URL = 'http://www.wcc.nrcs.usda.gov/awdbWebService/services?wsdl'

client = Client(URL, timeout=900)


'''
    Lazy Property Init
    ~~~~~~~~~~~~~~~~~~
    Lazy initialization of data intensive and readonly object properties
    NOTE: requires unique _get_+attrname function to initialize
'''


def lazy_init(fn):
    attr_name = '_' + fn.__name__

    @property
    def _lazy_init(self):
        attr_setter = getattr(self, '_get_' + fn.__name__)
        if not hasattr(self, attr_name):
            setattr(self, attr_name, attr_setter())
        return getattr(self, attr_name)

    return _lazy_init


'''
Misc Functions
~~~~~~~~~~~~~~~
'''

METMIN = 42.65 / 1000 / 60
GRPSIZE = 500
CamelCase = lambda name: name[0].upper() + name[1:]
lowerCamel = lambda name: name[0].lower() + name[1:]

DATE_FORMAT_TO = lambda x: datetime.datetime.strftime(x)
DATE_FORMAT_FROM = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M')
DATA_REQUEST_FMT = {
    'stationTriplets': str,
    'elementCd': str,
    'ordinal': str,
    'heightDepth': lambda x: {'value': x},
    'beginDate': DATE_FORMAT_TO,
    'endDate': DATE_FORMAT_TO}

'''
Query Functions
~~~~~~~~~~~~~~~
'''


def _get_object_filter(object_, filter_):
    with session_scope() as session:
        query = session.query(object_)
        object_list = query.filter_by(**filter_).all()
        session.expunge_all()
    return object_list

def grouper(iterable, n, fill_value=None):
    """

    :param iterable: iterable to group
    :param n: size of largest group
    :param fill_value: fill values for remainder
    :return: list of groups of size at max n-length
    """
    arguments = [iter(iterable)] * n
    groups = it.zip_longest(*arguments, fillvalue=fill_value)
    return [[val for val in group if val] for group in groups]

'''
Station Functions
~~~~~~~~~~~~~~~~~
'''
STATION_FILTER = {'FipsStateNumber': '02'}


def find_station_byid(id_number, state_abbr):
    with session_scope() as session:
        station = session.query(Station). \
            filter(Station.StationTriplet.like('%{}:{}%'.format(id_number, state_abbr))).\
                   first()
        session.expunge_all()
    return station

def find_stationtriplet_byid(id_number, state_abbr):
    with session_scope() as session:
        station_triplet_list = session.query(Station.StationTriplet). \
            filter(Station.StationTriplet.like('%{}:{}%'.format(id_number, state_abbr))).\
                   first()
        session.expunge_all()
    return station_triplet_list




def get_station_list_byelement(element_str, fips_number='02'):
    with session_scope() as session:
        '''
        select distinct s."StationTriplet" from station s, element e
          where e."StationTriplet" = s."StationTriplet" and
          e."ElementTriplet" like '%STO%' and
          s."FipsStateNumber" like '02';

        ''' # ^ example
        results = session.query(Station, Element). \
            filter(and_(Element.StationTriplet == Station.StationTriplet,
                        Station.FipsStateNumber == fips_number,
                        Element.ElementCd == element_str)).all()
        session.expunge_all()
    station_triplet_set = set([ntuple.Station.StationTriplet for ntuple in results])
    return list(station_triplet_set)

def get_station_objects(filters=STATION_FILTER):
    return _get_object_filter(Station, filters)

def get_station_list(local=True):
    print('Getting station list ...')
    if local:
        with session_scope() as session:
            station_tuples = session.query(Station.StationTriplet).all()
            session.expunge_all()
            station_list = [str(station_triplet) for station_triplet, in station_tuples]
    else:
        station_list = client.service.getStations()
    return station_list

def get_station_list_alaska(snotel_only=True):
    station_list_ = get_station_list_bycode('02')
    if snotel_only:
        station_list_ = [station_id for station_id in station_list_ if ':SNTL' in station_id]
    return station_list_

def get_station_list_bycode(fips_code):
    with session_scope() as session:
        results = session.query(Station).filter(Station.FipsStateNumber == fips_code).all()
        session.expunge_all()
    station_triplet_list = [station.StationTriplet for station in results]
    return station_triplet_list

def get_station_bytriplet(station_triplet, local=True):
    if local:
        with session_scope() as session:
            station = session.query(Station). \
                filter(Station.StationTriplet == station_triplet).first()
            session.expunge(station)
    else:
        station_meta = get_station_meta(station_triplet)
        station = construct_station(station_meta)
    return station

def get_station_meta(station_triplet=None, station_list=None):
    if station_triplet:
        return client.service.getStationMetadata(station_triplet)
    if station_list:
        return client.service.getStationMetadataMultiple(station_list)


def add_station(station):
    with session_scope() as session:
        session.merge(station)
    session.commit()

def update_station_list(station_list):
    """

    Update all stations METADATA from NWCC webservice.
    """
    metadata.create_all()
    n_stations = len(station_list)
    print('Station retrieved, {} total ...'.format(n_stations))
    print(','.join(station_list[:10] + ['...']))
    print('Getting station metadata, will take ~{:.2g} minutes ... '.format(n_stations * METMIN))
    station_meta_list = []
    ct = 0
    n_groups = n_stations / GRPSIZE
    for station_group in grouper(station_list, GRPSIZE):
        station_meta_list.extend(get_station_meta(station_list=station_group))
        ct += 1
        print('Retrieved station meta group {}/{}...'.format(ct, n_groups))
    print('Station metadata retrieved ...')
    for station_meta in station_meta_list:
        try:
            station = construct_station(station_meta)
            print('Adding: ' + station.__str__())
        except Exception as e:
            print(e)
            station = None
            print('Error Getting: {}'.format(station_meta['stationTriplet']))

        if station:
            try:
                add_station(station)
            except Exception as e:
                print(e)
                print('Error Adding: {}'.format(station.StationTriplet))
    print('Update complete ...')

def update_station_elements(station):
    print('Updating station {}'.format(station.StationTriplet))
    try:
        element_meta_list = get_element_bystationtriplet(station.StationTriplet, local=False)
        for el in element_meta_list:
            assert isinstance(el, Element)
            try:
                add_element(el)
            except Exception as e:
                raise(e('Error adding getting element for - {}'.format(station.StationTriplet)))
    except Exception as e:
        print(e)
        print('Error getting element list: {}'.format(station.StationTriplet))
        add_elements(element_meta_list)
        try:
            add_elements(element_meta_list)
        except Exception as e:
            raise(e('Error adding getting element for - {}'.format(station.StationTriplet)))

def update_stationlist_elements(station_list=None):
    """

    :rtype : None
    """
    if not station_list:
        station_list = get_station_list(local=True)
    n_stations = len(station_list)
    ct = 1
    for station_triplet in station_list:
        print('Updating station {};  {}/{}'.format(station_triplet, ct, n_stations))
        try:
            element_meta_list = get_element_bystationtriplet(station_triplet, local=False)
            for el in element_meta_list:
                assert isinstance(el, Element)
                add_element(el)
        except Exception as e:
            print(e)
            print('Error getting element list: {}'.format(station_triplet))
            ct += 1
            add_elements(element_meta_list)
            try:
                add_elements(element_meta_list)
            except Exception as e:
                print(e)
                Exception('Error adding elements for: {}'.format(station_triplet))
        ct += 1

def update_station_data(station, inpool=False):
    update_station_elements(station)
    if inpool:
        update_element_data_list_inpool(station.element_list)
    else:
        update_element_data_list_series(station.element_list)

'''
Element Functions
~~~~~~~~~~~~~~~~~
'''

def find_element_bydepth(station_triplet, element_cd, local=True, depth='min'):
    if local:
        with session_scope() as session:
            element_obj = session.query(Element). \
                filter(Element.StationTriplet == station_triplet). \
                filter(Element.ElementCd == element_cd). \
                filter(Element.Duration == 'HOURLY')
            if depth.lower() == 'max':
                element_obj = element_obj.order_by(Element.HeightDepth.asc()).first()
            else:
                element_obj = element_obj.order_by(Element.HeightDepth.desc()).first()
            session.expunge_all()
    return element_obj

def find_element(station_triplet, local=True, element_cd='STO', depth='min'):
    if local:
        with session_scope() as session:
            element_obj = session.query(Element).filter(Element.StationTriplet == station_triplet). \
                filter(Element.ElementCd==element_cd). \
                filter(Element.Duration=='HOURLY').first()
            session.expunge_all()
    return element_obj


def get_element_list():
    return client.service.getElements()

def get_element_bystationtriplet(station_triplet, local=True, filters=('duration', 'elementcd')):
    if local:
        with session_scope() as session:
            station_elements = session.query(Element). \
                filter(Element.StationTriplet == station_triplet).all()
            session.expunge_all()
    else:
        station_element_meta_list = client.service.getStationElements(station_triplet)
        station_elements = [construct_element(element_meta) for element_meta in station_element_meta_list]
    if filters:
        station_elements = filter_elements(station_elements, filters)
    return station_elements

def cast_element_request(element, local=True):
    """
    See details for data request: http://www.wcc.nrcs.usda.gov/web_service/AWDB_Web_Service_Reference.htm#getHourlyData
    :param element: instance of Element class
    :return: getHourlyData Request dictionary
    """
    assert isinstance(element, Element)
    request = {}
    data_request_fmt = copy.deepcopy(DATA_REQUEST_FMT)
    data_request_fmt.pop('stationTriplets')
    request['stationTriplets'] = element.StationTriplet
    for key in ['beginDate', 'endDate']:
        if key == 'endDate':
                request[key] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        elif key == 'beginDate':
            if element.LocalEndDate:
                request[key] = element.LocalEndDate.strftime('%Y-%m-%d %H:%M:%S')
            else:
                request[key] = element.BeginDate.strftime('%Y-%m-%d %H:%M:%S')
    for key, fun in data_request_fmt.items():
        request[key] = fun(getattr(element, CamelCase(key), None))
    return request

def parse_element_meta(element_meta):
    meta_dict = dict(element_meta)
    try:
        height_depth = meta_dict.pop('heightDepth')
        assert height_depth.unitCd == "in"
        meta_dict['HeightDepth'] = height_depth.value
    except:
        meta_dict['HeightDepth'] = None
    meta = dict([(CamelCase(key), value) for key, value in meta_dict.items()])
    meta['ElementTriplet'] = '{StationTriplet}:{ElementCd}:{Duration}:{HeightDepth}'.format(**meta)
    meta['BeginDate'] = datetime.datetime.strptime(meta['BeginDate'], '%Y-%m-%d %H:%M:%S')
    meta['EndDate'] = datetime.datetime.strptime(meta['EndDate'], '%Y-%m-%d %H:%M:%S')
    return meta

def construct_element(element_meta):
    meta_dict = parse_element_meta(element_meta)
    return Element(**meta_dict)

def filter_elements(element_list, filters=('duration', 'elementcd')):
    # TODO add more filters
    for filter_ in filters:
        if filter_.lower() == 'duration':
            element_list = [element for element in element_list if element.Duration in duration_toload]
        if filter_.lower() == 'elementcd':
            element_list = [element for element in element_list if element.ElementCd in elementcd_toload]
    return element_list

def add_element(element):
    try:
        with session_scope() as session:
            session.merge(element)
            session.commit()
        print('Added element: {}'.format(element))
    except:
        print('Error merging element: {}'.format(element))

def add_elements(element_meta_list):
    for element in element_meta_list:
        info = (element.StationTriplet, element.ElementCd)
        add_element(element)


def add_element_inpool(element):
    print('Attempting update element {} ...'.format(element))

    print('Deleting element {} ...'.format(element))
    with get_connection() as conn:
        with conn.cursor() as cur:
            query = 'DELETE from element WHERE "ElementTriplet" like (%s)'
            cur.execute(query, (element.ElementTriplet,))
            conn.commit()
    print('Reading element {} ...'.format(element))
    with get_connection() as conn:
        with conn.cursor() as cur:
            query = ('INSERT into element values '
                     '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')
            tup = (element.BeginDate, element.DataPrecision,
                   element.DataSource, element.Duration, element.ElementCd,
                   element.EndDate, element.Ordinal, element.OriginalUnitCd,
                   element.StationTriplet, element.StoredUnitCd,
                   element.HeightDepth, element.ElementTriplet, element.LocalBeginDate,
                   element.LocalEndDate)
            cur.execute(query, tup)
            conn.commit()
    print('Updated element {} ...'.format(element))

def update_element_data_list_inpool(element_list):
    if element_list:
        pool = Pool(maxtasksperchild=1)
        args = [(e, True) for e in element_list]
        pool.starmap(update_element_data, args)
        pool.terminate()
        pool.join()

def update_element_data_list_series(element_list):
    if element_list:
        for element in element_list:
            update_element_data(element, in_pool=False)

def update_element_data(element, in_pool=False):
    """

    :rtype : None
    """
    request = cast_element_request(element)
    print('REQUESTING: {}'.format(request))
    data_result = get_data_hourly(request)
    print('Done ...')
    assert len(data_result) == 1
    data_result = data_result[0]
    if 'values' in data_result:
        begin_date, end_date = update_data(element, data_result)
        element.LocalBeginDate = datetime.datetime.strptime(begin_date, '%Y-%m-%d')
        element.LocalEndDate = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        print('Updating element table ...')
        #add_element(element)
        if in_pool:
            add_element_inpool(element)
        else:
            add_element(element)
    else:
        print('No new data found ...')



'''
Data Functions
~~~~~~~~~~~~~~
'''

def delete_data(element_triplet, start_date, end_date):
    """

    :param element_triplet: station element triplet of data to delete
    :param start_date: start of data to delete
    :param end_date: end of data to delete
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            query = ('DELETE FROM data WHERE '
                     '"ElementTriplet" like (%s) and '
                     '"DateTime" >= (%s) and '
                     '"DateTime" <= (%s)')
            cur.execute(query, (element_triplet, start_date, end_date))
            conn.commit()

def parse_data_response_hourly(hourly_data, num_stations=1):
    """

    :param hourly_data:
    :param num_stations:
    :return:
    """
    data_out = []
    for nStation in range(num_stations):
        data = {}
        start_datetime, end_datetime, station_triplet, (_, data_list) = hourly_data[nStation]

        data['LocalStartDateTime'] = start_datetime[1]
        data['LocalEndDateTime'] = end_datetime[1]
        data['StationTriplet'] = station_triplet[1]
        data['DataList'] = [dict([(CamelCase(key[0]), key[1]) for key in D]) for D in data_list]
        data_out.append(data)
    return data_out

def parse_data_values(element, data_result):
    data_list = []
    for data_row in data_result.values:
        data_list.append((element.ElementTriplet, element.StationTriplet,
                          data_row.dateTime, data_row.flag, data_row.value))
    return list(set(data_list))

def parse_data_objects(data_list):
    parse_data_meta = lambda x: (x.DateTime, x.Value)
    date_time, value, flag = zip(*map(parse_data_meta, data_list))
    return date_time, np.array(value, dtype='double'), np.array(flag, dtype='str')

def add_data(data_list):
    with get_connection() as conn:
        with conn.cursor() as cur:
            sql = 'INSERT INTO data VALUES (%s, %s, %s, %s, %s)'
            cur.executemany(sql, data_list)
            conn.commit()

    '''
        Data Updates
        ~~~~~~~~~~~~
        Returns hourly data from NRCS, stores in Postgres tables
        NOTE: Set THREADS variable to control number threads used
    '''

def get_data_hourly(request):
    request['beginDate'] = request['beginDate'] + ' 00:00:00'
    request['endDate'] = request['endDate'] + ' 00:00:00'
    return client.service.getHourlyData(**request)

def get_data_byelement(element_triplet):
    return _get_object_filter(Data, {'ElementTriplet': element_triplet, 'Flag': 'V'})

def update_data_bystations(station_list):
    for station_triplet in station_list:
        try:
            print('Updating data from station {}'.format(station_triplet))
            element_list = get_element_bystationtriplet(station_triplet, local=True, filters=['duration', 'elementcd'])
            #update_element_data_list_inpool(element_list)
            update_element_data_list_series(element_list)
        except Exception as e:
            print(e)
            Exception('Error updating station {}:'.format(station_triplet))

def update_data_all():
    station_list = get_station_list(local=True)
    update_data_bystations(station_list)

def update_data(element, data_result):
    assert element.StationTriplet == data_result.stationTriplet
    print('DELETING from data ...')
    delete_data(element.ElementTriplet, data_result.beginDate, data_result.endDate)
    data_list = parse_data_values(element, data_result)
    print('ADDING into data {} values ...'.format(len(data_list)))
    add_data(data_list)
    return data_result.beginDate, data_result.endDate


'''
OO Objects
~~~~~~~~~~
'''

def parse_station_meta(meta):
    """

    :param meta: Station metadata from NWCC webservice
    :return: Python dictionary formatted for Station obj.
    """
    return dict((CamelCase(key), value) for key, value in dict(meta).items())

_dt_parser = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %M:%H:%S')
_Station_lookup = {'BeginDate': _dt_parser, 'CountyName': str, 'Elevation': int, 'EndDate': _dt_parser,
                   'FipsCountryCd': str, 'FipsCountyCd': str, 'FipsStateNumber': str, 'Huc': int,
                   'Hud': int, 'Latitude': float, 'Longitude': float, 'Name': str,
                   'StationDataTimeZone': float, 'StationTriplet': str}

def construct_station(station_meta):
    meta_dict = parse_station_meta(station_meta)
    for k, fun in _Station_lookup.items():
        if k in meta_dict:
            meta_dict[k] = fun(meta_dict[k])
    return Station(**meta_dict)


class Station(Base):
    '''    
    Snotel/SCAN automated weather station data abstraction.
    Called with station triplet, holds list of elements at that station
    '''
    __tablename__ = 'station'
    BeginDate = Column(types.DateTime)  # 1948-10-01 00:00:00"
    CountyName = Column(types.String)  # WESTON
    Elevation = Column(types.Integer)  # 4780.0
    EndDate = Column(types.DateTime)  # 2100-01-01 00:00:00
    FipsCountryCd = Column(types.String)  # US
    FipsCountyCd = Column(types.String)  # 045
    FipsStateNumber = Column(types.String)  # 56
    Huc = Column(types.BigInteger)  # 101201030505
    Hud = Column(types.BigInteger)  # 10120103
    Latitude = Column(types.Float)  # 43.93333
    Longitude = Column(types.Float)  # -104.76667
    Name = Column(types.String)  # UPTON 13 SW
    StationDataTimeZone = Column(types.Float)  # -7.0
    StationTriplet = Column(types.String, primary_key=True)  # 9207:WY:COOP

    _how = 'median'
    _freq = 'D'

    def __init__(self, freq='D', how='median', filter=True, *args, **kwargs):
        for arg in args:
            for key in arg:
                setattr(self, key, arg[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.freq = freq
        self.how = how
        self.filter = filter

    def __str__(self):
        return '<Snotel Station: {Name} {StationTriplet}>'.format(**self.__dict__)

    def __repr__(self):
        return self.__str__()

    def load(self):
        kwargs = dict(frequency=self.freq, apply_filter=self.filter, how=self.how)
        self.data_frame = self.get_data_frame(**kwargs)

    # Properties
    # ==========
    @property
    def lat(self):
        return self.Latitude

    @property
    def lon(self):
        return self.Longitude

    @property
    def station_id(self):
        return self.StationTriplet

    @property
    def freq(self):
        return self._freq

    @freq.setter
    def freq(self, freq_char):
        self._freq = freq_char

    @property
    def how(self):
        return self._how

    @how.setter
    def how(self, val):
        self._how = val



    # Data Frame
    # ==========
    @property
    def data_frame(self):
        if not hasattr(self, '_data_frame'):
            self._data_frame = self.get_data_frame(frequency=self.freq, apply_filter=True, how=self.how)
        return self._data_frame

    @data_frame.setter
    def data_frame(self, data_frame):
        self._data_frame = data_frame

    def get_data_frame(self, frequency='D', apply_filter=True, how='median'):
        raw_data_frame = self.get_raw_data_frame()
        if apply_filter:
            for col in raw_data_frame.columns:
                med_, std_ = raw_data_frame[col].median(), raw_data_frame[col].std()
                #mask_ = raw_data_frame[col].abs() > pd.rolling_std(raw_data_frame[col], window=48) * 3
                raw_data_frame[col] = raw_data_frame[col].mask(raw_data_frame[col].abs() > med_ + 3 * std_)
        raw_data_frame = raw_data_frame.apply(lambda x: x.tz_localize(pytz.FixedOffset(self.StationDataTimeZone)))
        return raw_data_frame

    def get_raw_data_frame(self):
        data_frame_list = dict([(element.ElementTriplet, element.series) for element in self.element_list])
        return pd.DataFrame(data_frame_list)

    @property
    def soil_day(self):
        if not hasattr(self, '_soil_day'):
            try:
                element = find_element_bydepth(self.station_id, 'STO', depth='MIN')
                element_triplet = element.ElementTriplet
            except:
                print('database not found -- guessing depth')
                try:
                    element_triplet = [key for key in self.data_frame.keys() if key.endswith('STO:HOURLY:-2.0')][0]
                except:
                    element_triplet = None
            if element_triplet:
                self._soil_day = self.data_frame[element_triplet].between_time(start_time='12:00', end_time='23:59').resample('D')
            else:
                self._soil_day = None

        return self._soil_day

    @property
    def soil_night(self):
        if not hasattr(self, '_soil_night'):
            try:
                element = find_element_bydepth(self.station_id, 'STO', depth='MIN')
                element_triplet = element.ElementTriplet
            except:
                print('database not found -- guessing depth')
                try:
                    element_triplet = [key for key in self.data_frame.keys() if key.endswith('STO:HOURLY:-2.0')][0]
                except:
                    element_triplet = None
            if element_triplet:
                self._soil_night = self.data_frame[element_triplet].between_time(start_time='00:00', end_time='12:00').resample('D')
            else:
                self._soil_night = None

        return self._soil_night

    @property
    def air_day(self):
        if not hasattr(self, '_air_day'):
            try:
                element_triplet = [key for key in self.data_frame.keys() if key.endswith('TOBS:HOURLY:None')][0]
                #element = find_element_bydepth(self.station_id, 'TOBS', depth='MIN')
            except:
                element_triplet = None
            if element_triplet:
                self._air_day = self.data_frame[element_triplet].between_time(start_time='12:00', end_time='23:59').resample('D')
            else:
                self._air_day = None

        return self._air_day

    @property
    def air_night(self):
        if not hasattr(self, '_air_night'):
            try:
                element_triplet = [key for key in self.data_frame.keys() if key.endswith('TOBS:HOURLY:None')][0]
                #element = find_element_bydepth(self.station_id, 'TOBS', depth='MIN')
            except:
                element_triplet = None
            if element_triplet:
                #element = find_element_bydepth(self.station_id, 'TOBS', depth='MIN')
                self._air_night= self.data_frame[element_triplet].between_time(start_time='00:00', end_time='12:00').resample('D')
            else:
                self._air_night = None

        return self._air_night

    @property
    def sd_day(self):
        if not hasattr(self, '_sd_day'):
            try:
                element_triplet = [k for k in self.data_frame.keys() if 'SNWD' in k][0]
                self._sd_day = self.data_frame[element_triplet].resample('D', how='median')
            except:
                self._sd_day = None
        return self._sd_day
    


    @property
    def sm_day(self):
        if not hasattr(self, '_sm_day'):
            element = find_element_bydepth(self.station_id, 'SMS', depth='MIN')
            if element:
                self._sm_day = self.data_frame[element.ElementTriplet].between_time(start_time='12:00', end_time='23:59').resample('D')
            else:
                self._sm_day = None

        return self._sm_day

    @property
    def sm_night(self):
        if not hasattr(self, '_sm_night'):
            element = find_element_bydepth(self.station_id, 'SMS', depth='MIN')
            if element:
                self._sm_night= self.data_frame[element.ElementTriplet].between_time(start_time='00:00', end_time='12:00').resample('D')
            else:
                self._sm_night = None

        return self._sm_night

    @property
    def element_list(self):
        if not hasattr(self, '_element_list'):
            element_list = get_element_bystationtriplet(self.StationTriplet)
            if element_list:
                self._element_list = filter_elements(element_list)
            else:
                return None
        return self._element_list

    '''
    Combine - Validation Data
    '''

class Element(Base):
    """ SNOTEL data element  """

    __tablename__ = 'element'
    BeginDate = Column(types.DateTime)  # "1965-02-01 00:00:00"
    DataPrecision = Column(types.Integer)  # = 0
    DataSource = Column(types.String)  # = "OBSERVED"
    Duration = Column(types.String)  # = "SEMIMONTHLY"
    ElementCd = Column(types.String)  # = "SNWD"
    EndDate = Column(types.DateTime)  # = "1992-06-01 00:00:00"
    Ordinal = Column(types.Integer)  # = 1
    OriginalUnitCd = Column(types.String)  # = "in"
    StationTriplet = Column(types.String)  # = "10G01:WY:SNOW"
    StoredUnitCd = Column(types.String)  # = "in"
    HeightDepth = Column(types.Float)  #
    ElementTriplet = Column(types.String, primary_key=True)  #
    LocalBeginDate = Column(types.DateTime)
    LocalEndDate = Column(types.DateTime)


    def __init__(self, *args, **kwargs):
        for arg in args:
            for key in arg:
                setattr(self, key, arg[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])


    def __repr__(self):
        return '<SNOTELELEMENT: {ElementTriplet} {BeginDate}:{EndDate}>'.format(**self.__dict__)

    @property
    def data_frame(self):
        if not hasattr(self, '_data_frame'):
            self.set_dataframe()
        return self._data_frame[self.ElementTriplet]

    @property
    def series(self):
        if not hasattr(self, '_data_frame'):
            self.set_dataframe()
        return self._data_frame[self.ElementTriplet]

    def set_dataframe(self):
        print('LOADING DATA {}, '.format(self.ElementTriplet)),
        element_data = pd.read_sql(
            """ SELECT "DateTime","Value"  FROM "data"
                WHERE "ElementTriplet" LIKE '{}' AND
                "StationTriplet" LIKE '{}' AND
                "Flag" = 'V' """.format(self.ElementTriplet, self.StationTriplet), ee, index_col='DateTime')
        element_data.columns = [self.ElementTriplet]
        element_data = element_data.sort()
        self._data_frame = element_data
        print('DONE')

    @property
    def data_list(self):
        if not hasattr(self, '_data_list'):
            data_list = get_data_byelement(self.ElementTriplet)
            if data_list:
                self._data_list = data_list
            else:
                return None
        return self._data_list


class Data(Base):
    __tablename__ = 'data'
    ElementTriplet = Column(types.String, primary_key=True)
    StationTriplet = Column(types.String)
    DateTime = Column(types.DateTime, primary_key=True)
    Flag = Column(types.String(1))
    Value = Column(types.Float)

    def __init__(self, *args, **kwargs):
        for arg in args:
            for key in arg:
                setattr(self, key, arg[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def __repr__(self):
        return '<SNOTELDATA:{StationTriplet}:{ElementTriplet}>'.format(**self.__dict__)


'''
Collections
~~~~~~~~~~~
'''
class StaticStation(object):
    def __init__(self, station_triplet, update=False):
        self.station_triplet = station_triplet
        self.station = self.find_station(station_triplet)
        self.update = update
        if update:
            self.do_update()
        self.load()

    def do_update(self):
        update_station_data(self.station)
        self.freeze(self.station)

    def load(self):
        data_frame = self._get_dataframe()
        self.station.data_frame = data_frame

    def freeze(self):
        self._set_dataframe(self.StationTriplet, self.station.data_frame)

    def find_station(self, station_triplet):
        return get_station_bytriplet(station_triplet, local=True)

    def _get_dataframe(self):
        tablename = self._tablename_fromtriplet(self.station_triplet)
        try:
            return pd.read_sql(tablename, collection_engine, index_col='DateTime')
        except:
            return

    def _set_dataframe(self):
        tablename = self._tablename_fromtriplet(self.station_triplet)
        self.data_frame.to_sql(tablename, collection_engine, if_exists='replace')

    def _tablename_fromtriplet(self, triplet_string):
        return triplet_string.replace(':', '_')

class Collection(object):

    def __init__(self, update=False):
        # init station list
        for station_id in self.station_id_list:
            station_ = self.find_station(station_id)
            if station_:
                self.station_list.append(station_)
        self.update = update
        if update:
            self.do_update()
        self.load()

    def do_update(self):
        for station in self.station_list:
            if station:
                update_station_data(station)
                self.freeze(station)

    def freeze(self, station):
        self._set_dataframe(station.StationTriplet, station.data_frame)
        return None

    def load(self):
        if not self.update:
            for station in self.station_list:
                data_frame = self._get_dataframe(station.StationTriplet)
                station.data_frame = data_frame
        return None

    def find_station(self, station_triplet):
        return get_station_bytriplet(station_triplet, local=False)

    @property
    def station_id_list(self):
        if not hasattr(self, '_station_id_list'):
            station_id_list = []
            for station_id in station_id_list:
                try:
                    station_id = find_stationtriplet_byid(station_id, 'AK')
                    station_id_list.append(station_id)
                except Exception as e:
                    print(e)
            self._station_id_list = station_id_list
        return self._station_id_list

    @property
    def station_list(self):
        if not hasattr(self, '_station_list'):
            self._station_list = []
        return self._station_list

    def _get_dataframe(self, station_id):
        tablename = self._tablename_fromtriplet(station_id)
        try:
            return pd.read_sql(tablename, collection_engine, index_col='DateTime')
        except:
            return

    def _set_dataframe(self, station_id, data_frame):
        tablename = self._tablename_fromtriplet(station_id)
        data_frame.to_sql(tablename, collection_engine, if_exists='replace')

    def _tablename_fromtriplet(self, triplet_string):
        return triplet_string.replace(':', '_')

import pickle as pck
class AlaskaCollection(Collection):
    _state_abbr = 'AK'
    _station_id_list = [962, 958, 968, 2212, 957, 1177, 1175,
                         2210, 2211, 2065, 2081, 950, 963, 1094,
                         1089, 967, 2080, 1233]
    #_station_id_list = [962, 958]

    def __init__(self):
        print('no postgres - all local')
        self.update = False
        self._station_list = pck.load(open(os.path.join(_PATH, 'ak_col.pck'), 'r'))
        self.load()

    @property
    def state_abbr(self):
        return self._state_abbr

    def find_station(self, station_id):
        try:
            station_triplet,  = find_stationtriplet_byid(station_id, self.state_abbr)
            return get_station_bytriplet(station_triplet, local=False)
        except Exception as e:
            print(Exception('Station not found {}'.format(station_id)))
            return None

def main():
    if args.do_update:
        if args.object.lower() == 'data':
            update_data_all()
        elif args.object.lower() == 'stations':
            station_list = get_station_list(local=False)
            update_station_list(station_list)
        elif args.object.lower() == 'elements':
            station_list = get_station_list(local=True)
            update_stationlist_elements(station_list)
        elif args.object.lower() == 'alaska':
            station_list = get_station_list_alaska()
            update_data_bystations(station_list)


if __name__ == '__main__':

    # parse input args
    parser = argparse.ArgumentParser(description='Python SNOTEL package .')
    parser.add_argument('-o', '--object', dest='update', default='data',
                        help='Choose what to update [{data}, stations, elements]')
    parser.add_argument('-u', '--update', dest='update', action='store_true',
                        help='o snotel update.')
    global args
    args = parser.parse_args()
    #sys.exit(main())
    AlaskaCollection()