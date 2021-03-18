import sys
from snotel import snotel
import pytest

sys.path.append('')
sys.stdout.flush()

TEST_STATION_TRIPLET = '2213:AK:SCAN'
FILTERS = ['duration', 'elementcd']


def test_init_database():
    print('Database init if not there.')


def test_update_station():
    print('Testing update station ...')
    snotel.update_station_list([TEST_STATION_TRIPLET])


def test_update_elements():
    print('Testing update station data ...')
    print('Testing one element from local...')
    element_list = snotel.get_element_bystationtriplet(TEST_STATION_TRIPLET, local=False, filters=FILTERS)
    snotel.add_elements(element_list)


def test_update_element_data():
    print('Testing one element from server...')
    element_list = snotel.get_element_bystationtriplet(TEST_STATION_TRIPLET, local=True, filters=FILTERS)
    el_ = [el for el in element_list if el.ElementCd == 'TOBS'][0]
    # el_ = [el for el in element_list if el.ElementCd == 'STO'][-1]
    snotel.update_element_data(el_, in_pool=False, data_format='par')


def test_set_element_data():
    print('Testing one element from server...')
    element_list = snotel.get_element_bystationtriplet(TEST_STATION_TRIPLET, local=True, filters=FILTERS)
    el_ = [el for el in element_list if el.ElementCd == 'TOBS'][0]
    # el_ = [el for el in element_list if el.ElementCd == 'STO'][-1]
    el_.set_dataframe(data_format='par')


def test_get_station_data():
    print('Testing station data from files ...')
    station = snotel.get_station_bytriplet(TEST_STATION_TRIPLET)
    print(station.air_night.head())


def test_update_station_element_data_pool():
    print('Testing one element from server...')
    element_list = snotel.get_element_bystationtriplet(TEST_STATION_TRIPLET, local=False, filters=FILTERS)
    # el_ = [el for el in element_list if el.ElementCd == 'TOBS'][0]
    snotel.update_element_data(element_list[0], in_pool=True, data_format='sql')


def test_get_remotestation():
    print('Testing get station')
    snotel.get_element_bystationtriplet(TEST_STATION_TRIPLET, local=False)

