import sys
from snotel import snotel
import pytest
sys.path.append('')
sys.stdout.flush()


TEST_STATION_TRIPLET = '2213:AK:SCAN'

def test_init_database():
    print('Database init if not there.')

def test_update_station():
    print('Testing update station ...')
    snotel.update_station_list([TEST_STATION_TRIPLET])

def test_update_elements():
    print('Testing update station data ...')
    print('Testing one element from local...')
    element_list = snotel.get_element_bystationtriplet(TEST_STATION_TRIPLET, local=False, filters=['duration', 'elementcd'])
    snotel.add_elements(element_list)

def test_update_element_data():
    print('Testing one element from server...')
    element_list = snotel.get_element_bystationtriplet(TEST_STATION_TRIPLET, local=True, filters=['duration', 'elementcd'])
    snotel.update_element_data(element_list[0], in_pool=False)

def test_update_station_element_data_pool():
    print('Testing one element from server...')
    element_list = snotel.get_station_element(TEST_STATION_TRIPLET, local=True, filters=['duration', 'elementcd'])
    snotel.update_element_data(element_list[0], in_pool=True)


def test_get_remotestation():
    print('Testing get station')
    snotel.get_station(TEST_STATION_TRIPLET, local=False)
