import sys
from snotel import snotel
import pytest

sys.stdout.flush()


TEST_STATION_TRIPLET = '1177:AK:SNTL'

def test_init_database():
    print('Database init if not there.')



def test_update_station():
    print('Testing update station ...')
    snotel.update_station_list([TEST_STATION_TRIPLET])
    print('Testing update station element ...')
    snotel.update_station_elements(([TEST_STATION_TRIPLET]))
    print('Testing update station data ...')
    print('Testing one element from local...')
    element_list = snotel.get_station_element(TEST_STATION_TRIPLET, local=True, filters=['duration', 'elementcd'])
    for element in element_list:
        snotel.update_element_data(element, in_pool=False)
    print('Testing one element from server...')
    element_list = snotel.get_station_element(TEST_STATION_TRIPLET, local=True, filters=['duration', 'elementcd'])
    snotel.update_element_data(element_list[0], in_pool=False)
    print('Testing multiprocessing-pool ...')
    snotel.update_station_data([TEST_STATION_TRIPLET])

def test_multiprocessing():
    print('Testing multiprocessing-pool ...')
    snotel.update_station_data([TEST_STATION_TRIPLET])


def test_get_remotestation():
    print('Testing get station')
    snotel.get_station(TEST_STATION_TRIPLET, local=False)
