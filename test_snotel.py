import snotel

TEST_STATION = '1177:AK:SNTL'

def test_update_station(test_station_triplet):
    print('Testing update station ...')
    snotel.update_stations([test_station_triplet])
    print('Testing update station element ...')
    snotel.update_station_elements(([test_station_triplet]))
    print('Testing update station data ...')
    print('Testing one element from local...')
    element_list = snotel.get_station_element(test_station_triplet, local=True, filters=['duration', 'elementcd'])
    for element in element_list:
        snotel.update_element_data(element, in_pool=False)
    print('Testing one element from server...')
    element_list = snotel.get_station_element(test_station_triplet, local=True, filters=['duration', 'elementcd'])
    snotel.update_element_data(element_list[0], in_pool=False)
    print('Testing multiprocessing-pool ...')
    snotel.update_station_data([test_station_triplet])

def test_multiprocessing(station_triplet):
    print('Testing multiprocessing-pool ...')
    snotel.update_station_data([station_triplet])


def test_get_remotestation(station_triplet):
    print('Testing get station')
    snotel.get_station(station_triplet, local=False)


def main():
    #test_get_remotestation(TEST_STATION)
    test_update_station(TEST_STATION)

if __name__ == '__main__':
    main()
