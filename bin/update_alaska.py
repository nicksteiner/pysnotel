import sys
import pytest

sys.path.append('/tmp/pysnotel_0')
sys.stdout.flush()
from snotel import snotel
FILTERS = ['duration', 'elementcd']

def main():
    print('Getting Stations')
    snotel_list = snotel.get_station_list(local=False)
    snotel_ak = [id_ for id_ in snotel_list if ":AK:" in id_]
    # update stations
    snotel.update_station_list(snotel_ak) # DONE
    # update elements
    for trip_ in snotel_ak:
        print('Updating Station: {}'.format(trip_))
        element_list = snotel.get_element_bystationtriplet(trip_, local=True, filters=FILTERS)
        snotel.update_element_data_list_inpool(element_list)
        #for el in element_list:
        #    snotel.update_element_data(el)
        #    snotel.add_elements(element_list)


if __name__ == '__main__':
    main()