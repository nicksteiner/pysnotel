def check_element_load(element_cd):
        return element_cd in elements_toload

ELEMENTS = {
        'TAVG':  ('AIR TEMPERATURE AVERAGE', 'degF', False),
        'TMAX':  ('AIR TEMPERATURE MAXIMUM', 'degF', False),
        'TMIN':  ('AIR TEMPERATURE MINIMUM', 'degF', False),
        'TOBS':  ('AIR TEMPERATURE OBSERVED', 'degF', True),
        'PRES':  ('BAROMETRIC PRESSURE', 'inch_Hg', False),
        'BATT':  ('BATTERY', 'volt', False),
        'BATV':  ('BATTERY AVERAGE', 'volt', False),
        'BATX':  ('BATTERY MAXIMUM', 'volt', False),
        'BATN':  ('BATTERY MINIMUM', 'volt', False),
        'ETIB':  ('BATTERY-ETI PRECIP GUAGE', 'volt', False),
        'COND':  ('CONDUCTIVITY', 'umho', True),
        'DPTP':  ('DEW POINT TEMPERATURE', 'degF', False),
        'DIAG':  ('DIAGNOSTICS', 'unitless', False),
        'SRDOX': ('DISCHARGE MANUAL/EXTERNAL ADJUSTED MEAN', 'cfs', False),
        'DISO':  ('DISSOLVED OXYGEN', 'mgram/l', False),
        'DISP':  ('DISSOLVED OXYGEN - PERCENT SATURATION', 'pct', False),
        'DIVD':  ('DIVERSION DISCHARGE OBSERVED MEAN', 'cfs', False),
        'DIV':   ('DIVERSION FLOW VOLUME OBSERVED', 'ac_ft', False),
        'ZDUM':  ('DUMMY LABEL', 'volt', False),
        'HFTV':  ('ENERGY GAIN OR LOSS FROM GROUND', 'watt/m2', False),
        'EVAP':  ('EVAPORATION', 'in', False),
        'FUEL':  ('FUEL MOISTURE', 'pct', False),
        'FMTMP': ('FUEL TEMPERATURE INTERNAL', 'degF', False),
        'VOLT':  ('GENERIC VOLTAGE', 'volt', False),
        'TGSV':  ('GROUND SURFACE INTERFACE TEMPERATURE AVERAGE', 'degF', False),
        'TGSX':  ('GROUND SURFACE INTERFACE TEMPERATURE MAXIMUM', 'degF', False),
        'TGSN':  ('GROUND SURFACE INTERFACE TEMPERATURE MINIMUM', 'degF', False),
        'TGSI':  ('GROUND SURFACE INTERFACE TEMPERATURE OBSERVED', 'degF', True),
        'JDAY':  ('JULIAN DATE', 'julian_day', False),
        'MXPN':  ('MAXIMUM', 'degF', False),
        'MNPN':  ('MINIMUM', 'degF', False),
        'NTRDV': ('NET SOLAR RADIATION AVERAGE', 'watt/m2', False),
        'NTRDX': ('NET SOLAR RADIATION MAXIMUM', 'watt/m2', False),
        'NTRDN': ('NET SOLAR RADIATION MINIMUM', 'watt/m2', False),
        'NTRDC': ('NET SOLAR RADIATION OBSERVED', 'watt/m2', True),
        'H2OPH': ('PH', 'unitless', False),
        'PARV':  ('PHOTOSYNTHETICALLY ACTIVE RADIATION (PAR) AVERAGE', 'micromole/m2/s', False),
        'PART':  ('PHOTOSYNTHETICALLY ACTIVE RADIATION (PAR) TOTAL', 'millimole/m2', False),
        'HOLD':  ('PLACE HOLDER', 'unitless', False),
        'PREC':  ('PRECIPITATION ACCUMULATION', 'in', False),
        'PRCP':  ('PRECIPITATION INCREMENT', 'in', False),
        'PRCPSA':('PRECIPITATION INCREMENT - SNOW-ADJ', 'in', False),
        'ETIL':  ('PULSE LINE MONITOR-ETI GUAGE', 'volt', False),
        'RDC':   ('REAL DIELECTRIC CONSTANT', 'unitless', True),
        'RHUM':  ('RELATIVE HUMIDITY', 'pct', False),
        'RHUMV': ('RELATIVE HUMIDITY AVERAGE', 'pct', False),
        'RHENC': ('RELATIVE HUMIDITY ENCLOSURE', 'pct', False),
        'RHUMX': ('RELATIVE HUMIDITY MAXIMUM', 'pct', False),
        'RHUMN': ('RELATIVE HUMIDITY MINIMUM', 'pct', False),
        'REST':  ('RESERVOIR STAGE', 'ft', False),
        'RESC':  ('RESERVOIR STORAGE VOLUME', 'ac_ft', False),
        'SRDOO': ('RIVER DISCHARGE OBSERVED MEAN', 'cfs', False),
        'RVST':  ('RIVER STAGE LEVEL', 'ft', False),
        'SAL':   ('SALINITY', 'gram/l', False),
        'SNWD':  ('SNOW DEPTH', 'in', True),
        'SNWDV': ('SNOW DEPTH AVERAGE', 'in', False),
        'SNWDX': ('SNOW DEPTH MAXIMUM', 'in', False),
        'SNWDN': ('SNOW DEPTH MINIMUM', 'in', False),
        'SNOW':  ('SNOW FALL', 'in', True),
        'WTEQ':  ('SNOW WATER EQUIVALENT', 'in', True),
        'WTEQV': ('SNOW WATER EQUIVALENT AVERAGE', 'in', False),
        'WTEQX': ('SNOW WATER EQUIVALENT MAXIMUM', 'in', False),
        'WTEQN': ('SNOW WATER EQUIVALENT MINIMUM', 'in', False),
        'SMOV':  ('SOIL MOISTURE BARS AVERAGE', 'bar', False),
        'SMOC':  ('SOIL MOISTURE BARS CURRENT', 'bar', False),
        'SMOX':  ('SOIL MOISTURE BARS MAXIMUM', 'bar', False),
        'SMON':  ('SOIL MOISTURE BARS MINIMUM', 'bar', False),
        'SMS':   ('SOIL MOISTURE PERCENT', 'pct', True),
        'SMV':   ('SOIL MOISTURE PERCENT AVERAGE', 'pct', False),
        'SMX':   ('SOIL MOISTURE PERCENT MAXIMUM', 'pct', False),
        'SMN':   ('SOIL MOISTURE PERCENT MINIMUM', 'pct', False),
        'STV':   ('SOIL TEMPERATURE AVERAGE', 'degF', False),
        'STX':   ('SOIL TEMPERATURE MAXIMUM', 'degF', False),
        'STN':   ('SOIL TEMPERATURE MINIMUM', 'degF', False),
        'STO':   ('SOIL TEMPERATURE OBSERVED', 'degF', True),
        'SRAD':  ('SOLAR RADIATION', 'watt/m2', True),
        'SRADV': ('SOLAR RADIATION AVERAGE', 'watt/m2', False),
        'SRADX': ('SOLAR RADIATION MAXIMUM', 'watt/m2', False),
        'SRADN': ('SOLAR RADIATION MINIMUM', 'watt/m2', False),
        'SRADT': ('SOLAR RADIATION TOTAL', 'watt/m2', False),
        'LRAD':  ('SOLAR RADIATION/LANGLEY', 'langley', False),
        'LRADX': ('SOLAR RADIATION/LANGLEY MAXIMUM', 'langley', False),
        'LRADT': ('SOLAR RADIATION/LANGLEY TOTAL', 'langley', False),
        'SRMV':  ('STREAM STAGE (GAUGE HEIGHT) AVERAGE', 'ft', False),
        'SRMX':  ('STREAM STAGE (GAUGE HEIGHT) MAXIMUM', 'ft', False),
        'SRMN':  ('STREAM STAGE (GAUGE HEIGHT) MINIMUM', 'ft', False),
        'SRMO':  ('STREAM STAGE (GAUGE HEIGHT) OBSERVED', 'ft', False),
        'SRVO':  ('STREAM VOLUME, ADJUSTED', 'ac_ft', False),
        'SRVOX': ('STREAM VOLUME, ADJUSTED EXTERNAL', 'ac_ft', False),
        'SRVOO': ('STREAM VOLUME, OBSERVED', 'ac_ft', False),
        'OI':    ('TELECONNECTION INDEX', 'unitless', False),
        'CDD':   ('TEMPERATURE, DEGREE DAYS OF COOLING', 'deg_dayF', False),
        'GDD':   ('TEMPERATURE, DEGREE DAYS OF GROWING', 'deg_dayF', False),
        'HDD':   ('TEMPERATURE, DEGREE DAYS OF HEATING', 'deg_dayF', False),
        'TURB':  ('TURBIDITY', 'ntu', False),
        'ZUNK':  ('UNKNOWN LABEL', 'unitless', False),
        'RESA':  ('USABLE LAKE STORAGE VOLUME', 'ac_ft', False),
        'PVPV':  ('VAPOR PRESSURE - PARTIAL', 'kPa', False),
        'SVPV':  ('VAPOR PRESSURE - SATURATED', 'kPa', False),
        'WLEVV': ('WATER LEVEL AVERAGE', 'in', False),
        'WLEVX': ('WATER LEVEL MAXIMUM', 'in', False),
        'WLEVN': ('WATER LEVEL MINIMUM', 'in', False),
        'WLEV':  ('WATER LEVEL OBSERVED', 'in', False),
        'WTEMP': ('WATER TEMPERATURE', 'degF', False),
        'WTAVG': ('WATER TEMPERATURE AVERAGE', 'degF', False),
        'WTMAX': ('WATER TEMPERATURE MAXIMUM', 'degF', False),
        'WTMIN': ('WATER TEMPERATURE MINIMUM', 'degF', False),
        'WELL':  ('WELL DEPTH', 'ft', False),
        'WDIRV': ('WIND DIRECTION AVERAGE', 'degree', False),
        'WDIR':  ('WIND DIRECTION OBSERVED', 'degree', False),
        'WDIRZ': ('WIND DIRECTION STANDARD DEVIATION', 'degree', False),
        'WDMVV': ('WIND MOVEMENT AVERAGE', 'mile', False),
        'WDMVX': ('WIND MOVEMENT MAXIMUM', 'mile', False),
        'WDMVN': ('WIND MOVEMENT MINIMUM', 'mile', False),
        'WDMV':  ('WIND MOVEMENT OBSERVED', 'mile', False),
        'WDMVT': ('WIND MOVEMENT TOTAL', 'mile', False),
        'WSPDV': ('WIND SPEED AVERAGE', 'mph', False),
        'WSPDX': ('WIND SPEED MAXIMUM', 'mph', False),
        'WSPDN': ('WIND SPEED MINIMUM', 'mph', False),
        'WSPD':  ('WIND SPEED OBSERVED', 'mph', False),
        }
elementcd_toload = [name for name, (_,_,load) in ELEMENTS.iteritems() if load]
duration_toload = ['HOURLY']