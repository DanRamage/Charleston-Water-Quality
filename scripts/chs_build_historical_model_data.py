import sys
sys.path.append('../commonfiles/python')

import logging.config

from datetime import datetime, timedelta
from pytz import timezone
from shapely.geometry import Polygon
import logging.config
import optparse
import ConfigParser
from collections import OrderedDict

from wqHistoricalData import wq_data
from wqHistoricalData import tide_data_file_ex,station_geometry,sampling_sites, wq_defines, geometry_list
from NOAATideData import noaaTideData
from wq_sites import wq_sample_sites
from wqDatabase import wqDB
from chs_get_historical_data import parse_sheet_data
"""
florida_wq_data
Class is responsible for retrieving the data used for the sample sites models.
"""
class chs_wq_historical_data(wq_data):
  """
  Function: __init__
  Purpose: Initializes the class.
  Parameters:
    boundaries - The boundaries for the NEXRAD data the site falls within, this is required.
    xenia_database_name - The full file path to the xenia database that houses the NEXRAD and other
      data we use in the models. This is required.
  """
  def __init__(self, **kwargs):
    wq_data.__init__(self, **kwargs)

    self.site = None
    #The main station we retrieve the values from.
    self.tide_station =  None
    #These are the settings to correct the tide for the subordinate station.
    self.tide_offset_settings =  None
    self.tide_data_obj = None

    if self.logger:
      self.logger.debug("Connection to xenia db: %s" % (kwargs['xenia_database_name']))
    self.xenia_db = wqDB(kwargs['xenia_database_name'], type(self).__name__)

  """
  def __del__(self):
    if self.logger:
      self.logger.debug("Closing connection to xenia db")
    self.xenia_db.DB.close()

    if self.logger:
      self.logger.debug("Closing connection to thredds endpoint.")
    self.ncObj.close()

    if self.logger:
      self.logger.debug("Closing connection to hycom endpoint.")
    self.hycom_model.close()
  """

  def reset(self, **kwargs):
    self.site = kwargs['site']
    #The main station we retrieve the values from.
    self.tide_station = kwargs['tide_station']
    #These are the settings to correct the tide for the subordinate station.
    self.tide_offset_settings = kwargs['tide_offset_params']

    self.tide_data_obj = None
    if 'tide_data_obj' in kwargs and kwargs['tide_data_obj'] is not None:
      self.tide_data_obj = kwargs['tide_data_obj']

  """
  Function: query_data
  Purpose: Retrieves all the data used in the modelling project.
  Parameters:
    start_data - Datetime object representing the starting date to query data for.
    end_date - Datetime object representing the ending date to query data for.
    wq_tests_data - A OrderedDict object where the retrieved data is store.
  Return:
    None
  """
  def query_data(self, start_date, end_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Site: %s start query data for datetime: %s" % (self.site.name, start_date))

    self.initialize_return_data(wq_tests_data)

    self.get_tide_data(start_date, wq_tests_data)

    if self.logger:
      self.logger.debug("Site: %s Finished query data for datetime: %s" % (self.site.name, start_date))

  """
  Function: initialize_return_data
  Purpose: INitialize our ordered dict with the data variables and assign a NO_DATA
    initial value.
  Parameters:
    wq_tests_data - An OrderedDict that is initialized.
  Return:
    None
  """
  def initialize_return_data(self, wq_tests_data):
    if self.logger:
      self.logger.debug("Creating and initializing data dict.")
    #Build variables for the base tide station.
    var_name = 'tide_range_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_stage_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA

    #Build variables for the subordinate tide station.
    var_name = 'tide_range_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA

    for boundary in self.site.contained_by:
      if len(boundary.name):
        for prev_hours in range(24, 192, 24):
          clean_var_boundary_name = boundary.name.lower().replace(' ', '_')
          var_name = '%s_nexrad_summary_%d' % (clean_var_boundary_name, prev_hours)
          wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_dry_days_count' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_rainfall_intesity' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_total_1_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_2_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_3_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA


    if self.logger:
      self.logger.debug("Finished creating and initializing data dict.")

    return

  def get_tide_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    use_web_service = True
    if self.tide_data_obj is not None:
      use_web_service = False
      date_key = start_date.strftime('%Y-%m-%dT%H:%M:%S')
      if date_key in self.tide_data_obj:
        tide_rec = self.tide_data_obj[date_key]
        wq_tests_data['tide_range_%s' % (self.tide_station)] = tide_rec['range']
        wq_tests_data['tide_hi_%s' % (self.tide_station)] = tide_rec['hi']
        wq_tests_data['tide_lo_%s' % (self.tide_station)] = tide_rec['lo']

        try:
          #Get previous 24 hours.
          tide_start_time = (start_date - timedelta(hours=24))
          tide_end_time = start_date

          tide = noaaTideData(use_raw=True, logger=self.logger)

          tide_stage = tide.get_tide_stage(begin_date = tide_start_time,
                             end_date = tide_end_time,
                             station=self.tide_station,
                             datum='MLLW',
                             units='feet',
                             time_zone='GMT')
          wq_tests_data['tide_stage_%s' % (self.tide_station)] = tide_stage

        except Exception,e:
          if self.logger:
            self.logger.exception(e)

      #THe web service is unreliable, so if we were using the history csv file, it may still be missing
      #a record, if so, let's try to look it up on the web.
      else:
        use_web_service = True
    if self.tide_data_obj is None or use_web_service:
      #Get previous 24 hours.
      tide_start_time = (start_date - timedelta(hours=24))
      tide_end_time = start_date

      tide = noaaTideData(use_raw=True, logger=self.logger)
      #tide = noaaTideDataExt(use_raw=True, logger=self.logger)
      #Date/Time format for the NOAA is YYYYMMDD

      try:

        tide_data = tide.calcTideRange(beginDate = tide_start_time,
                           endDate = tide_end_time,
                           station=self.tide_station,
                           datum='MLLW',
                           units='feet',
                           timezone='GMT',
                           smoothData=False)
        """
        tide_data = tide.calcTideRangeExt(beginDate = tide_start_time,
                           endDate = tide_end_time,
                           station=self.tide_station,
                           datum='MLLW',
                           units='feet',
                           timezone='GMT',
                           smoothData=False)
        """
      except Exception,e:
        if self.logger:
          self.logger.exception(e)
      else:
        if tide_data and tide_data['HH'] is not None and tide_data['LL'] is not None:
          try:
            range = tide_data['HH']['value'] - tide_data['LL']['value']
          except TypeError, e:
            if self.logger:
              self.logger.exception(e)
          else:
            #Save tide station values.
            wq_tests_data['tide_range_%s' % (self.tide_station)] = range
            wq_tests_data['tide_hi_%s' % (self.tide_station)] = tide_data['HH']['value']
            wq_tests_data['tide_lo_%s' % (self.tide_station)] = tide_data['LL']['value']
            wq_tests_data['tide_stage_%s' % (self.tide_station)] = tide_data['tide_stage']
        else:
          if self.logger:
            self.logger.error("Tide data for station: %s date: %s not available or only partial." % (self.tide_station, start_date))

    #Save subordinate station values
    if wq_tests_data['tide_hi_%s'%(self.tide_station)] != wq_defines.NO_DATA:
      offset_hi = wq_tests_data['tide_hi_%s'%(self.tide_station)] * self.tide_offset_settings['hi_tide_height_offset']
      offset_lo = wq_tests_data['tide_lo_%s'%(self.tide_station)] * self.tide_offset_settings['lo_tide_height_offset']

      tide_station = self.tide_offset_settings['tide_station']
      wq_tests_data['tide_range_%s' % (tide_station)] = offset_hi - offset_lo
      wq_tests_data['tide_hi_%s' % (tide_station)] = offset_hi
      wq_tests_data['tide_lo_%s' % (tide_station)] = offset_lo

    if self.logger:
      self.logger.debug("Finished retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    return


def create_historical_summary(**kwargs):
  logger = logging.getLogger('create_historical_summary_logger')

  config_file = kwargs['config_file']
  boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
  sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
  wq_historical_db = config_file.get('database', 'name')

  #Load the sample site information. Has name, location and the boundaries that contain the site.
  wq_sites = wq_sample_sites()
  wq_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

  #If we provide a tide file that has the historical data, we'll load it instead of using
  #the SOAP webservice.
  tide_file = None
  if kwargs['tide_data_file'] is not None and len(kwargs['tide_data_file']):
    tide_file = tide_data_file_ex()
    tide_file.open(kwargs['tide_data_file'])


    #Dates in the spreadsheet are stored in EST. WE want to work internally in UTC.
    eastern = timezone('US/Eastern')

    output_keys = ['station_name', 'sample_date', 'enterococcus_value', 'enterococcus_code', 'autonumber', 'County']

    sites_not_found = []
    current_site = None
    processed_sites = []
    #stop_date = eastern.localize(datetime.strptime('2014-01-29 00:00:00', '%Y-%m-%d %H:%M:%S'))
    #stop_date = stop_date.astimezone(timezone('UTC'))
    try:
      chs_wq_data = chs_wq_historical_data(xenia_database_name=wq_historical_db,
                                    use_logger=True)
      chs_results = parse_sheet_data(kwargs['water_keepers_historical_data'], wq_sites)
    except Exception, e:
      if logger:
        logger.exception(e)
    else:
      # Build list of all dates.
      water_keeper_dates = []
      for site_name in chs_results:
        logger.debug("Site: %s getting dates" % (site_name))
        site_data = chs_results[site_name]
        for rec in site_data:
          if rec['sample_date'] not in water_keeper_dates:
            logger.debug("Site: %s adding date: %s" % (site_name, rec['sample_date']))
            water_keeper_dates.append(rec['sample_date'])

            site = wq_sites.get_site(site_name)
            try:
              #Get the station specific tide stations
              tide_station = config_file.get(site.name, 'tide_station')
              offset_tide_station = config_file.get(site.name, 'offset_tide_station')
              tide_offset_settings = {
                'tide_station': config_file.get(offset_tide_station, 'station_id'),
                'hi_tide_time_offset': config_file.getint(offset_tide_station, 'hi_tide_time_offset'),
                'lo_tide_time_offset': config_file.getint(offset_tide_station, 'lo_tide_time_offset'),
                'hi_tide_height_offset': config_file.getfloat(offset_tide_station, 'hi_tide_height_offset'),
                'lo_tide_height_offset': config_file.getfloat(offset_tide_station, 'lo_tide_height_offset')
              }

            except ConfigParser.Error, e:
              if logger:
                logger.exception(e)

          chs_wq_data.reset(site=site,
                            tide_station=tide_station,
                            tide_offset_params=tide_offset_settings,
                            tide_data_obj=tide_file)

          sample_site_filename = os.join(kwargs['summary_out_directory'], site)
          write_header = True
          try:
            if logger:
              logger.debug("Opening sample site history file: %s" % (sample_site_filename))
            site_data_file = open(sample_site_filename, 'w')
          except IOError, e:
            if logger:
              logger.exception(e)
            raise e
        if logger:
          logger.debug("Start building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (row['SPLocation'], wq_utc_date, wq_date))
        """  
        site_data = OrderedDict([('autonumber', row['autonumber']),
                                 ('station_name',row['SPLocation']),
                                 ('sample_datetime', wq_date.strftime("%Y-%m-%d %H:%M:%S")),
                                 ('sample_datetime_utc', wq_utc_date.strftime("%Y-%m-%d %H:%M:%S")),
                                 ('County', row['County']),
                                 ('enterococcus_value', row['enterococcus']),
                                 ('enterococcus_code', row['enterococcus_code'])])
        """
        try:
          wq_data.query_data(wq_utc_date, wq_utc_date, site_data)
        except Exception,e:
          if logger:
            logger.exception(e)
          sys.exit(-1)
        #wq_data_obj.append(site_data)
        header_buf = []
        data = []
        for key in site_data:
          if write_header:
            header_buf.append(key)
          if site_data[key] != wq_defines.NO_DATA:
            data.append(str(site_data[key]))
          else:
            data.append("")
        if write_header:
          site_data_file.write(",".join(header_buf))
          site_data_file.write('\n')
          header_buf[:]
          write_header = False

        site_data_file.write(",".join(data))
        site_data_file.write('\n')
        site_data_file.flush()
        data[:]
        if logger:
          logger.debug("Finished building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (row['SPLocation'], wq_utc_date, wq_date))

    if logger:
      logger.debug("Stations not matching: %s" % (", ".join(sites_not_found)))


def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-i", "--ImportData", dest="import_data",
                    help="Directory to import XMRG files from" )
  parser.add_option("-b", "--BuildSummaryData",
                    action="store_true", default=True,
                    dest="build_summary_data",
                    help="Flag that specifies to construct summary file.")
  parser.add_option("-w", "--WaterKeeperHistoricalFile", dest="wk_historical_wq_file",
                    help="Input file with the dates and stations we are creating summary for." )
  parser.add_option("-s", "--HistoricalSummaryOutPath", dest="summary_out_path",
                    help="Directory to write the historical summary data to." )
  parser.add_option("-d", "--StartDate", dest="starting_date",
                    help="Date to use for the retrieval." )
  parser.add_option("-m", "--StartTimeMidnight", dest="start_time_midnight",
                    action="store_true", default=True,
                    help="Set time to 00:00:00 for the queries instead of the sample time." )
  parser.add_option("-t", "--TideDataFile", dest="tide_data_file",
                    help="If used, this is the path to a tide data csv file.", default=None )


  (options, args) = parser.parse_args()

  if(options.config_file is None):
    parser.print_help()
    sys.exit(-1)

  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(options.config_file)

    logger = None
    logConfFile = config_file.get('logging', 'config_file')
    if(logConfFile):
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger('build_historical_logger')
      logger.info("Log file opened.")


  except ConfigParser.Error, e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)
  else:
    starting_date = None
    if options.starting_date:
      starting_date = timezone('UTC').localize(datetime.strptime(options.starting_date, '%Y-%m-%dT%H:%M:%S'))
    if options.build_summary_data:
      create_historical_summary(config_file=config_file,
                                water_keepers_historical_data=options.wk_historical_wq_file,
                                sites_historical_out_directory=options.summary_out_path,
                                start_time_midnight=options.start_time_midnight,
                                tide_data_file=options.tide_data_file)
  if logger:
    logger.info("Log file closed.")
  return


if __name__ == "__main__":
  main()
