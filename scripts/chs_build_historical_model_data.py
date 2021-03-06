import sys
sys.path.append('../commonfiles/python')
import os
import logging.config
import math

from datetime import datetime, timedelta
from pytz import timezone
import logging.config
import optparse
import ConfigParser
from collections import OrderedDict

from wqHistoricalData import wq_data
from wqHistoricalData import tide_data_file_ex,station_geometry,sampling_sites, wq_defines, geometry_list
from wq_sites import wq_sample_sites
from wqDatabase import wqDB
from chs_get_historical_data import parse_sheet_data, parse_dhec_sheet_data
from wq_output_results import wq_sample_data,wq_samples_collection

from xeniaSQLAlchemy import xeniaAlchemy, multi_obs, func
from xeniaSQLiteAlchemy import xeniaAlchemy as sl_xeniaAlchemy, multi_obs as sl_multi_obs, func as sl_func
from sqlalchemy import or_
from stats import calcAvgSpeedAndDir
from xenia import qaqcTestFlags

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


    self.logger.debug("Connection to nexrad xenia db: %s" % (kwargs['xenia_database_name']))
    self.nexrad_db = wqDB(kwargs['xenia_database_name'], __name__)

    self.logger.debug("Connection to xenia db: %s" % (kwargs['xenia_database_name']))
    self.xenia_db = sl_xeniaAlchemy()
    if self.xenia_db.connectDB('sqlite', None, None, kwargs['xenia_database_name'], None, False):
      self.logger.info("Succesfully connect to DB: %s" %(kwargs['xenia_database_name']))
    else:
      self.logger.error("Unable to connect to DB: %s" %(kwargs['xenia_database_name']))

    self.nos_stations = ['nos.8665530.met']
    self.nos_variables = [('wind_speed', 'm_s-1'),
                          ('water_temperature', 'celsius')]
    self.usgs_stations = ['usgs.021720709.wq', 'usgs.021720869.wq', 'usgs.021720710.wq', 'usgs.021720698.wq', 'usgs.0217206935.wq', 'usgs.021720677.wq']
    self.usgs_variables = [('water_conductivity', 'uS_cm-1'),
                           ('salinity', 'psu'),
                      ('gage_height', 'm'),
                      ('water_temperature', 'celsius'),
                      ('oxygen_concentration', 'mg_L-1')]


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

    self.get_nexrad_data(start_date, wq_tests_data)
    self.get_tide_data(start_date, wq_tests_data)

    for station in self.nos_stations:
      if start_date.date() == datetime.strptime('2015-05-06', '%Y-%m-%d').date():
        i=0
      for variable in self.nos_variables:
        self.get_platform_data(station, variable[0], variable[1], start_date, wq_tests_data)


    for station in self.usgs_stations:
      for variable in self.usgs_variables:
        self.get_platform_data(station, variable[0], variable[1], start_date, wq_tests_data)
    
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

        var_name = '%s_nexrad_rainfall_intensity' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_total_1_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_2_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_3_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

    for station in self.nos_stations:
      for variable in self.nos_variables:
        var_name = '%s-%s' % (station.replace('.', '_'), variable[0])
        wq_tests_data[var_name] = wq_defines.NO_DATA
        if variable[0] == 'wind_speed':
          var_name = '%s-%s' % (station.replace('.', '_'), 'wind_from_direction')
          wq_tests_data[var_name] = wq_defines.NO_DATA

    for station in self.usgs_stations:
      for variable in self.usgs_variables:
        var_name = '%s-%s' % (station.replace('.', '_'), variable[0])
        wq_tests_data[var_name] = wq_defines.NO_DATA


    if self.logger:
      self.logger.debug("Finished creating and initializing data dict.")

    return

  def get_platform_data(self, platform_handle, variable, uom, start_date, wq_tests_data):
    try:
      self.logger.debug("Platform: %s Obs: %s(%s) Date: %s query" % (platform_handle, variable, uom, start_date))

      station = platform_handle.replace('.', '_')
      var_name = '%s-%s' % (station, variable)
      end_date = start_date
      begin_date = start_date - timedelta(hours=24)
      if variable != 'wind_speed':
        sensor_id = self.xenia_db.sensorExists(variable, uom, platform_handle, 1)
      else:
        sensor_id = self.xenia_db.sensorExists(variable, uom, platform_handle, 1)
        wind_dir_id = self.xenia_db.sensorExists('wind_from_direction', 'degrees_true', platform_handle, 1)

      if sensor_id is not -1 and sensor_id is not None:
        recs = self.xenia_db.session.query(sl_multi_obs) \
          .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S')) \
          .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S')) \
          .filter(sl_multi_obs.sensor_id == sensor_id) \
          .filter(or_(sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD, sl_multi_obs.qc_level == None)) \
          .order_by(sl_multi_obs.m_date).all()
        if variable == 'wind_speed':
          dir_recs = self.xenia_db.session.query(sl_multi_obs) \
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S')) \
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S')) \
            .filter(sl_multi_obs.sensor_id == wind_dir_id) \
            .filter(or_(sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD, sl_multi_obs.qc_level == None)) \
            .order_by(sl_multi_obs.m_date).all()

        if len(recs):
          if variable == 'wind_speed':
            if sensor_id is not None and wind_dir_id is not None:
              wind_dir_tuples = []
              direction_tuples = []
              scalar_speed_avg = None
              speed_count = 0
              for wind_speed_row in recs:
                for wind_dir_row in dir_recs:
                  if wind_speed_row.m_date == wind_dir_row.m_date:
                    #self.logger.debug("Building tuple for Speed(%s): %f Dir(%s): %f" % (
                    #wind_speed_row.m_date, wind_speed_row.m_value, wind_dir_row.m_date, wind_dir_row.m_value))
                    if scalar_speed_avg is None:
                      scalar_speed_avg = 0
                    scalar_speed_avg += wind_speed_row.m_value
                    speed_count += 1
                    # Vector using both speed and direction.
                    wind_dir_tuples.append((wind_speed_row.m_value, wind_dir_row.m_value))
                    # Vector with speed as constant(1), and direction.
                    direction_tuples.append((1, wind_dir_row.m_value))
                    break

              if len(wind_dir_tuples):
                avg_speed_dir_components = calcAvgSpeedAndDir(wind_dir_tuples)
                self.logger.debug("Platform: %s Avg Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                                    avg_speed_dir_components[
                                                                                                      0],
                                                                                                    avg_speed_dir_components[
                                                                                                      0],
                                                                                                    avg_speed_dir_components[
                                                                                                      1]))

                # Unity components, just direction with speeds all 1.
                avg_dir_components = calcAvgSpeedAndDir(direction_tuples)
                scalar_speed_avg = scalar_speed_avg / speed_count
                wq_tests_data[var_name] = scalar_speed_avg
                wind_dir_var_name = '%s-%s' % (station, 'wind_from_direction')
                wq_tests_data[wind_dir_var_name] = avg_dir_components[1]
                self.logger.debug(
                  "Platform: %s Avg Scalar Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                           scalar_speed_avg,
                                                                                           scalar_speed_avg,
                                                                                           avg_dir_components[1]))


          else:
            wq_tests_data[var_name] = sum(rec.m_value for rec in recs) / len(recs)
            self.logger.debug("Platform: %s Avg %s: %f Records used: %d" % (
              platform_handle, variable, wq_tests_data[var_name], len(recs)))
            if variable == 'water_conductivity':
              water_con = wq_tests_data[var_name]
              if uom == 'uS_cm-1':
                water_con = water_con / 1000.0
                salinity_var = '%s-%s' % (station, 'salinity')
              wq_tests_data[salinity_var] = 0.47413 / (math.pow((1 / water_con), 1.07) - 0.7464 * math.pow(10, -3))
              self.logger.debug("Platform: %s Avg %s: %f Records used: %d" % (
                platform_handle, 'salinity', wq_tests_data[salinity_var], len(recs)))
        else:
          self.logger.error("Platform: %s sensor: %s(%s) Date: %s had no data" % (platform_handle, variable, uom, start_date))
      else:
        self.logger.error("Platform: %s sensor: %s(%s) does not exist" % (platform_handle, variable, uom))

    except Exception as e:
      self.logger.exception(e)
      return False

    return True

  def get_tide_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    use_web_service = True
    if self.tide_data_obj is not None:
      use_web_service = False
      date_key = start_date.strftime('%Y-%m-%dT%H:%M:%S')
      if date_key in self.tide_data_obj:
        tide_rec = self.tide_data_obj[date_key]
        wq_tests_data['tide_range_%s' % (self.tide_station)] = float(tide_rec['range'])
        wq_tests_data['tide_hi_%s' % (self.tide_station)] = float(tide_rec['hh'])
        wq_tests_data['tide_lo_%s' % (self.tide_station)] = float(tide_rec['ll'])
        wq_tests_data['tide_stage_%s' % (self.tide_station)] = float(tide_rec['tide_stage'])

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

  def get_nexrad_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

    #Collect the radar data for the boundaries.
    for boundary in self.site.contained_by:
      clean_var_bndry_name = boundary.name.lower().replace(' ', '_')

      platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
      if self.logger:
        self.logger.debug("Start retrieving nexrad platfrom: %s" % (platform_handle))
      # Get the radar data for previous 8 days in 24 hour intervals
      for prev_hours in range(24, 192, 24):
        var_name = '%s_nexrad_summary_%d' % (clean_var_bndry_name, prev_hours)
        radar_val = self.nexrad_db.getLastNHoursSummaryFromRadarPrecip(platform_handle,
                                                                    start_date,
                                                                    prev_hours,
                                                                    'precipitation_radar_weighted_average',
                                                                    'mm')
        if radar_val != None:
          #Convert mm to inches
          wq_tests_data[var_name] = radar_val * 0.0393701
        else:
          if self.logger:
            self.logger.error("No data available for boundary: %s Date: %s. Error: %s" %(var_name, start_date, self.nexrad_db.getErrorInfo()))

      #calculate the X day delay totals
      if wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_24' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_1_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_24' % (clean_var_bndry_name)]

      if wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_2_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)]

      if wq_tests_data['%s_nexrad_summary_96' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_3_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_96' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)]

      prev_dry_days = self.nexrad_db.getPrecedingRadarDryDaysCount(platform_handle,
                                             start_date,
                                             'precipitation_radar_weighted_average',
                                             'mm')
      if prev_dry_days is not None:
        var_name = '%s_nexrad_dry_days_count' % (clean_var_bndry_name)
        wq_tests_data[var_name] = prev_dry_days

      rainfall_intensity = self.nexrad_db.calcRadarRainfallIntensity(platform_handle,
                                                               start_date,
                                                               60,
                                                              'precipitation_radar_weighted_average',
                                                              'mm')
      if rainfall_intensity is not None:
        var_name = '%s_nexrad_rainfall_intensity' % (clean_var_bndry_name)
        wq_tests_data[var_name] = rainfall_intensity


      if self.logger:
        self.logger.debug("Finished retrieving nexrad platfrom: %s" % (platform_handle))

    if self.logger:
      self.logger.debug("Finished retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))


def create_historical_summary(**kwargs):
  logger = logging.getLogger('create_historical_summary_logger')

  config_file = kwargs['config_file']
  boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
  sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
  wq_historical_db = config_file.get('historic_database', 'name')

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
    utc_tz = timezone('UTC')
    current_site = None
    processed_sites = []
    #stop_date = eastern.localize(datetime.strptime('2014-01-29 00:00:00', '%Y-%m-%d %H:%M:%S'))
    #stop_date = stop_date.astimezone(timezone('UTC'))
    try:
      chs_wq_data = chs_wq_historical_data(xenia_database_name=wq_historical_db,
                                    use_logger=True)

      chs_results = wq_samples_collection()
      if len(kwargs['water_keepers_historical_data']):
        parse_sheet_data(kwargs['water_keepers_historical_data'], wq_sites, chs_results)
      elif len(kwargs['dhec_historical_data']):
        for file in kwargs['dhec_historical_data']:
          parse_dhec_sheet_data(file, chs_results)
    except Exception, e:
      if logger:
        logger.exception(e)
    else:
      # Build list of all dates.
      water_keeper_dates = []
      for site_name in chs_results:
        #Sort dates.
        site_data = chs_results[site_name]
        site_data.sort(key=lambda x: x.date_time, reverse=False)

        site = wq_sites.get_site(site_name)
        if site is not None:
          sample_site_filename = os.path.join(kwargs['summary_out_directory'], "%s.csv" % (site.name))
          write_header = True
          try:
            if logger:
              logger.debug("Opening sample site history file: %s" % (sample_site_filename))
            site_data_file = open(sample_site_filename, 'w')
          except IOError, e:
            logger.exception(e)
            raise e
          else:

            logger.debug("Site: %s getting dates" % (site_name))
            site_data = chs_results[site_name]
            auto_number = 0
            for rec in site_data:
              #wq_date = rec['sample_date']
              wq_date = rec.date_time
              wq_utc_date = wq_date.astimezone(utc_tz)

              logger.debug(
                "Start building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (site.name, wq_utc_date, wq_date))

              entero_value = rec.value
              if rec.date_time not in water_keeper_dates:
                logger.debug("Site: %s adding date: %s" % (site_name, wq_date))
                water_keeper_dates.append(rec.date_time)

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

              site_data = OrderedDict([('autonumber', auto_number),
                                       ('station_name',site.name),
                                       ('sample_datetime', wq_date.strftime("%Y-%m-%d %H:%M:%S")),
                                       ('sample_datetime_utc', wq_utc_date.strftime("%Y-%m-%d %H:%M:%S")),
                                       ('enterococcus_value', entero_value)])
              try:
                chs_wq_data.query_data(wq_utc_date, wq_utc_date, site_data)
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
                logger.debug("Finished building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (site.name, wq_utc_date, wq_date))
              auto_number += 1

          site_data_file.close()
        else:
          logger.error("Site: %s not found in sample sites." % (site_name))
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
  parser.add_option("-w", "--WaterKeeperHistoricalFile", dest="wk_historical_wq_file", default='',
                    help="Input file with the dates and stations we are creating summary for." )
  parser.add_option("--DHECHistoryFile", dest="dhec_excel_file", default='',
                    help="DHEC Excel File with historical etcoc data." )

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
  #Water keepers file is midnight based, dhec has sample times.
  """
  --DHECHistoryFile=/Users/danramage/Documents/workspace/WaterQuality/Charleston-Water-Quality/data/historic/wq_sample_data/CWK_RWQMP13_Database.xls,/Users/danramage/Documents/workspace/WaterQuality/Charleston-Water-Quality/data/historic/wq_sample_data/CWK_RWQMP14_Database.xls,/Users/danramage/Documents/workspace/WaterQuality/Charleston-Water-Quality/data/historic/wq_sample_data/CWK_RWQMP15_Database.xls,/Users/danramage/Documents/workspace/WaterQuality/Charleston-Water-Quality/data/historic/wq_sample_data/CWK_RWQMP16_Database.xls  
  --WaterKeeperHistoricalFile="/Users/danramage/Documents/workspace/WaterQuality/Charleston-Water-Quality/data/historic/wq_sample_data/Recreational Water Quality Monitoring Program Data.xlsx"
  """

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
                                dhec_historical_data=options.dhec_excel_file.split(','),
                                summary_out_directory=options.summary_out_path,
                                start_time_midnight=options.start_time_midnight,
                                tide_data_file=options.tide_data_file)
  if logger:
    logger.info("Log file closed.")
  return


if __name__ == "__main__":
  main()
