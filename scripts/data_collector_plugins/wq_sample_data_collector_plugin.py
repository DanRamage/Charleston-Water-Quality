import sys
sys.path.append('../')
sys.path.append('../../commonfiles/python')
import os
import logging.config
from data_collector_plugin import data_collector_plugin
import ConfigParser
import traceback
import time
#from yapsy.IPlugin import IPlugin
#from multiprocessing import Process

from get_wq_sample_data import check_email_for_update
from chs_get_historical_data import parse_dhec_sheet_data
from wq_sites import wq_sample_sites
from wq_output_results import wq_sample_data,wq_samples_collection,wq_advisories_file,wq_station_advisories_file
from data_result_types import data_result_types
from smtp_utils import smtpClass

class wq_sample_data_collector_plugin(data_collector_plugin):

  def __init__(self):
    data_collector_plugin.__init__(self)
    self.output_queue = None

  def initialize_plugin(self, **kwargs):
    try:
      plugin_details = kwargs['details']

      self.ini_file = plugin_details.get('Settings', 'ini_file')
      self.output_queue = kwargs['queue']

      email_ini_file = plugin_details.get("MonitorEmail", "ini_file")
      config_file = ConfigParser.RawConfigParser()
      config_file.read(email_ini_file)
      self.monitor_mailhost = config_file.get("sample_data_collector_plugin", "mailhost")
      self.port = config_file.get("sample_data_collector_plugin", "port")
      self.monitor_fromaddr = config_file.get("sample_data_collector_plugin", "fromaddr")
      self.monitor_toaddrs = config_file.get("sample_data_collector_plugin", "toaddrs").split(',')
      self.monitor_subject = config_file.get("sample_data_collector_plugin", "subject")
      self.monitor_user = config_file.get("sample_data_collector_plugin", "user")
      self.monitor_password = config_file.get("sample_data_collector_plugin", "password")

      return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self):
    try:
      start_time = time.time()
      config_file = ConfigParser.RawConfigParser()
      config_file.read(self.ini_file)

      logger = None
      log_conf_file = config_file.get('logging', 'config_file')
      if log_conf_file:
        logging.config.fileConfig(log_conf_file)
        logger = logging.getLogger('sc_rivers_wq_data_harvest_logger')
        logger.info("Log file opened.")
    except ConfigParser.Error, e:
      traceback.print_exc("No log configuration file given, logging disabled.")
    else:
      try:
        boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
        sites_location_file = config_file.get('boundaries_settings', 'sample_sites')

        results_file = config_file.get('json_settings', 'advisory_results')
        station_results_directory = config_file.get('json_settings', 'station_results_directory')

      except ConfigParser.Error, e:
        logger.exception(e)
      else:
        try:
          wq_sites = wq_sample_sites()
          wq_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

          wq_data_files = check_email_for_update(self.ini_file)
          if logger is not None:
            logger.debug("Files: %s found" % (wq_data_files))

          renamed_files = []
          wq_data_collection = wq_samples_collection()
          for wq_file in wq_data_files:
            file_name, exten = os.path.splitext(wq_file)
            if exten == '.xls' or exten == '.xlsx':
              sample_date = parse_dhec_sheet_data(wq_file, wq_data_collection)
              # Rename the file to have the sample date in filename.
              file_path, file_name = os.path.split(wq_file)
              file_name, file_ext = os.path.splitext(file_name)
              new_filename = os.path.join(file_path, "%s-sample_results%s" % (sample_date.strftime("%Y-%m-%d"), file_ext))
              logger.debug("Renaming file: %s to %s" % (wq_file, new_filename))
              try:
                os.rename(wq_file, new_filename)
                renamed_files.append(new_filename)
              except Exception as e:
                logger.exception(e)
            else:
              self.logger.error("File: %s is not the excel file we are looking for.")

          # Create the geojson files
          if len(wq_data_collection):
            current_advisories = wq_advisories_file(wq_sites)
            current_advisories.create_file(results_file, wq_data_collection)

            for site in wq_sites:
              site_advisories = wq_station_advisories_file(site)
              site_advisories.create_file(station_results_directory, wq_data_collection)

          self.output_queue.put((data_result_types.SAMPLING_DATA_TYPE, wq_data_collection))

          try:
            self.logger.debug("Emailing sample data collector file list.")
            if len(renamed_files):
              mail_body = "Files: %s downloaded and processed" % (renamed_files)
            else:
              mail_body = "ERROR: No files downloaded."
            subject = "[WQ]Saluda River Sample Data"
            # Now send the email.
            smtp = smtpClass(host=self.monitor_mailhost,
                             user=self.monitor_user,
                             password=self.monitor_password,
                             port=self.port,
                             use_tls=True)

            smtp.rcpt_to(self.monitor_toaddrs)
            smtp.from_addr(self.monitor_fromaddr)
            smtp.subject(subject)
            smtp.message(mail_body)
            smtp.send(content_type="text")
            self.logger.debug("Finished emailing sample data collector file list.")
          except Exception as e:
            if self.logger:
              self.logger.exception(e)

          if logger is not None:
            logger.info("Log file closed.")
        except Exception, e:
          if(logger):
            logger.exception(e)
      self.logger.debug("run finished in %f seconds." % (time.time() - start_time))
    return
