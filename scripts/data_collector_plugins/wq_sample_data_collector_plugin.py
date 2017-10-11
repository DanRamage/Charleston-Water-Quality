import sys
sys.path.append('../')
sys.path.append('../../commonfiles/python')
import os
import logging.config
from data_collector_plugin import data_collector_plugin
import ConfigParser
import traceback
import time
import poplib
import email
from datetime import datetime
from pytz import timezone
#from yapsy.IPlugin import IPlugin
#from multiprocessing import Process

from wq_sites import wq_sample_sites
from wq_output_results import wq_sample_data,wq_samples_collection,wq_advisories_file,wq_station_advisories_file
from data_result_types import data_result_types
from smtp_utils import smtpClass

def parse_dhec_sheet_data(xl_file_name, wq_data_collection):
  from xlrd import xldate
  import xlrd


  station_to_site_map = {
    'AR2': 'Brittlebank Park',
    'JIC2': 'James Island Creek 2',
    'JIC1': 'James Island Creek 1',
    'SC1': 'Shem Creek 1',
    'SC2': 'Shem Creek 2',
    'SC3': 'Shem Creek 3',
    'FB1': 'Folly Beach'}
  logger = logging.getLogger('chs_historical_logger')
  logger.debug("Starting parse_dhec_sheet_data, parsing file: %s" % (xl_file_name))

  wb = xlrd.open_workbook(filename = xl_file_name)

  est_tz = timezone('US/Eastern')
  #utc_tz = timezone('UTC')
  sample_date = None
  try:
    sheet = wb.sheet_by_name('Results')
  except Exception as e:
    logger.exception(e)
  else:
    row_headers = []
    results_ndx = \
    station_ndx = \
    date_ndx = \
    parameter_ndx = \
    time_ndx = None
    for row_ndx,data_row in enumerate(sheet.get_rows()):
      if row_ndx != 0:
        try:
          wq_sample_rec = wq_sample_data()
          if data_row[parameter_ndx].value == "Enterococci":
            station_name = data_row[station_ndx].value.strip()
            if station_name in station_to_site_map:
              wq_sample_rec.station = station_to_site_map[station_name]
              try:
                date_val = xlrd.xldate.xldate_as_datetime(data_row[date_ndx].value, wb.datemode)
              except Exception as e:
                try:
                  date_val = datetime.strptime(data_row[date_ndx].value, "%Y-%m-%d")
                except Exception as e:
                  logger.error("Date format error on line: %d" % (row_ndx))
                  logger.exception(e)
                  break
              try:
                time_val = xldate.xldate_as_datetime(data_row[time_ndx].value, False)
                #time_val = datetime.strptime(data_row[time_ndx].value, "%H%M")
              except Exception as e:
                val = data_row[time_ndx].value
                try:
                  time_val = datetime.strptime(str(val), "%H%M")
                except Exception as e:
                  logger.error("Time format error on line: %d" % (row_ndx))
                  time_val = datetime.strptime('00:00:00', '%H:%M:%S')

              wq_sample_rec.date_time = (est_tz.localize(datetime.combine(date_val.date(), time_val.time())))
              #wq_sample_rec.date_time = (est_tz.localize(datetime.combine(date_val.date(), time_val.time()))).astimezone(utc_tz)
              wq_sample_rec.value = data_row[results_ndx].value
              logger.debug("Site: %s Date: %s Value: %s" % (wq_sample_rec.station,
                                                            wq_sample_rec.date_time,
                                                            wq_sample_rec.value))
              if sample_date is None or date_val > sample_date:
                sample_date = date_val
              wq_data_collection.append(wq_sample_rec)
        except Exception as e:
          logger.error("Error found on row: %d" % (row_ndx))
          logger.exception(e)
      else:
        #Copy the header names out
        for cell in data_row:
          row_headers.append(cell.value)
        station_ndx = row_headers.index('Station')
        date_ndx = row_headers.index('Date')
        time_ndx = row_headers.index('Time')
        results_ndx = row_headers.index('Result')
        parameter_ndx = row_headers.index("Parameter")

  return sample_date

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

  def check_email_for_update(self, config_filename):
    file_list = []
    logger = logging.getLogger('wq_data_harvest_logger')
    logger.debug("Starting check_email_for_update")
    try:
      config_file = ConfigParser.RawConfigParser()
      config_file.read(config_filename)

      email_ini_config_filename = config_file.get("password_protected_configs", "settings_ini")
      email_ini_config_file = ConfigParser.RawConfigParser()
      email_ini_config_file.read(email_ini_config_filename)

      email_host = email_ini_config_file.get("wq_results_email_settings", "host")
      email_user = email_ini_config_file.get("wq_results_email_settings", "user")
      email_password = email_ini_config_file.get("wq_results_email_settings", "password")
      email_host_port = email_ini_config_file.get("wq_results_email_settings", "port")
      destination_directory = email_ini_config_file.get("wq_results_email_settings", "destination_directory")
    except (ConfigParser.Error, Exception) as e:
      logger.exception(e)

    connected = False
    for attempt_cnt in range(0, 5):
      try:
        logger.info("Attempt: %d to connect to email server." % (attempt_cnt))
        pop3_obj = poplib.POP3_SSL(email_host, email_host_port)
        pop3_obj.user(email_user)
        pop3_obj.pass_(email_password)
        connected = True
        logger.info("Successfully connected to email server.")
        break
      except (poplib.error_proto, Exception) as e:
        logger.exception(e)
        time.sleep(5)
    if connected:
      emails, total_bytes = pop3_obj.stat()
      for i in range(emails):
        # return in format: (response, ['line', ...], octets)
        msg_num = i + 1
        response = pop3_obj.retr(msg_num)
        raw_message = response[1]

        str_message = email.message_from_string("\n".join(raw_message))

        # save attach
        for part in str_message.walk():
          logger.debug("Content type: %s" % (part.get_content_type()))

          if part.get_content_maintype() == 'multipart':
            continue

          if part.get('Content-Disposition') is None:
            logger.debug("No content disposition")
            continue

          filename = part.get_filename()
          if filename.find('xlsx') != -1 or filename.find('xls') != -1:
            download_time = datetime.now()
            logger.debug("Attached filename: %s" % (filename))
            save_file = "%s_%s" % (download_time.strftime("%Y-%m-%d_%H_%M_%S"), filename)
            saved_file_name = os.path.join(destination_directory, save_file)
            logger.debug("Saving file as filename: %s" % (saved_file_name))
            try:
              with open(saved_file_name, 'wb') as out_file:
                out_file.write(part.get_payload(decode=1))
                out_file.close()
                file_list.append(saved_file_name)
                pop3_obj.dele(msg_num)
            except Exception as e:
              logger.exception(e)
      pop3_obj.quit()

      logger.debug("Finished check_email_for_update")
    return file_list

  def run(self):
    try:
      start_time = time.time()
      config_file = ConfigParser.RawConfigParser()
      config_file.read(self.ini_file)

      logger = None
      log_conf_file = config_file.get('logging', 'wq_sample_data_log_file')
      if log_conf_file:
        logging.config.fileConfig(log_conf_file)
        logger = logging.getLogger('wq_data_harvest_logger')
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

          wq_data_files = self.check_email_for_update(self.ini_file)
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
