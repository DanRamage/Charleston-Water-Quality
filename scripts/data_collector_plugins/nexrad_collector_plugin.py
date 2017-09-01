import sys
sys.path.append('../../commonfiles/python')
import logging.config
from data_collector_plugin import data_collector_plugin
from datetime import datetime
from pytz import timezone
import ConfigParser
import traceback

from wqXMRGProcessing import wqXMRGProcessing

class nexrad_collector_plugin(data_collector_plugin):

  def initialize_plugin(self, **kwargs):
    try:
      plugin_details = kwargs['details']
      self.ini_file = plugin_details.get('Settings', 'ini_file')
      return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self):
    try:
      config_file = ConfigParser.RawConfigParser()
      config_file.read(self.ini_file)
      logging.config.fileConfig(config_file.get('logging', 'xmrg_ingest'))
    except (ConfigParser.Error, Exception) as e:
      traceback.print_exc(e)
    else:
      try:
        logger = logging.getLogger(__name__)
        backfill_hours = config_file.getint('nexrad_database', 'backfill_hours')
        fill_gaps = config_file.getboolean('nexrad_database', 'fill_gaps')
      except (ConfigParser.Error, Exception) as e:
        traceback.print_exc(e)
      else:
        try:

          xmrg_proc = wqXMRGProcessing(logger=True)
          xmrg_proc.load_config_settings(config_file = self.ini_file)

          start_date_time = timezone('US/Eastern').localize(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).astimezone(timezone('UTC'))
          if fill_gaps:
            logger.info("Fill gaps Start time: %s Prev Hours: %d" % (start_date_time, backfill_hours))
            xmrg_proc.fill_gaps(start_date_time, backfill_hours)
          else:
            logger.info("Backfill N Hours Start time: %s Prev Hours: %d" % (start_date_time, backfill_hours))
            file_list = xmrg_proc.download_range(start_date_time, backfill_hours)
            xmrg_proc.import_files(file_list)
        except Exception as e:
          logger.exception(e)
    return
