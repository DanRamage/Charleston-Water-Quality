#!/usr/bin/env bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate;

python /home/xeniaprod/scripts/commonfiles/python/wqXMRGProcessing.py --ConfigFile=/home/xeniaprod/scripts/Charleston-Water-Quality/config/chs_historical_build_config.ini --ImportData=/mnt/waterquality/xmrg/2016-all/may,/mnt/waterquality/xmrg/2016-all/jun,/mnt/waterquality/xmrg/2016-all/jul,/mnt/waterquality/xmrg/2016-all/aug