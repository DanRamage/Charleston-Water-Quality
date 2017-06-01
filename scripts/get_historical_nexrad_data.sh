#!/usr/bin/env bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate;

python /home/xeniaprod/scripts/commonfiles/python/wqXMRGProcessing.py --ConfigFile=/home/xeniaprod/scripts/Charleston-Water-Quality/config/chs_historical_build_config.ini --ImportData=/mnt/waterquality/xmrg/2016/jan,/mnt/waterquality/xmrg/2016/feb,/mnt/waterquality/xmrg/2016/mar,/mnt/waterquality/xmrg/2016/apr,/mnt/waterquality/xmrg/2016/may,/mnt/waterquality/xmrg/2016/jun,/mnt/waterquality/xmrg/2016/jul,/mnt/waterquality/xmrg/2016/aug,/mnt/waterquality/xmrg/2016/sep,/mnt/waterquality/xmrg/2016/oct,/mnt/waterquality/xmrg/2016/nov,/mnt/waterquality/xmrg/2016/dec