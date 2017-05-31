#!/usr/bin/env bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate;

python /home/xeniaprod/scripts/commonfiles/python/wqXMRGProcessing.py --ConfigFile=/home/xeniaprod/scripts/Charleston-Water-Quality/config/chs_historical_build_config.ini --ImportData=/mnt/waterquality/xmrg/2014/xmrg/jan,/mnt/waterquality/xmrg/2014/xmrg/feb,/mnt/waterquality/xmrg/2014/xmrg/mar,/mnt/waterquality/xmrg/2014/xmrg/apr,/mnt/waterquality/xmrg/2014/xmrg/may,/mnt/waterquality/xmrg/2014/xmrg/jun,/mnt/waterquality/xmrg/2014/xmrg/jul,/mnt/waterquality/xmrg/2014/xmrg/aug,/mnt/waterquality/xmrg/2014/xmrg/sep,/mnt/waterquality/xmrg/2014/xmrg/oct,/mnt/waterquality/xmrg/2014/xmrg/nov,/mnt/waterquality/xmrg/2014/xmrg/dec