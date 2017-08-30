#!/bin/bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate;

python /home/xeniaprod/scripts/Charleston-Water-Quality/commonfiles/python/wqXMRGProcessing.py --ConfigFile=/home/xeniaprod/scripts/Charleston-Water-Quality/config/chs_prediction_engine_config.ini --FillGaps --BackfillNHours=192