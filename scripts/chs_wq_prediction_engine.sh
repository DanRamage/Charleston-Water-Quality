#!/bin/bash

source /usr2/virtualenvs/pyenv2.7/bin/activate;

cd /home/xeniaprod/scripts/Charleston-Water-Quality/scripts;

python chs_wq_prediction_engine.py --ConfigFile=/home/xeniaprod/scripts/Charleston-Water-Quality/config/chs_prediction_engine.ini >> /home/xeniaprod/tmp/log/chs_wq_prediction_engine_sh.log 2>&1
