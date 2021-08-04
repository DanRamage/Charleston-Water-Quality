#!/bin/bash

source /usr/local/virtualenv/pyenv-3.8.5/bin/activate

cd /home/xeniaprod/scripts/Charleston-Water-Quality/scripts;

python chs_wq_prediction_engine.py --ConfigFile=/home/xeniaprod/scripts/Charleston-Water-Quality/config/chs_prediction_engine.ini >> /home/xeniaprod/tmp/log/chs_wq_prediction_engine_sh.log 2>&1
