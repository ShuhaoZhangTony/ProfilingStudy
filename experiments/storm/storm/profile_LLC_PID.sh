#!/bin/bash
ocperf.py stat --per-socket -e \
offcore_response.demand_code_rd.llc_miss.any_response,\
offcore_response.demand_code_rd.llc_miss.local_dram,\
offcore_response.demand_code_rd.llc_miss.remote_dram,\
offcore_response.demand_data_rd.llc_miss.any_response,\
offcore_response.demand_data_rd.llc_miss.local_dram,\
offcore_response.demand_data_rd.llc_miss.remote_dram \
-o $1 -a -p $2
