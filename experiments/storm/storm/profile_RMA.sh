#!/bin/bash
ocperf.py stat  --per-socket  -e \
task-clock,\
cpu-migrations,\
context-switches,\
offcore_response.demand_code_rd.llc_miss.remote_dram,\
offcore_response.demand_data_rd.llc_miss.remote_dram,\
offcore_response.all_demand_mlc_pref_reads.llc_miss.any_response,\
offcore_response.all_demand_mlc_pref_reads.llc_miss.local_dram,\
offcore_response.all_demand_mlc_pref_reads.llc_miss.remote_hitm_hit_forward -o $1 -a sleep $2
