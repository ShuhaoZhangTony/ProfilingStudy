#!/bin/bash
#NCS is used for reads to PCIe space, NCB is used for transfering data without coherency, and DRS is used for transfering data with coherency (cachable PCI transactions)
SET1='-e uncore_imc_0/event=cas_count_read/ -e uncore_imc_1/event=cas_count_read/ -e uncore_imc_2/event=cas_count_read/ -e uncore_imc_3/event=cas_count_read/'
SET2='-e uncore_imc_0/event=cas_count_write/ -e uncore_imc_1/event=cas_count_write/ -e uncore_imc_2/event=cas_count_write/ -e uncore_imc_3/event=cas_count_write/'


ocperf.py stat --per-socket -e \
uncore_imc_0, \
uncore_imc_1, \
uncore_imc_4, \
uncore_imc_5 -a -o $1 sleep 30
