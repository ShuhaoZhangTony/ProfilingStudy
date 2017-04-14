#!/bin/bash
numactl  --cpunodebind=0 -l ~/Documents/apache-storm-1.0.0-s0/bin/storm supervisor 
