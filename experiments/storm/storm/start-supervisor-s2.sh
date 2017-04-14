#!/bin/bash
rm -rf ~/storm-local-s2/*
rm -rf logs-s2/*
numactl  --cpunodebind=2 -l ~/Documents/apache-storm-1.0.0-s2/bin/storm supervisor 
