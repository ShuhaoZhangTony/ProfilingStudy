#!/bin/bash
rm -rf ~/storm-local-s1/*
rm -rf logs-s1/*
numactl  --cpunodebind=1 -l ~/Documents/apache-storm-1.0.0-s1/bin/storm supervisor 
