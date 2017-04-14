#!/bin/bash
rm -rf ~/storm-local-s3/*
rm -rf logs-s3/*
numactl  --cpunodebind=3 -l ~/Documents/apache-storm-1.0.0-s3/bin/storm supervisor 
