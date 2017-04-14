#!/bin/bash
rm -fr ~/storm-local/*
rm ~/Documents/apache-storm-0.9.5/logs/*
numactl  --cpunodebind=0  --membind=0 ~/Documents/zookeeper-3.4.8/bin/zkServer.sh start&
numactl  --cpunodebind=0  --membind=0 ~/Documents/apache-storm-0.9.5/bin/storm nimbus&
numactl  --cpunodebind=0  --membind=0 ~/Documents/apache-storm-0.9.5/bin/storm supervisor&
numactl  --cpunodebind=0  --membind=0 ~/Documents/apache-storm-0.9.5/bin/storm ui&
