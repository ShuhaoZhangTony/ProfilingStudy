#!/bin/bash
rm -rf ~/zookeeper/*
rm -rf ~/storm-local/*
rm -rf logs/*
~/Documents/zookeeper-3.5.1-alpha/bin/zkServer.sh start
~/Documents/apache-storm-1.0.1/bin/storm nimbus
