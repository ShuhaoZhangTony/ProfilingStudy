#!/bin/bash
~/sbin/stop-all.sh

config=~/conf/spark-defaults.conf
exe=~/sbin/start-slave.sh
case $1 in
        1)#1 core      
        sed -i '33s/.*/process=\"numactl -C 0\"/' $exe #change process pin
        ;;
        2)#2 cores
        sed -i '33s/.*/process=\"numactl -C 0,1\"/' $exe #change process pin
        ;;
        3)#4 cores
        sed -i '33s/.*/process=\"numactl -C 0,1,2,3\"/' $exe #change process pin
        ;;
        4)#8 cores
        sed -i '33s/.*/process=\"numactl -N 0 -m 0\"/' $exe #change process pin
        ;;
        5)#16 cores
        sed -i '33s/.*/process=\"numactl -N 0 -m 0\"/' $exe #change process pin
        ;;
        6)#32 cores
        sed -i '33s/.*/process=\"numactl -N 0 -m 0\"/' $exe #change process pin
        ;;
esac
~/sbin/start-all.sh
sleep 15

