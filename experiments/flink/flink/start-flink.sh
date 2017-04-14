#/bin/bash
~/bin/stop-cluster.sh

TM=~/bin/taskmanager.sh
case $1 in
	1)#1 core
        sed -i '22s/.*/use_hp=0/' $TM
	sed -i '19s/.*/vm=8/' $TM #change memory size
	sed -i '20s/.*/process=\"numactl -C 3\"/' $TM #change process pin
	;;
	2)#2 cores
        sed -i '22s/.*/use_hp=0/' $TM
	sed -i '19s/.*/vm=32/' $TM #change memory size
	sed -i '20s/.*/process=\"numactl -C 3,7\"/' $TM #change process pin
	;;
	3)#4 cores
        sed -i '22s/.*/use_hp=0/' $TM
	sed -i '19s/.*/vm=64/' $TM #change memory size
	sed -i '20s/.*/process=\"numactl -C 3,7,11,15\"/' $TM #change process pin
	;;
	4)#8 cores
        sed -i '22s/.*/use_hp=0/' $TM
	sed -i '19s/.*/vm=100/' $TM #change memory size
	sed -i '20s/.*/process=\"numactl -N 3 --localalloc\"/' $TM #change process pin
	;;
	5)#16 cores
        sed -i '22s/.*/use_hp=0/' $TM
	sed -i '19s/.*/vm=200/' $TM #change memory size
	sed -i '20s/.*/process=\"numactl -N 0,3 --localalloc\"/' $TM #change process pin
	;;
	6)#32 cores
	sed -i '22s/.*/use_hp=0/' $TM
	sed -i '19s/.*/vm=400/' $TM #change memory size
	sed -i '20s/.*/process=\"numactl -N 0,1,2,3 -m 0,1,2,3\"/' $TM #change process pin
	;;
	7)#8 cores with batch
	sed -i '22s/.*/use_hp=0/' $TM
        sed -i '19s/.*/vm=100/' $TM #change memory size
        sed -i '20s/.*/process=\"numactl -N 3 --localalloc\"/' $TM #change process pin
	;;
	8)#8 cores with HP
	sed -i '22s/.*/use_hp=1/' $TM
        sed -i '19s/.*/vm=100/' $TM #change memory size
        sed -i '20s/.*/process=\"numactl -N 3 --localalloc\"/' $TM #change process pin
	;;
esac

#case $2 in 
#	0)#no jit
#	sed -i '21s/.*/jit=0/' $TM
#	;;
#	1)#with jit
#	sed -i '21s/.*/jit=1/' $TM
#	;;
#esac
#case $3 in
#        0)#no hp
#        sed -i '22s/.*/use_hp=0/' $TM
#        ;;
#        1)#with hp
#        sed -i '22s/.*/use_hp=1/' $TM
#        ;;
#esac

~/bin/start-cluster.sh
sleep 15
