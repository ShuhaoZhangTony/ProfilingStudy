#!/bin/bash
URL_Master="spark://155.69.149.213:7070"
HOME="/home/spark"
JAVA_HOME="$HOME/Documents/jdk1.8.0_77"
USE_LARGEPAGE="0"
JIT="0"
opt=1

#set up to 1 and tune_arg to 0 in order to enable tuning.
#tune_arg: 1 (tune for 1 core), 2 (tune for 2 cores), ... 6 (tune for 32 cores)
#tune_arg: 7 (tune for 8 cores with Batch), 8 (tune for 8 cores with HP), 9 (tune for 8 cores with HP), 10 (tune for 32 cores with batch + HP)
tune_arg=1
up=8


#set -x
searchdir="--search-dir all:rp=$HOME/spark-app/lib --search-dir all:rp=$JAVA_HOME/bin --search-dir all:rp=$HOME/Documents/apache-spark-1.0.0/lib  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64  --search-dir all:rp=$HOME/parallel_studio/vtune_amplifier_xe_2016.2.0.444464/lib64/runtime"
num_workers="1"
co1="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA  -XX:+UseLargePages"
co2="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA -javaagent:$HOME/Documents/intrace-agent.jar"
co3="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"

if [ -z $1 ]; then
	script_test="1"
else
	script_test=$1
fi
#option of cluster configuration
opt=1
opt_end=1

#ratio of driver memory (of total provided memory)
ratio=0.1

while [ $opt -le $opt_end ] ;
do	
	case $opt in
		1) 
		vm=5
		CPU=1
		;;
		2) 
		vm=16
		CPU=2
		;;
		3) 
		vm=64
		CPU=4
		;;
		4) 
		vm=128
		CPU=8
		;;
		5) 
		vm=256
		CPU=16
		;;
		6) 
		vm=384
		CPU=32
		;;
	esac
	echo $vm
	echo $ratio 
	dm=$(awk -v m=$vm -v r=$ratio 'BEGIN { print int(1024*m*r) }')
	em=$(awk -v m=$vm -v d=$dm 'BEGIN { print 1024*m-d }')
	echo $dm
	echo $em
	bt="1"
	bt_end="1"
	while [ $bt -le $bt_end ] ;
	do
		x=0
		x_end=0
		arg="-n $num_workers -bt $bt -m remote"
		while [ $x -le $x_end ] ;
		do
			app=4
			app_end=4
			while [ $app -le $app_end ] ;
			do
				if [ $script_test == 0 ] ; then
					bash ~/start-spark.sh $opt
					sleep 15	
				fi
				
				case $app in  
				4) 
					count_number=4
					let ct4=$ct4_end+1	
					MY_PATH2=/media/spark/ProfilingData/output_word-count/$opt\_$bt
					if [ $script_test == 0 ] ; then
						mkdir -p $MY_PATH2
					fi
					if [ $USE_LARGEPAGE == 1 ] ; then
					 spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co1" -a word-count -mp $MY_PATH2 -tune $tune_arg -cn $count_number 
					else
						if [ $JIT == 1 ] ; then
							echo "run word-count with JIT logging"
							spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co2" -a word-count -mp $MY_PATH2 -tune $tune_arg -cn $count_number 
						else
							echo "run word-count with $bt, $dm, $em, $tune_arg"
							if [ $script_test == 0 ] ; then
								spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co3" -a word-count -mp $MY_PATH2 -tune $tune_arg -cn $count_number 
							fi
						fi
					fi					
					;;
				5) 
					count_number=100
					let ct3=$ct3_end+1						
					let ct4=$ct4_end+1	
					MY_PATH2=/media/spark/ProfilingData/output_fraud-detection/$opt\_$bt
					if [ $script_test == 0 ] ; then
						mkdir -p $MY_PATH2
					fi
					if [ $USE_LARGEPAGE == 1 ] ; then
					 spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co1" -mp $MY_PATH2 -tune $tune_arg  -a fraud-detection -cn $count_number-ct1 $ct1 -ct2 $ct2
					else
						if [ $JIT == 1 ] ; then
							echo "run fraud-detection with JIT logging"
							spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co2" -mp $MY_PATH2 -tune $tune_arg -a fraud-detection -cn $count_number
						else
							echo "run fraud-detection with $bt $ct1 $ct2"
							if [ $script_test == 0 ] ; then
								spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co3" -mp $MY_PATH2 -tune $tune_arg -a fraud-detection -cn $count_number
							fi
						fi
					fi					
					;;
				6) 
					count_number=2
					let ct3=$ct3_end+1
					let ct4=$ct4_end+1					
					MY_PATH2=/media/spark/ProfilingData/output_log-processing/$opt\_$bt
					if [ $script_test == 0 ] ; then
						mkdir -p $MY_PATH2
					fi
					if [ $USE_LARGEPAGE == 1 ] ; then
					 spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co1" -mp $MY_PATH2 -tune $tune_arg -a log-processing -cn $count_number -n $num_workers
					else
						if [ $JIT == 1 ] ; then
							echo "run log-processing with JIT logging"
							spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co2" -mp $MY_PATH2 -tune $tune_arg -a log-processing -cn $count_number
						else
							echo "run log-processing with $bt $ct1 $ct2"
							if [ $script_test == 0 ] ; then
								spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co3" -mp $MY_PATH2 -tune $tune_arg -a log-processing -cn $count_number
							fi
						fi
					fi					
					;;
				7) 
					count_number=4
					let ct4=$ct4_end+1
					MY_PATH2=/media/spark/ProfilingData/output_spike-detection/$opt\_$bt
					if [ $script_test == 0 ] ; then
						mkdir -p $MY_PATH2
					fi
					if [ $USE_LARGEPAGE == 1 ] ; then
					 spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co1" -mp $MY_PATH2 -tune $tune_arg -a spike-detection -cn $count_number 
					else
						if [ $JIT == 1 ] ; then
							echo "run spike-detection with JIT logging"
							spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co2" -mp $MY_PATH2 -tune $tune_arg -a spike-detection -cn $count_number 
						else
							echo "run spike-detection with $bt"
							if [ $script_test == 0 ] ; then
								spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar $arg -co "$co3" -mp $MY_PATH2 -tune $tune_arg -a spike-detection -cn $count_number 
							fi
						fi
					fi					
					;;
				8) 
					count_number=1
					let ct4=$ct4_end+1	
					MY_PATH2=/media/spark/ProfilingData/output_voipstream/$opt\_$bt
					if [ $script_test == 0 ] ; then
						mkdir -p $MY_PATH2
					fi
					if [ $USE_LARGEPAGE == 1 ] ; then
					 spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar -a voipstream $arg -co "$co1" -mp $MY_PATH2 -tune $tune_arg -cn $count_number 
					else
						if [ $JIT == 1 ] ; then
							echo "run voipstream with JIT logging"
							spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar -a voipstream $arg -co "$co2" -mp $MY_PATH2 -tune $tune_arg -cn $count_number 
						else
							echo "run voipstream with $bt"
							if [ $script_test == 0 ] ; then
								spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar -a voipstream $arg -co "$co3" -mp $MY_PATH2 -tune $tune_arg -cn $count_number 
							fi
						fi
					fi					
					;;
				9) 
					count_number=1
					let ct3=$ct3_end+1
					let ct4=$ct4_end+1
					MY_PATH2=/media/spark/ProfilingData/output_traffic-monitoring/$opt\_$bt
					if [ $script_test == 0 ] ; then
						mkdir -p $MY_PATH2
					fi
					if [ $USE_LARGEPAGE == 1 ] ; then
					 spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar -a traffic-monitoring $arg -co "$co1" -mp $MY_PATH2 -tune $tune_arg -cn $count_number
					else
						if [ $JIT == 1 ] ; then
							echo "run traffic-monitoring with JIT logging"
							spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar -a traffic-monitoring $arg -co "$co2" -mp $MY_PATH2 -tune $tune_arg -cn $count_number
						else
							echo "run traffic-monitoring with $bt $ct1 $ct2"
							if [ $script_test == 0 ] ; then
								spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar -a traffic-monitoring $arg -co "$co3" -mp $MY_PATH2 -tune $tune_arg -cn $count_number
							fi
						fi
					fi					
					;;
				10) 
					count_number=1					
					MY_PATH2=/media/spark/ProfilingData/output_linear-road-full/$opt\_$bt\_$ct4
					if [ $script_test == 0 ] ; then
						mkdir -p $MY_PATH2
					fi
					if [ $USE_LARGEPAGE == 1 ] ; then
					 spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar -a linear-road-full $arg -co "$co1" -mp $MY_PATH2 -tune $tune_arg -cn $count_number
					else
						if [ $JIT == 1 ] ; then
							echo "run linear-road-full with JIT logging"
							spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar -a linear-road-full $arg -co "$co2" -mp $MY_PATH2 -tune $tune_arg -cn $count_number
						else
							echo "run linear-road-full with $bt"
							if [ $script_test == 0 ] ; then
								spark-submit --master $URL_Master --class spark.applications.SparkRunner --driver-memory $dm\m --executor-memory $em\m --total-executor-cores $CPU /home/spark/spark-app/target/spark-applications-1.0-SNAPSHOT-jar-with-dependencies.jar -a linear-road-full $arg -co "$co3" -mp $MY_PATH2 -tune $tune_arg -cn $count_number
							fi
						fi
					fi					
					;;
				esac	 		
				if [  $x != 0 ] ; then
					rm  $MY_PATH2/spout_threadId.txt
					while [ ! -s  $MY_PATH2/spout_threadId.txt ]
					do
						sleep 20
					done
					r=$(<$MY_PATH2/spout_threadId.txt)
					echo "$r"
					jstack $r >> $MY_PATH2/threaddump_$x.txt
				fi
				case $x in
					1)	#General Exploration with CPU concurrency and Memory Bandwidth
						amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -data-limit=1024 -duration=100 $searchdir  --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/resource >> $MY_PATH2/profile1.txt;;
					2)	#general
						amplxe-cl -duration=200 -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_REQUESTS.ALL_DATA_RD:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003 -data-limit=1024 $searchdir --start-paused --resume-after 10 --target-pid $r -result-dir $MY_PATH2/general >> $MY_PATH2/profile2.txt;;
					3)	#context switch
						amplxe-cl -collect advanced-hotspots -knob collection-detail=stack-sampling -data-limit=0 $searchdir --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/context >> $MY_PATH2/profile3.txt;;
					4)	#Remote memory
						amplxe-cl -collect-with runsa -knob event config=OFFCORE_RESPONSE.ALL_DATA_RD.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.DEMAND_DATA_RD.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_CODE_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.REMOTE_HITM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.REMOTE_HIT_FORWARD_0:sa=100003 -data-limit=0 $searchdir --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/remote >> $MY_PATH2/profile4.txt;;
				esac
				if [ $script_test == 0 ] ; then
					while [ ! -s  $MY_PATH2/sink.txt ]
						do
							echo wait for application:$app
							sleep 20
						done
				fi
			let app=$app+1
			done #app loop
		let x=$x+1	
		done #x profiling loop
	let bt=$bt*2
	done #bt batch loop
let opt=$opt+1
done #opt loop	
