#!/bin/bash
#set -x
JAVA_HOME="/home/storm/Documents/jdk1.8.0_77"
JAR_PATH="/home/storm/storm-app/target/storm-applications-1.0-SNAPSHOT-jar-with-dependencies.jar"
MAIN="storm.applications.StormRunner"
JIT=0
#set up to >1 and tune_arg to 0 in order to enable tuning.
#tunearg: 1 (tune for 1 core), 2 (tune for 2 cores), ... 6 (tune for 32 cores)
#tune_arg: 7 (tune for 8 cores with Batch), 8 (tune for 8 cores with HP), 9 (tune for 8 cores with HP), 10 (tune for 32 cores with batch + HP)
tune_arg=0
#opt: processor configuration
opt=4
num_workers=1
up=2
app_start=1
app_start_end=1
x_start=0
x_start_end=0
duration=100
vtune_duration=50
placement="false"
alo_plan_start=0
alo_plan_end=0
case $opt in
	1) 
	USE_LARGEPAGE=0
	vm="8";;
	2) 
	USE_LARGEPAGE=0
	vm="16";;
	3) 
	USE_LARGEPAGE=0
	vm="64";;
	4)
	USE_LARGEPAGE=0
	vm="100";;#make sure it can fit into one socket.
	5) 
	USE_LARGEPAGE=0
	vm="200";;#make sure it can fit into two sockets.
	6) 
	USE_LARGEPAGE=0
	vm=$((400 / num_workers));;#make sure it can fit into four sockets.
	7) 
	USE_LARGEPAGE=0		
	vm="100";;#Batch in 1 socket
	8) 
	USE_LARGEPAGE=1
	vm="100";;#HugePage in 1 socket
esac

xmx=Xmx$vm\g
xms=Xms$vm\g
searchdir="--search-dir all:rp=/home/storm/storm-app/lib --search-dir all:rp=$JAVA_HOME/bin --search-dir all:rp=/home/storm/Documents/apache-storm-1.0.1/lib  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server  --search-dir all:rp=/usr/lib/jvm/java-8-oracle/jre/lib/amd64  --search-dir all:rp=/home/tony/parallel_studio/vtune_amplifier_xe_2016.2.0.444464/lib64/runtime"



if [ $vm -gt 100 ] ; then
	co1="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA  -XX:+UseLargePages -XX:LargePageSizeInBytes=2m"
	co2="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA  -agentlib:jprof=gencalib,logpath=/home/storm/log"
	co3="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseNUMA"
else
	co1="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseLargePages -XX:LargePageSizeInBytes=2m"
	co2="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -agentlib:jprof=gencalib,logpath=/home/storm/log"
	co3="-server -$xmx -$xms -XX:+UseG1GC -XX:MaxGCPauseMillis=200 "
fi

arg1="$arg -co \"$co1\""
arg2="$arg -co \"$co2\""
arg3="$arg -co \"$co3\""

if [ -z $1 ]; then
	script_test=1
else
	script_test=$1
fi


alo_plan=$alo_plan_start
while [ $alo_plan -le $alo_plan_end ] ;
do
bt=1
bt_end=1
while [ $bt -le $bt_end ] ;
do
	arg="-n $num_workers -bt $bt -m remote -r $duration"
	x=$x_start
	x_end=$x_start_end
	while [ $x -le $x_end ] ;
	do
		#app from 1:4 
		#SG(1),AC,TM,LR(4)
		app=$app_start
		app_end=$app_start_end
		while [ $app -le $app_end ] ;
		do	
			ct4=1
			ct4_end=$up
			while [ $ct4 -le  $ct4_end ] ;
			do		
				ct3=1
				ct3_end=$up
				while [ $ct3 -le  $ct3_end ] ;
				do
					ct2=1
					ct2_end=$up
					while [ $ct2 -le  $ct2_end ] ;
					do 
						ct1=2
						ct1_end=$up
						while [ $ct1 -le  $ct1_end ] ;
						do				
				count_number=1000
				case $app in  
					1) 
						#disable ct2,3 and 4.
						let ct2_end=1
						let ct2=$ct2_end+1
						let ct3_end=1
						let ct3=$ct3_end+1
						let ct4_end=1
						let ct4=$ct4_end+1
						
						echo "run stream grep with option:$opt, batch:$bt thread:$ct1"
						MY_PATH2=/media/tony/ProfilingData/output_stream-greping/$opt\_$bt\_$ct1
						
						if [ $script_test == 0 ] ; then
							mkdir -p $MY_PATH2
						
							if [ $USE_LARGEPAGE == 1 ] ; then
							 storm jar $JAR_PATH $MAIN mytest $arg -co "$co1" -a streamgrep -mp $MY_PATH2 -tune $tune_arg -cn $count_number -ct1 $ct1
							else
								if [ $JIT == 1 ] ; then
									echo "run stream-grep with JIT logging"
							 		storm jar $JAR_PATH $MAIN mytest $arg -co "$co2" -a streamgrep -mp $MY_PATH2 -tune $tune_arg -cn $count_number -ct1 $ct1
								else
						 			storm jar $JAR_PATH $MAIN mytest $arg -co "$co3" -a "streamgrep" -mp $MY_PATH2 -tune $tune_arg -cn $count_number -ct1 $ct1
								fi
							fi
						fi					
						;;
					2)
						echo "run ac, not supported yet"
						;;
					3) 
						#disable ct3 and 4.
						let ct3_end=1
						let ct3=$ct3_end+1
						let ct4_end=1
						let ct4=$ct4_end+1
						
						echo "run traffic monitoring with option:$opt, batch:$bt thread:$ct1, $ct2"
						MY_PATH2=/media/tony/ProfilingData/output_traffic-monitoring/$opt\_$bt\_$ct1\_$ct2
						
						if [ $script_test == 0 ] ; then
							mkdir -p $MY_PATH2					
							if [ $USE_LARGEPAGE == 1 ] ; then
							 storm jar $JAR_PATH $MAIN mytest $arg -co "$co1" -a traffic-monitoring -mp $MY_PATH2 -tune $tune_arg -cn $count_number -ct1 $ct1 -ct2 $ct2
							else
								if [ $JIT == 1 ] ; then
									echo "run traffic monitoring with JIT logging"
							 		storm jar $JAR_PATH $MAIN mytest $arg -co "$co2" -a traffic-monitoring -mp $MY_PATH2 -tune $tune_arg -cn $count_number -ct1 $ct1 -ct2 $ct2
								else
							 		storm jar $JAR_PATH $MAIN mytest $arg -co "$co3" -a traffic-monitoring -mp $MY_PATH2 -tune $tune_arg -cn $count_number -ct1 $ct1 -ct2 $ct2
								fi
							fi
						fi					
						;;
					4) 
						#disable ct 4.
						let ct4_end=1
						let ct4=$ct4_end+1
						
						echo "run linear-road-full with option:$opt, batch:$bt thread:$ct1, $ct2, $ct3"
						MY_PATH2=/media/tony/ProfilingData/output_linear-road-full/$opt\_$bt\_$ct1\_$ct2\_$ct3
						
						if [ $script_test == 0 ] ; then
							mkdir -p $MY_PATH2					
							if [ $USE_LARGEPAGE == 1 ] ; then
							 storm jar $JAR_PATH $MAIN mytest $arg -co "$co1" -a linear-road-full -mp $MY_PATH2 -tune $tune_arg -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3
							else
								if [ $JIT == 1 ] ; then
									echo "run traffic monitoring with JIT logging"
							 		storm jar $JAR_PATH $MAIN mytest $arg -co "$co2" -a linear-road-full -mp $MY_PATH2 -tune $tune_arg -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3
								else
							 		storm jar $JAR_PATH $MAIN mytest $arg -co "$co3" -a linear-road-full -mp $MY_PATH2 -tune $tune_arg -cn $count_number -ct1 $ct1 -ct2 $ct2 -ct3 $ct3
								fi
							fi
						fi					
						;;
			esac	 		
			if [ $script_test == 0 ] ; then
			if [  $x != 0 ] ; then
				rm  $MY_PATH2/sink_threadId.txt
				while [ ! -s  $MY_PATH2/sink_threadId.txt ]
				do
					sleep 1
				done
					r=$(<$MY_PATH2/sink_threadId.txt)
					#r=$(<$MY_PATH2/vd_threadId.txt)
				#echo "$r"
				jstack $r > $MY_PATH2/threaddump_$x.txt
				#jps | grep worker > wpid
				#awk '{print $1}' wpid > worker.pid
				#while IFS= read -r line; do
			 	#	jstack $line > $MY_PATH2/threaddump_$line.txt
				#done < worker.pid

			fi
			case $x in
			1)	#General Exploration with CPU concurrency and Memory Bandwidth
				#amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -data-limit=1024 -duration=100 $searchdir  --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/resource >> $MY_PATH2/profile1.txt;;
				amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -data-limit=1024 -duration=$vtune_duration $searchdir -result-dir $MY_PATH2/resource >> $MY_PATH2/profile1.txt;;
			2)	#general
				#amplxe-cl -duration=200 -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_REQUESTS.ALL_DATA_RD:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_HIT.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003 -data-limit=1024 $searchdir --start-paused --resume-after 10 --target-pid $r -result-dir $MY_PATH2/general >> $MY_PATH2/profile2.txt;;
				#amplxe-cl -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_REQUESTS.ALL_DATA_RD:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_HIT.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003 -data-limit=1024 $searchdir -duration=$vtune_duration -result-dir $MY_PATH2/general >> $MY_PATH2/profile2.txt &
				amplxe-cl -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_REQUESTS.ALL_DATA_RD:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_HIT.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003 -data-limit=1024 $searchdir -duration=$vtune_duration  --target-pid $r -result-dir $MY_PATH2/general >> $MY_PATH2/profile2.txt &
			;;
			6)	#context switch
				amplxe-cl -collect advanced-hotspots -knob collection-detail=stack-sampling -data-limit=0 $searchdir --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/context >> $MY_PATH2/profile6.txt;;
			4)	#Remote memory
				#amplxe-cl -collect-with runsa -knob event config=OFFCORE_RESPONSE.ALL_DATA_RD.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.DEMAND_DATA_RD.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_CODE_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.REMOTE_HITM_0:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.REMOTE_HIT_FORWARD_0:sa=100003 -data-limit=0 $searchdir --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/remote >> $MY_PATH2/profile4.txt;;
				 amplxe-cl -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.DEMAND_CODE_RD.LLC_MISS.REMOTE_DRAM_0:sa=100003,OFFCORE_RESPONSE.DEMAND_DATA_RD.LLC_MISS.REMOTE_DRAM_0:sa=100003 $searchdir --target-pid $r -result-dir $MY_PATH2/general >> $MY_PATH2/profile4.txt;;
			5)	#intel PMU
				#toplev.py -l3 --no-desc -x, sleep 100 -o $MY_PATH2/profile5.txt
				toplev.py -l3 sleep 10
				;;
			3) 	#ocperf
				./profile_RMA.sh $MY_PATH2/profile3.txt $vtune_duration
				;;
			7)	#IMC
				perf stat -e uncore_imc_0/event=0x4,umask=0x3/,uncore_imc_1/event=0x4,umask=0x3/,uncore_imc_4/event=0x4,umask=0x3/,uncore_imc_5/event=0x4,umask=0x3/ -a –per-socket
				;;
			8)	#ocperf PID
				 ./profile_RMA_PID.sh $MY_PATH2/profile8.txt $r
				;;
			9)	#ocperf PID LLC
				 ./profile_LLC_PID.sh $MY_PATH2/profile9.txt $r
			esac
				while [ ! -s  $MY_PATH2/elapsed_time.txt ]
					do
						echo wait for application:$app
						sleep 5
					done
				case $app in
				1)
					python /home/storm/Documents/StormUIAPI/query.py  streamgrep >> $MY_PATH2/query.txt
				;;
				2)
					python /home/storm/Documents/StormUIAPI/query.py  advertise >> $MY_PATH2/query.txt
				;;
				3)
					python /home/storm/Documents/StormUIAPI/query.py  traffic-monitoring >> $MY_PATH2/query.txt
				;;
				4)
					python /home/storm/Documents/StormUIAPI/query.py linear-road-full >> $MY_PATH2/query.txt
				;;
				esac
				if [  $x != 0 ] ; then
                                        mkdir -p $HOME/vtune/$app/$x
                                        mv $MY_PATH2 $HOME/vtune/$app/$x
                                fi

			fi			
						sleep 2
						let ct1=$ct1*2
						done # end of thread 1
					let ct2=$ct2*2
					done # end of thread 2
				let ct3=$ct3*2
				done # end of thread 3
			let ct4=$ct4*2
			done # end of thread 4
		let app=$app+1
		done #app loop
	let x=$x+1	
	done #x profiling loop
let bt=$bt*2
done #bt batch loop
let alo_plan=$alo_plan+1
done # alo plan loop
