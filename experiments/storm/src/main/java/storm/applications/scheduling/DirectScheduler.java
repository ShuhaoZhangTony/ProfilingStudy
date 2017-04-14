package storm.applications.scheduling;

import org.apache.storm.scheduler.*;
import clojure.lang.PersistentArrayMap;

import java.util.*;

/**
 * 直接分配调度器,可以分配组件到指定节点中
 * Created by zhexuan on 15/7/6.
 * <p/>
 * Update: support executor grouping and allocation with NUMA aware
 * Modified by Tony on 30/12/2015
 */
public class DirectScheduler implements IScheduler {

    @Override
    public void prepare(Map conf) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        System.out.println("DirectScheduler: begin scheduling");
        // Gets the topology which we want to schedule
        Collection<TopologyDetails> topologyDetailes;
        TopologyDetails topology;
        //作业是否要指定分配的标识
        String assignedFlag;
        Map map = null;
        Iterator<String> iterator = null;

        topologyDetailes = topologies.getTopologies();
        for (TopologyDetails td : topologyDetailes) {
            map = td.getConf();
            assignedFlag = (String) map.get("assigned_flag");


            if (assignedFlag == null) {
                System.out.println("Fail to set assignedFlag");
            }

            //如找到的拓扑逻辑的分配标为1则代表是要分配的,否则走系统的调度
            if (assignedFlag != null && assignedFlag.equals("1")) {
                System.out.println("finding topology named " + td.getName());
                topologyAssign(cluster, td, map);
            } else {
                System.out.println("topology assigned is null");
            }
        }

        System.out.println("the rest will be handled by even scheduler");
        //其余的任务由系统自带的调度器执行
        new EvenScheduler().schedule(topologies, cluster);


    }


    /**
     * 拓扑逻辑的调度
     *
     * @param cluster  集群
     * @param topology 具体要调度的拓扑逻辑
     * @param map      map配置项
     */
    private void topologyAssign(Cluster cluster, TopologyDetails topology, Map map) {
        Set<String> keys;
        PersistentArrayMap designMap;
        Iterator<String> iterator;

        iterator = null;
        // make sure the special topology is submitted,
        if (topology != null) {
            designMap = (PersistentArrayMap) map.get("design_map");
            if (designMap != null) {
                //System.out.println("design map size is " + designMap.size());
                keys = designMap.keySet();
                iterator = keys.iterator();

                System.out.println("keys size is " + keys.size());
            }

            if (designMap == null || designMap.size() == 0) {
                System.out.println("design map is null");
            }

            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
                System.out.println("Our special topology does not need scheduling.");
            } else {
                System.out.println("Our special topology needs scheduling.");

                //System.out.println("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                /*SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
                if (currentAssignment != null) {
                    System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                    System.out.println("current assignments: {}");
                }*/


                String nodeName;
                if (designMap != null && iterator != null) {
                    while (iterator.hasNext()) {
                        nodeName = iterator.next();

                        // find out all the needs-scheduling components of this topology
                        Map<String, List<ExecutorDetails>> allExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                        System.out.println("Process on " + nodeName + ", now all needs scheduling(component->executor): " + allExecutors);

                        Object o = designMap.get(nodeName);

                        /*if(o instanceof HashMap){
                            System.out.println("We obtained a hashmap from designMap");
                        }else{
                            System.out.println("O is actually:"+o.getClass().toString());
                        }*/

                        PersistentArrayMap container = (PersistentArrayMap) o;
                        Iterator<String> componentInContainer = container.keySet().iterator();
                        String componentName;
                        long no_executor_of_component;

                        //combine all executors from the container into one executor group and launch them into one worker into this socket.
                        List<ExecutorDetails> executor_groups = new LinkedList();

                        //extract information for each component of this socket container
                        while (componentInContainer.hasNext()) {
                            componentName = componentInContainer.next();
                            no_executor_of_component = (long) container.get(componentName);
                            List<ExecutorDetails> executors = allExecutors.get(componentName);//get all the executors of this component.
                            System.out.println(no_executor_of_component + " of " + componentName + ">");
                            for (int i = 0; i < no_executor_of_component; i++) {
                                executor_groups.add(executors.get(i));
                            }
                        }

                        cluster.assign(cluster.getAvailableSlots(findSupervisorByName(cluster, nodeName)).get(0), topology.getId(), executor_groups);
                    }
                }

                System.out.println("assign the rest of the executor to slot 0 of socket 0");
                // find out all the rest needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> allExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                System.out.println("The rest needs scheduling(component->executor): " + allExecutors);


                SupervisorDetails supervisor0 = findSupervisorByName(cluster, "socket0");//the targeting supervisor that is going to be schedule to. Maybe multiple of them!

                List<ExecutorDetails> combined_executors = new LinkedList();
                for (List<ExecutorDetails> executors : allExecutors.values()) {
                    combined_executors.addAll(executors);
                }
                cluster.assign(cluster.getAvailableSlots(supervisor0).get(0), topology.getId(), combined_executors);
            }
        }
    }

    private SupervisorDetails findSupervisorByName(Cluster cluster, String target_name) {
        Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
        SupervisorDetails target = null;//the targeting supervisor that is going to be schedule to. Maybe multiple of them!
        for (SupervisorDetails supervisor : supervisors) {
            @SuppressWarnings("rawtypes")
            Map meta = (Map) supervisor.getSchedulerMeta();

            if (meta != null && meta.get("name") != null) {
                // System.out.println("supervisor name:" + meta.get("name"));

                if (meta.get("name").equals(target_name)) {
                    System.out.println("Supervisor find");
                    target = supervisor;
                    break;
                }
            } else {
                System.out.println("Supervisor meta null");
            }

        }
        return target;
    }
//    /**
//     * 组件调度
//     *
//     * @param cluster        集群的信息
//     * @param topology       待调度的拓扑细节信息
//     * @param totalExecutors 组件的执行器
//     * @param componentName  组件的名称
//     * @param supervisorName 节点的名称
//     */
////    private void componentAssign(Cluster cluster, TopologyDetails topology, Map<String, List<ExecutorDetails>> totalExecutors, String componentName, String supervisorName) {
////        if (!totalExecutors.containsKey(componentName)) {
////            System.out.println("Not found " + componentName + "in the executors set.");
////        } else {
////            System.out.println("Start to schedule executors from the component of: " + componentName);
////            List<ExecutorDetails> executors = totalExecutors.get(componentName);
////
////            // find out the our "special-supervisor" from the supervisor metadata
////            Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
////            SupervisorDetails specialSupervisor = null;//the targeting supervisor that is going to be schedule to. Maybe multiple of them!
////            for (SupervisorDetails supervisor : supervisors) {
////                @SuppressWarnings("rawtypes")
////                Map meta = (Map) supervisor.getSchedulerMeta();
////
////                if (meta != null && meta.get("name") != null) {
////                    System.out.println("supervisor name:" + meta.get("name"));
////
////                    if (meta.get("name").equals(supervisorName)) {
////                        System.out.println("Supervisor finding");
////                        specialSupervisor = supervisor;
////                        break;
////                    }
////                } else {
////                    System.out.println("Supervisor meta null");
////                }
////
////            }
////
////            // found the special supervisor
////            if (specialSupervisor != null) {
////                //System.out.println("Found the special-supervisor");
////                List<WorkerSlot> availableSlots = cluster.getAvailableSlots(specialSupervisor);
////
////                /*
////                 * // 如果目标节点上已经没有空闲的slot,则进行强制释放
////                if (availableSlots.isEmpty() && !executors.isEmpty()) {
////                    for (Integer port : cluster.getUsedPorts(specialSupervisor)) {
////                        cluster.freeSlot(new WorkerSlot(specialSupervisor.getId(), port));
////                    }
////                }*/
////
////                // 重新获取可用的slot
////                //availableSlots = cluster.getAvailableSlots(specialSupervisor);
////
////                // 选取节点上第一个slot,进行分配
////                cluster.assign(availableSlots.get(0), topology.getId(), executors);//put all executors of this component into slot 0 of the dest supervisor.
////                System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
////            } else {
////                System.out.println("There is no supervisor find!!!");
////            }
////        }
////    }
}