package storm.applications.scheduling;

import clojure.lang.PersistentVector;
import org.apache.commons.collections.map.HashedMap;
import org.apache.storm.scheduler.*;

import java.util.*;

/**
 * 直接分配调度器,可以分配组件到指定节点中
 * Created by zhexuan on 15/7/6.
 * <p/>
 * Update: support executor grouping and allocation with NUMA aware
 * Modified by Tony on 30/12/2015
 */
public class FinegrainedScheduler implements IScheduler {
    List<ExecutorDetails> Sockets0_executorGroup = new LinkedList<>();
    List<ExecutorDetails> Sockets1_executorGroup = new LinkedList<>();
    List<ExecutorDetails> Sockets2_executorGroup = new LinkedList<>();
    List<ExecutorDetails> Sockets3_executorGroup = new LinkedList<>();
    @Override
    public void prepare(Map conf) {

    }


    private void clean(){
        Sockets0_executorGroup = new LinkedList<>();
        Sockets1_executorGroup = new LinkedList<>();
        Sockets2_executorGroup = new LinkedList<>();
        Sockets3_executorGroup = new LinkedList<>();
    }
    public List<ExecutorDetails> get_executorGroup(String socket) {
        switch (socket) {
            case "socket0":
                return Sockets0_executorGroup;
            case "socket1":
                return Sockets1_executorGroup;
            case "socket2":
                return Sockets2_executorGroup;
            case "socket3":
                return Sockets3_executorGroup;
        }
        System.out.println("Only four sockets are supported! (socket 0~3");
        return null;
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
      //  System.out.println("FinegrainedScheduler: begin fine grained scheduling");
        // Gets the topology which we want to schedule
        Collection<TopologyDetails> topologyDetailes;
        TopologyDetails topology;
        //作业是否要指定分配的标识
        boolean needSchedule = false;
        Map config = null;
        Iterator<String> iterator = null;

        topologyDetailes = topologies.getTopologies();
        for (TopologyDetails td : topologyDetailes) {
            config = td.getConf();
            needSchedule = (boolean) config.get("alo");

            //如找到的拓扑逻辑的分配标为1则代表是要分配的,否则走系统的调度
            if (needSchedule) {
                //System.out.println("finding topology named " + td.getName());
                topologyAssign(cluster, td, config);
            } else {
                System.out.println("topology assigned is null");
            }
        }
      //  System.out.println("the rest will be handled by even scheduler");
        //其余的任务由系统自带的调度器执行
      //  new EvenScheduler().schedule(topologies, cluster);
    }


    /**
     * 拓扑逻辑的调度
     *
     * @param cluster  集群
     * @param topology 具体要调度的拓扑逻辑
     * @param conf     map配置项
     */
    private void topologyAssign(Cluster cluster, TopologyDetails topology, Map conf) {
        Set<String> keys;
        PersistentVector designMap;
        Iterator<String> iterator;

        iterator = null;
        // make sure the special topology is submitted,
        if (topology != null) {
            designMap = (PersistentVector) (conf.get("design_map"));

            if (designMap == null || designMap.size() == 0) {
                System.out.println("design conf is null");
            } else {
                iterator = designMap.listIterator();
            }

            boolean needsScheduling = cluster.needsScheduling(topology);

            if (needsScheduling&&cluster.getNeedsSchedulingComponentToExecutors(topology).size()!=0) {
                //System.out.println("Our special topology does not need scheduling.");
                System.out.println("Our special topology needs scheduling.");
                Map<String, List<ExecutorDetails>> allExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                Map<String,Integer> component_allocateStartingPoint=new HashedMap();


                System.out.println("All needs scheduling(component->executor): " + allExecutors);
                //build combined Executor groups.  Executors being scheduled into the same socket are grouped together.
                String[] allocate_configuration;

                if (designMap != null && iterator != null) {
                    while (iterator.hasNext()) {
                        allocate_configuration = iterator.next().split(",");
                        String nodeName = allocate_configuration[0];
                        String componentName = allocate_configuration[1];
                        int allocate_number = Integer.parseInt(allocate_configuration[2]);
                        System.out.println("nodeName:"+nodeName+"componentName:"+componentName+"allocate_number:"+allocate_number);
                        List<ExecutorDetails> executor_groups;
                        List<ExecutorDetails> executors = allExecutors.get(componentName);
                        if(!component_allocateStartingPoint.containsKey(componentName)){
                           executor_groups = executors.subList(0, allocate_number);// support allocating only a subset numbers of executors.
                            component_allocateStartingPoint.put(componentName,allocate_number);
                            System.out.println(component_allocateStartingPoint.entrySet());

                        }else{
                            int start=component_allocateStartingPoint.get(componentName);
                            System.out.println("start:"+start+"componentName:"+componentName+"executors:"+executors);
                           executor_groups = executors.subList(start, start+allocate_number);// support allocating only a subset numbers of executors.
                            component_allocateStartingPoint.put(componentName,start+allocate_number);
                        }

                        System.out.println("Add:"+nodeName+","+get_executorGroup(nodeName)+","+componentName+","+executor_groups);
                        get_executorGroup(nodeName).addAll(executor_groups);
                        System.out.println("Finish:"+nodeName+","+get_executorGroup(nodeName)+","+componentName+","+executor_groups);
                    }
                }

                for (int i = 1; i < 4; i++) {//allocate executor group to each socket. Don't care socket 0.
                    String nodeName = "socket".concat(String.valueOf(i));
                    List<WorkerSlot> availableSlots = cluster.getAvailableSlots(findSupervisorByName(cluster, nodeName));
                    if (availableSlots.size() != 0 && availableSlots.get(0) != null) {
                        System.out.println("allocate " + get_executorGroup(nodeName) + "to " + nodeName);
                        cluster.assign(availableSlots.get(0), topology.getId(), get_executorGroup(nodeName));
                    } else {
                        System.out.println("Failed allocate " + get_executorGroup(nodeName) + "to " + nodeName);
                    }
                }
                System.out.println("assign the rest of the executor to slot 0 of socket 0");
                assign_rest(cluster, topology);
            }
            clean();
        }
    }


    private void assign_rest(Cluster cluster, TopologyDetails topology) {
        //boolean needsScheduling = cluster.needsScheduling(topology);
        //while(needsScheduling) {
            Map<String, List<ExecutorDetails>> allExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
            System.out.println("The rest needs scheduling(component->executor): " + allExecutors);
            SupervisorDetails supervisor0 = findSupervisorByName(cluster, "socket0");//the targeting supervisor that is going to be schedule to. Maybe multiple of them!
            List<ExecutorDetails> combined_executors = new LinkedList<>();
            for (List<ExecutorDetails> executors : allExecutors.values()) {
                combined_executors.addAll(executors);
            }
            cluster.assign(cluster.getAvailableSlots(supervisor0).get(0), topology.getId(), combined_executors);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
         //   needsScheduling = cluster.needsScheduling(topology);
        //}
    }

    private SupervisorDetails findSupervisorByName(Cluster cluster, String target_name) {
        Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
        LinkedList<SupervisorDetails> supervisor_arr = new LinkedList<>();
        SupervisorDetails target = null;//the targeting supervisor that is going to be schedule to. Maybe multiple of them!
        for (SupervisorDetails supervisor : supervisors) {
            @SuppressWarnings("rawtypes")
            Map meta = (Map) supervisor.getSchedulerMeta();

            if (meta != null && meta.get("name") != null) {
                // System.out.println("supervisor name:" + meta.get("name"));
                if (meta.get("name").equals(target_name)) {
                    System.out.println("Supervisor " + target_name + " find");
                    target = supervisor;
                    break;
                }
            } else {
                System.out.println("Supervisor meta null");
            }
            supervisor_arr.add(supervisor);
        }
        //no supervisor found so far
        if (target == null) {
            //  SupervisorDetails[] supervisor = (SupervisorDetails[]) supervisors.toArray();
            switch (target_name) {
                case "socket0": {
                    target = supervisor_arr.get(0);
                    break;
                }
                case "socket1": {
                    target = supervisor_arr.get(1);
                    break;
                }
                case "socket2": {
                    target = supervisor_arr.get(2);
                    break;
                }
                case "socket3": {
                    target = supervisor_arr.get(3);
                    break;
                }
            }
        }
        return target;
    }
}