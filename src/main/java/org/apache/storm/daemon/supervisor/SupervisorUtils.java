/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.daemon.supervisor;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.supervisor.psim.ProcessSimulator;
import org.apache.storm.daemon.worker.Worker;
import org.apache.storm.daemon.worker.WorkerShutdown;
import org.apache.storm.daemon.worker.WorkerStatus;
import org.apache.storm.daemon.worker.heartbeat.WorkerLocalHeartbeat;
import org.apache.storm.thrift.TException;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.LocalAssignment;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.Utils;

public class SupervisorUtils {
  private static final Logger LOG = LoggerFactory
      .getLogger(SupervisorUtils.class);

  @ClojureClass(className = "backtype.storm.daemon.supervisor#assignments-snapshot")
  public static Map<String, Assignment> assignmentsSnapshot(
      StormClusterState stormClusterState, RunnableCallback callback,
      Map<String, Assignment> assignmentVersions) throws Exception {

    Set<String> stormIds = stormClusterState.assignments(callback);

    Map<String, Assignment> newAssignments = new HashMap<String, Assignment>();
    if (stormIds == null) {
      return newAssignments;
    }
    for (String sid : stormIds) {
      Assignment recordedAssignment = assignmentVersions.get(sid);
      if (recordedAssignment != null) {
        int recordedVersion = recordedAssignment.getVersion();
        int assignmentVersion =
            stormClusterState.assignmentVersion(sid, callback);
        if (assignmentVersion == recordedVersion) {
          newAssignments.put(sid, recordedAssignment);
        } else {
          Assignment newAssignment =
              stormClusterState.assignmentInfoWithVersion(sid, callback);
          newAssignments.put(sid, newAssignment);
        }
      } else {
        Assignment newAssignment =
            stormClusterState.assignmentInfoWithVersion(sid, callback);
        newAssignments.put(sid, newAssignment);
      }
    }
    return newAssignments;
  }

  /**
   * 
   * @param assignmentsSnapshot
   * @param stormId
   * @param assignmentId
   * @return worker-port ---> LocalAssignment
   */
  @ClojureClass(className = "backtype.storm.daemon.supervisor#read-my-executors")
  private static Map<Integer, LocalAssignment> readMyExecutors(
      Map<String, Assignment> assignmentsSnapshot, String stormId,
      String assignmentId) {
    Map<Integer, LocalAssignment> portToLocalAssignment =
        new HashMap<Integer, LocalAssignment>();

    Assignment assignment = assignmentsSnapshot.get(stormId);
    Map<ExecutorInfo, WorkerSlot> executorToNodePort =
        assignment.getExecutorToNodeport();
    if (executorToNodePort == null) {
      LOG.warn("No taskToWorkerSlot of assignement's " + assignment);
      return portToLocalAssignment;
    }
    Set<Map.Entry<ExecutorInfo, WorkerSlot>> entries =
        executorToNodePort.entrySet();
    for (Map.Entry<ExecutorInfo, WorkerSlot> entry : entries) {
      ExecutorInfo executor = entry.getKey();
      WorkerSlot nodePort = entry.getValue();
      Integer port = nodePort.getPort();
      String node = nodePort.getNodeId();
      if (!assignmentId.equals(node)) {
        // not localhost
        continue;
      }
      if (portToLocalAssignment.containsKey(port)) {
        LocalAssignment la = portToLocalAssignment.get(port);
        List<ExecutorInfo> taskIds = la.get_executors();
        taskIds.add(executor);
      } else {
        List<ExecutorInfo> taskIds = new ArrayList<ExecutorInfo>();
        taskIds.add(executor);
        LocalAssignment la = new LocalAssignment(stormId, taskIds);
        portToLocalAssignment.put(port, la);
      }
    }
    return portToLocalAssignment;
  }

  /**
   * 
   * @param assignmentsSnapshot
   * @param assignmentId
   * @return Returns map from port to struct containing :storm-id and :executors
   */
  @ClojureClass(className = "backtype.storm.daemon.supervisor#read-assignments")
  public static Map<Integer, LocalAssignment> readAssignments(
      Map<String, Assignment> assignmentsSnapshot, String assignmentId) {
    Map<Integer, LocalAssignment> result =
        new HashMap<Integer, LocalAssignment>();
    for (String sid : assignmentsSnapshot.keySet()) {
      Map<Integer, LocalAssignment> myExecutors =
          readMyExecutors(assignmentsSnapshot, sid, assignmentId);
      for (Map.Entry<Integer, LocalAssignment> entry : myExecutors.entrySet()) {
        Integer port = entry.getKey();
        LocalAssignment la = entry.getValue();
        if (result.containsKey(port)) {
          throw new RuntimeException(
              "Should not have multiple topologies assigned to one port");
        } else {
          result.put(port, la);
        }
      }
    }
    return result;
  }

  /**
   * 
   * @param assignmentsSnapshot
   * @param assignmentId
   * @param existingAssignment
   * @param retries
   * @return Returns map from port to struct containing :storm-id and :executors
   */
  @ClojureClass(className = "backtype.storm.daemon.supervisor#read-assignments")
  public static Map<Integer, LocalAssignment> readAssignments(
      Map<String, Assignment> assignmentsSnapshot, String assignmentId,
      Map<Integer, LocalAssignment> existingAssignment, AtomicInteger retries) {

    try {
      Map<Integer, LocalAssignment> assignments =
          readAssignments(assignmentsSnapshot, assignmentId);
      retries.set(0);
      return assignments;
    } catch (RuntimeException e) {
      if (retries.get() > 2) {
        throw e;
      } else {
        retries.addAndGet(1);
      }
      LOG.warn(e.getMessage() + ": retrying " + retries + " of 3");
      return existingAssignment;
    }
  }

  /**
   * 
   * @param assignmentsSnapshot
   * @return stormId --> stormCodeLocation
   */
  @ClojureClass(className = "backtype.storm.daemon.supervisor#read-storm-code-locations")
  public static Map<String, String> readStormCodeLocations(
      Map<String, Assignment> assignmentsSnapshot) {
    Map<String, String> result = new HashMap<String, String>();
    for (Map.Entry<String, Assignment> entry : assignmentsSnapshot.entrySet()) {
      String stormId = entry.getKey();
      Assignment assignment = entry.getValue();
      result.put(stormId, assignment.getMasterCodeDir());
    }
    return result;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#read-downloaded-storm-ids")
  public static Set<String> readDownloadedStormIds(Map conf) throws IOException {
    String path = ConfigUtil.supervisorStormdistRoot(conf);
    Set<String> result = new HashSet<String>();
    for (String stormId : CoreUtil.readDirContents(path)) {
      result.add(CoreUtil.urlDecode(stormId));
    }
    return result;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#read-worker-heartbeat")
  public static WorkerLocalHeartbeat readWorkerHeartbeat(Map conf,
      String workerId) {
    WorkerLocalHeartbeat ret = null;
    try {
      LocalState localState = ConfigUtil.workerState(conf, workerId);
      ret = (WorkerLocalHeartbeat) localState.get(Common.LS_WORKER_HEARTBEAT);
    } catch (IOException e) {
      LOG.warn("Failed to read local heartbeat for workerId : " + workerId
          + ",Ignoring exception." + CoreUtil.stringifyError(e));
    }
    return ret;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#my-worker-ids")
  public static Set<String> myWorkerIds(Map conf) {
    String path;
    try {
      path = ConfigUtil.workerRoot(conf);
    } catch (IOException e1) {
      LOG.error("Failed to get Local worker dir", e1);
      return new HashSet<String>();
    }
    return CoreUtil.readDirContents(path);
  }

  /**
   * 
   * @param conf
   * @return Returns map from worker id to heartbeat
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#read-worker-heartbeats")
  public static Map<String, WorkerLocalHeartbeat> readWorkerHeartbeats(Map conf)
      throws IOException {
    Map<String, WorkerLocalHeartbeat> workerHeartbeats =
        new HashMap<String, WorkerLocalHeartbeat>();
    Set<String> workerIds = myWorkerIds(conf);
    if (workerIds == null) {
      return workerHeartbeats;
    }
    for (String workerId : workerIds) {
      WorkerLocalHeartbeat whb = readWorkerHeartbeat(conf, workerId);
      workerHeartbeats.put(workerId, whb);
    }
    return workerHeartbeats;
  }

  @ClojureClass(className = "backtype.storm.daemon.supervisor#matches-an-assignment?")
  public static boolean matchesAnAssignment(
      WorkerLocalHeartbeat workerHeartbeat,
      Map<Integer, LocalAssignment> assignedExecutors) {
    boolean isMatch = true;
    LocalAssignment localAssignment =
        assignedExecutors.get(workerHeartbeat.getPort());
    if (localAssignment == null) {
      isMatch = false;
    } else if (!workerHeartbeat.getStormId().equals(
        localAssignment.get_topology_id())) {
      // topology id not equal
      LOG.info("topology id not equal whb={} ,localAssignment={} ",
          workerHeartbeat.getStormId(), localAssignment.get_topology_id());
      isMatch = false;
    } else if (!(workerHeartbeat.getTaskIds().equals(localAssignment
        .get_executors()))) {
      // task-id isn't equal
      LOG.info("task-id isn't equal whb={} ,localAssignment={}",
          workerHeartbeat.getTaskIds(), localAssignment.get_executors());
      isMatch = false;
    }
    return isMatch;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#download-storm-code")
  public static void downloadStormCode(Map conf, String stormId,
      String masterCodeDir, Object downloadLock) throws IOException,
      TException, AuthorizationException {
    boolean isDistributeMode = ConfigUtil.isDistributedMode(conf);
    if (isDistributeMode) {
      downloadDistributeStormCode(conf, stormId, masterCodeDir, downloadLock);
    } else {
      downloadLocalStormCode(conf, stormId, masterCodeDir, downloadLock);
    }
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.supervisor#download-storm-code#distribute")
  public static void downloadDistributeStormCode(Map conf, String stormId,
      String masterCodeDir, Object downloadLock) throws IOException,
      TException, AuthorizationException {
    // STORM_LOCAL_DIR/supervisor/tmp/(UUID)
    synchronized (downloadLock) {
      String tmproot =
          ConfigUtil.supervisorTmpDir(conf) + "/"
              + UUID.randomUUID().toString();
      String stormroot = ConfigUtil.supervisorStormdistRoot(conf, stormId);
      FileUtils.forceMkdir(new File(tmproot));

      String masterStormjarPath = ConfigUtil.masterStormjarPath(masterCodeDir);
      String supervisorStormJarPath =
          ConfigUtil.supervisorStormjarPath(tmproot);
      Utils
          .downloadFromMaster(conf, masterStormjarPath, supervisorStormJarPath);

      String masterStormcodePath =
          ConfigUtil.masterStormcodePath(masterCodeDir);
      String supervisorStormcodePath =
          ConfigUtil.supervisorStormcodePath(tmproot);
      Utils.downloadFromMaster(conf, masterStormcodePath,
          supervisorStormcodePath);

      String masterStormConfPath =
          ConfigUtil.masterStormconfPath(masterCodeDir);
      String supervisorStormconfPath =
          ConfigUtil.supervisorStormconfPath(tmproot);
      Utils.downloadFromMaster(conf, masterStormConfPath,
          supervisorStormconfPath);

      CoreUtil.extractDirFromJar(supervisorStormJarPath,
          ConfigUtil.RESOURCES_SUBDIR, tmproot);
      FileUtils.moveDirectory(new File(tmproot), new File(stormroot));
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#download-storm-code#local")
  public static void downloadLocalStormCode(Map conf, String stormId,
      String masterCodeDir, Object downloadLock) throws IOException, TException {
    String stormroot = ConfigUtil.supervisorStormdistRoot(conf, stormId);

    synchronized (downloadLock) {
      FileUtils.copyDirectory(new File(masterCodeDir), new File(stormroot));

      ClassLoader classloader = Thread.currentThread().getContextClassLoader();
      String resourcesJar = resourcesJar();
      URL url = classloader.getResource(ConfigUtil.RESOURCES_SUBDIR);
      String targetDir =
          stormroot + CoreUtil.filePathSeparator()
              + ConfigUtil.RESOURCES_SUBDIR;
      if (resourcesJar != null) {
        LOG.info("Extracting resources from jar at " + resourcesJar + " to "
            + targetDir);
        CoreUtil.extractDirFromJar(resourcesJar, ConfigUtil.RESOURCES_SUBDIR,
            stormroot);
      } else if (url != null) {
        LOG.info("Copying resources at " + url.toString() + " to " + targetDir);
        FileUtils.copyDirectory(new File(url.getFile()), (new File(targetDir)));
      }
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.supervisor#resources-jar")
  private static String resourcesJar() {
    String path = CoreUtil.currentClasspath();
    if (path == null) {
      return null;
    }
    String[] paths = path.split(File.pathSeparator);
    List<String> jarPaths = new ArrayList<String>();
    for (String s : paths) {
      if (s.endsWith(".jar")) {
        jarPaths.add(s);
      }
    }

    List<String> rtn = new ArrayList<String>();
    int size = jarPaths.size();
    for (int i = 0; i < size; i++) {
      if (CoreUtil.zipContainsDir(jarPaths.get(i), ConfigUtil.RESOURCES_SUBDIR)) {
        rtn.add(jarPaths.get(i));
      }
    }
    if (rtn.size() == 0)
      return null;
    return rtn.get(0);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.supervisor#launch-worker")
  public static void launchWorker(Map conf, SupervisorData supervisor,
      String stormId, Integer port, String workerId) throws Exception {
    if (ConfigUtil.isLocalMode(conf)) {
      launchLocalWorker(supervisor, stormId, port, workerId);
    } else {
      launchDistributedWorker(supervisor, stormId, port, workerId);
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#launch-worker#local")
  private static void launchLocalWorker(SupervisorData supervisor,
      String stormId, Integer port, String workerId) throws Exception {
    Map conf = supervisor.getConf();
    String pid = CoreUtil.uuid();
    WorkerShutdown worker =
        Worker.mkWorker(conf, supervisor.getSharedContext(), stormId,
            supervisor.getAssignmentId(), port, workerId);
    ProcessSimulator.registerProcess(pid, worker);
    supervisor.getWorkerThreadPids().put(workerId, pid);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.supervisor#launch-worker#distributed")
  private static void launchDistributedWorker(SupervisorData supervisor,
      String stormId, Integer port, String workerId) throws IOException {
    Map conf = supervisor.getConf();
    String stormLocalHostname =
        CoreUtil
            .parseString(conf.get(Config.STORM_LOCAL_HOSTNAME), "localhost");
    String stormhome = System.getProperty("storm.home");
    String stormOptions = System.getProperty("storm.options");
    String stormLogDir =
        CoreUtil.parseString(System.getProperty("storm.log.dir"), stormhome
            + CoreUtil.filePathSeparator() + "logs");
    String stormroot = ConfigUtil.supervisorStormdistRoot(conf, stormId);
    String jlp = jlp(stormroot, conf);
    String stormjar = ConfigUtil.supervisorStormjarPath(stormroot);
    Map stormConf = ConfigUtil.readSupervisorStormConf(conf, stormId);

    String classPath =
        CoreUtil.addToClasspath(CoreUtil.currentClasspath(),
            new String[] { stormjar });
    String[] topoClasspath = null;
    String topologyClassPath =
        CoreUtil.parseString(stormConf.get(Config.TOPOLOGY_CLASSPATH), null);
    if (null != topologyClassPath) {
      topoClasspath = new String[] { topologyClassPath };
      classPath =
          CoreUtil.addToClasspath(CoreUtil.currentClasspath(), topoClasspath);
    }

    // worker-childopts
    String workerChildopts = " ";
    String s = (String) conf.get(Config.WORKER_CHILDOPTS);
    if (s != null) {
      workerChildopts += substituteChildopts(s, workerId, stormId, port);
    }

    // topo-worker-childopts
    String topoWorkerChildopts = " ";
    s = (String) conf.get(Config.TOPOLOGY_WORKER_CHILDOPTS);
    if (s != null) {
      topoWorkerChildopts += substituteChildopts(s, workerId, stormId, port);
    }

    // worker-gc-childopts
    String workerGcChildopts = " ";
    s = (String) conf.get(Config.WORKER_GC_CHILDOPTS);
    if (s != null) {
      workerGcChildopts += substituteChildopts(s, workerId, stormId, port);
    }

    // topology-worker-environment
    Map<String, String> topologyWorkerEnvironment =
        new HashMap<String, String>();
    Map<String, String> env =
        (Map<String, String>) stormConf.get(Config.TOPOLOGY_ENVIRONMENT);
    if (env != null) {
      topologyWorkerEnvironment.putAll(env);
    }
    topologyWorkerEnvironment.put("LD_LIBRARY_PATH", jlp);

    String user =
        CoreUtil.parseString(stormConf.get(Config.TOPOLOGY_SUBMITTER_USER), "");

    String logfilename = "worker-" + port + ".log";

    String stormConfDir = System.getProperty("storm.conf.dir");

    StringBuilder command = new StringBuilder();
    command.append(javaCmd());
    command.append(" -server ");
    command.append(workerChildopts);
    command.append(workerGcChildopts);
    command.append(topoWorkerChildopts);
    command.append(" -Djava.library.path=");
    command.append(jlp);
    command.append(" -Dlogfile.name=");
    command.append(logfilename);
    command.append(" -Dstorm.home=");
    command.append(stormhome);
    command.append(" -Dstorm.options=");
    command.append(stormOptions);
    command.append(" -Dstorm.log.file=");
    command.append(logfilename);
    command.append(" -Dstorm.log.dir=");
    command.append(stormLogDir);
    if (stormConfDir != null) {
      command.append(" -Dlog4j.configuration=File:" + stormConfDir
          + "/storm.log4j.properties ");
      command.append(" -Dstorm.conf.dir=");
      command.append(stormConfDir);
    } else {
      command.append(" -Dlog4j.configuration=File:storm.log4j.properties");
    }
    command.append(" -Dstorm.id=");
    command.append(stormId);
    command.append(" -Dstorm.local.hostname=");
    command.append(stormLocalHostname);
    command.append(" -Dworker.id=");
    command.append(workerId);
    command.append(" -Dworker.port=");
    command.append(port);
    command.append(" -cp ");
    command.append(classPath);
    command.append(" com.tencent.jstorm.daemon.worker.Worker ");
    command.append(stormId);
    command.append(" ");
    command.append(supervisor.getAssignmentId());
    command.append(" ");
    command.append(port);
    command.append(" ");
    command.append(workerId);
    LOG.info("Launching worker with command: " + command);
    Process p = null;
    try {
      p = CoreUtil.launchProcess(command.toString(), topologyWorkerEnvironment);
    } finally {
      if (p != null) {
        IOUtils.closeQuietly(p.getOutputStream());
        IOUtils.closeQuietly(p.getInputStream());
        IOUtils.closeQuietly(p.getErrorStream());
      }
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#write-log-metadata-to-yaml-file!")
  private static void writeLogMetadataToYamlFile(String stormId, Integer port,
      Map<String, String> data, Map conf) throws IOException {
    // TODO
    CoreUtil.getLogMetadataFile(stormId, port);

  }

  @ClojureClass(className = "backtype.storm.daemon.supervisor#write-log-metadata!")
  private static void writeLogMetadata(Map stormConf, String user,
      String workerId, String stormId, Integer port, Map conf)
      throws IOException {
    List<String> logsGroups = (List<String>) stormConf.get(Config.LOGS_GROUPS);
    List<String> topologyGroups =
        (List<String>) stormConf.get(Config.TOPOLOGY_GROUPS);
    Set<String> groups = new HashSet<String>(logsGroups);

    Map<String, String> data = new HashMap<String, String>();
    data.put(Config.TOPOLOGY_SUBMITTER_USER, user);
    data.put("worker-id", workerId);
    // TODO
    data.put(Config.LOGS_GROUPS, user);
    data.put(Config.LOGS_USERS, user);
    writeLogMetadataToYamlFile(stormId, port, data, conf);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#jlp")
  private static String jlp(String stormroot, Map conf) {
    String resourceRoot =
        stormroot + File.separator + ConfigUtil.RESOURCES_SUBDIR;
    String os = System.getProperty("os.name").replaceAll("\\s+", "_");
    String arch = System.getProperty("os.arch");
    String javaLibraryPath = (String) conf.get(Config.JAVA_LIBRARY_PATH);
    String archResourceRoot = resourceRoot + File.separator + os + "-" + arch;
    return archResourceRoot + File.pathSeparator + resourceRoot
        + File.pathSeparator + javaLibraryPath;
  }

  /**
   * 
   * @param value
   * @param workerId
   * @param topologyId
   * @param port
   * @return Generates runtime childopts by replacing keys with
   *         topology-id,worker-id, port
   */
  @ClojureClass(className = "backtype.storm.daemon.supervisor#substitute-childopts")
  private static String substituteChildopts(String value, String workerId,
      String topologyId, Integer port) {
    if (value == null) {
      return null;
    }
    return value.replaceAll("%ID%", port.toString())
        .replaceAll("%WORKER-ID%", workerId)
        .replaceAll("%TOPOLOGY-ID%", topologyId)
        .replaceAll("%WORKER-PORT%", port.toString());
  }

  /**
   * 
   * @return java home path
   */
  @ClojureClass(className = "backtype.storm.daemon.supervisor#java-cmd")
  private static String javaCmd() {
    String javaHome = System.getenv("JAVA_HOME");
    if (javaHome == null) {
      return "java";
    } else {
      String filePathSeparator = CoreUtil.filePathSeparator();
      return javaHome + filePathSeparator + "bin" + filePathSeparator + "java";
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static String getChildOpts(Map stormConf, Map conf) {
    String childopts = " ";
    conf.putAll(stormConf);
    if ((String) conf.get(Config.WORKER_GC_CHILDOPTS) != null) {
      childopts += (String) conf.get(Config.WORKER_GC_CHILDOPTS);
    }
    if ((String) conf.get(Config.WORKER_CHILDOPTS) != null) {
      childopts += " " + (String) conf.get(Config.WORKER_CHILDOPTS);
    }
    if ((String) conf.get(Config.TOPOLOGY_WORKER_CHILDOPTS) != null) {
      childopts += " " + (String) conf.get(Config.TOPOLOGY_WORKER_CHILDOPTS);
    }
    return childopts;
  }

  public static Set<String> deadWorkers = new HashSet<String>();

  @ClojureClass(className = "backtype.storm.daemon.supervisor#get-dead-workers")
  public Set<String> getDeadWorkers() {
    return deadWorkers;
  }

  @ClojureClass(className = "backtype.storm.daemon.supervisor#add-dead-worker")
  public void addDeadWorker(String workerId) {
    deadWorkers.add(workerId);
  }

  @ClojureClass(className = "backtype.storm.daemon.supervisor#remove-dead-worker")
  public static void removeDeadWorker(String workerId) {
    deadWorkers.remove(workerId);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#is-worker-hb-timed-out?")
  public static boolean isWorkerHbTimedOut(int now, WorkerLocalHeartbeat hb,
      Map conf) {
    return ((now - hb.getTimeSecs()) > CoreUtil.parseInt(
        conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS), 30));
  }

  public static boolean isWorkerProcessDied(String id) {
    if (deadWorkers.contains(id)) {
      LOG.info("Worker Process " + id + " has died!");
      return true;
    }
    return false;
  }

  /**
   * 
   * @param supervisor
   * @param assignedExecutors
   * @param now
   * @return Returns map from worker id to worker heartbeat. if the heartbeat is
   *         nil, then the worker is dead (timed out or never wrote heartbeat
   * @throws Exception
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#read-allocated-workers")
  public static Map<String, StateHeartbeat> readAllocatedWorkers(
      SupervisorData supervisor,
      Map<Integer, LocalAssignment> assignedExecutors, int now)
      throws Exception {
    Map conf = supervisor.getConf();
    LocalState localState = supervisor.getLocalState();
    Map<String, StateHeartbeat> workeridHbstate =
        new HashMap<String, StateHeartbeat>();

    Map<String, WorkerLocalHeartbeat> idToHeartbeat =
        readWorkerHeartbeats(conf);
    if (idToHeartbeat.size() == 0) {
      LOG.info("No worker running now ...");
      return workeridHbstate;
    }

    @SuppressWarnings("unchecked")
    Map<String, Integer> workers =
        (Map<String, Integer>) localState.get(Common.LS_APPROVED_WORKERS);

    Set<String> approvedIds = new HashSet<String>();
    if (workers != null) {
      approvedIds = workers.keySet();
    }

    for (Map.Entry<String, WorkerLocalHeartbeat> entry : idToHeartbeat
        .entrySet()) {
      String id = entry.getKey();// workerid
      WorkerLocalHeartbeat hb = entry.getValue();
      WorkerStatus state = null;
      if (hb == null) {
        state = WorkerStatus.notStarted;
      } else if (!approvedIds.contains(id)
          || !matchesAnAssignment(hb, assignedExecutors)) {
        state = WorkerStatus.disallowed;
      } else if (hb.getProcessId() != null
          && !CoreUtil.isProcessExists(hb.getProcessId())) {
        state = WorkerStatus.processNotExists;
      } else if (isWorkerProcessDied(id) || isWorkerHbTimedOut(now, hb, conf)) {
        state = WorkerStatus.timedOut;
      } else {
        state = WorkerStatus.valid;
      }
      Object objs[] =
          { id, state.toString(), hb == null ? "null" : hb.toString(),
              String.valueOf(now) };
      LOG.debug("Worker {} is {}: {} at supervisor time-secs {} ", objs);
      workeridHbstate.put(id, new StateHeartbeat(state, hb));
    }
    return workeridHbstate;
  }

  @ClojureClass(className = "backtype.storm.daemon.supervisor#assigned-storm-ids-from-port-assignments")
  public static Set<String> assignedStormIdsFromPortAssignments(
      Map<Integer, LocalAssignment> newAssignment) {
    Set<String> assignedStormIds = new HashSet<String>();
    for (LocalAssignment entry : newAssignment.values()) {
      assignedStormIds.add(entry.get_topology_id());
    }
    return assignedStormIds;
  }

  @ClojureClass(className = "backtype.storm.daemon.supervisor#generate-supervisor-id")
  public static String generateSupervisorId() {
    return CoreUtil.uuid();
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#worker-launcher")
  public static Process workerLauncher(Map conf, String user,
      Map<String, String> environment) throws IOException {
    if (user == null) {
      throw new IllegalArgumentException(
          "User cannot be blank when calling worker-launcher.");
    }
    String stormHome = System.getProperty("storm.home");
    String wl =
        CoreUtil.parseString(conf.get(Config.SUPERVISOR_WORKER_LAUNCHER),
            stormHome + "/bin/worker-launcher");
    String command = wl + " " + user;
    return CoreUtil.launchProcess(command, environment);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.supervisor#worker-launcher-and-wait")
  public static void workerLauncherAndWait(Map conf, String user,
      Map<String, String> environment) throws IOException {
    Process process = null;
    try {
      process = workerLauncher(conf, user, environment);
      process.waitFor();
    } catch (InterruptedException e) {
      LOG.info("interrupted.");
    }
    if (process != null) {
      process.exitValue();
    }
  }

  /**
   * Launches a process owned by the given user that deletes the given path
   * recursively. Throws RuntimeException if the directory is not removed.
   * 
   * @param conf
   * @param id
   * @param user
   * @param path
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public static void rmrAsUser(Map conf, String id, String user, String path)
      throws IOException {
    Map<String, String> environment = new HashMap<String, String>();
    environment.put("rmr", path);
    workerLauncherAndWait(conf, user, environment);
    if (CoreUtil.existsFile(path)) {
      throw new RuntimeException(path + " was not deleted");
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.supervisor#try-cleanup-worker")
  public static void tryCleanupWorker(Map conf, String id, String user) {
    try {
      String workerRoot = ConfigUtil.workerRoot(conf, id);
      if (new File(workerRoot).exists()) {
        boolean supervisorRunWorkerAsUser =
            CoreUtil.parseBoolean(
                conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false);
        if (supervisorRunWorkerAsUser) {
          rmrAsUser(conf, id, user, workerRoot);
        } else {
          CoreUtil.rmr(ConfigUtil.workerHeartbeatsRoot(conf, id));
          // this avoids a race condition with worker or subprocess writing pid
          // around same time
          CoreUtil.rmpath(ConfigUtil.workerPidsRoot(conf, id));
          CoreUtil.rmpath(ConfigUtil.workerRoot(conf, id));
        }
        ConfigUtil.removeWorkerUser(conf, id);
        removeDeadWorker(id);
      }
    } catch (Exception e) {
      LOG.warn("Failed to cleanup worker " + id + ". Will retry later. For "
          + CoreUtil.stringifyError(e));
    }
  }
}
