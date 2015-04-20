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
package org.apache.storm.daemon.worker;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.DistributedClusterState;
import org.apache.storm.cluster.StormZkClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.worker.executor.Executor;
import org.apache.storm.daemon.worker.executor.ExecutorShutdown;
import org.apache.storm.daemon.worker.heartbeat.WorkerHeartbeatRunable;
import org.apache.storm.daemon.worker.heartbeat.WorkerLocalHeartbeatRunable;
import org.apache.storm.daemon.worker.messaging.loader.WorkerReceiveThreadShutdown;
import org.apache.storm.daemon.worker.threads.RefreshConnections;
import org.apache.storm.daemon.worker.threads.RefreshStormActive;
import org.apache.storm.daemon.worker.transfer.TransferTuplesHandler;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.AsyncLoopThread;
import org.apache.storm.util.thread.RunnableCallback;
import org.apache.storm.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.messaging.IContext;
import backtype.storm.security.auth.AuthUtils;
import backtype.storm.security.auth.IAutoCredentials;
import backtype.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.daemon.worker")
public class Worker {
  private static Logger LOG = LoggerFactory.getLogger(Worker.class);
  private WorkerData workerData;
  @SuppressWarnings("rawtypes")
  private Map conf;
  private IContext sharedMqContext;
  private String stormId;
  private String assignmentId;
  private int port;
  private String workerId;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private List<ACL> acls;
  private DistributedClusterState clusterState;
  private StormZkClusterState stormClusterState;
  private Map<String, String> initialCredentials;
  private Collection<IAutoCredentials> autoCreds;
  private Subject subject;
  private String workerProcessId;
  private static WorkerShutdown worker = null;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Worker(Map conf, IContext sharedMqContext, String stormId,
      String assignmentId, int port, String workerId) throws Exception {
    this.conf = conf;
    this.sharedMqContext = sharedMqContext;
    this.stormId = stormId;
    this.assignmentId = assignmentId;
    this.port = port;
    this.workerId = workerId;
    this.stormConf = ConfigUtil.readSupervisorStormConf(conf, stormId);
    this.stormConf = overrideLoginConfigWithSystemProperty(stormConf);
    this.acls = Utils.getWorkerACL(stormConf);
    this.clusterState = new DistributedClusterState(conf, stormConf, acls);
    this.stormClusterState = new StormZkClusterState(clusterState, acls);
    this.initialCredentials =
        stormClusterState.credentials(stormId, null).get_creds();
    this.autoCreds = AuthUtils.GetAutoCredentials(stormConf);
    this.workerProcessId = CoreUtil.processPid();
    this.subject =
        AuthUtils.populateSubject(null, autoCreds, initialCredentials);
    if (ConfigUtil.isDistributedMode(conf)) {
      String pidPath =
          ConfigUtil.workerPidPath(conf, workerId, workerProcessId);
      CoreUtil.touch(pidPath);
      LOG.info("Current worker's pid is " + pidPath);
    }
  }

  public WorkerShutdown execute() throws Exception {

    return Subject.doAs(subject,
        new PrivilegedExceptionAction<WorkerShutdown>() {

          @Override
          public WorkerShutdown run() throws Exception {
            workerData =
                new WorkerData(conf, sharedMqContext, stormId, assignmentId,
                    port, workerId, workerProcessId);

            // Worker Local Heartbeat
            RunnableCallback heartbeatFn =
                new WorkerLocalHeartbeatRunable(workerData);
            // do this here so that the worker process dies if this fails
            // it's important that worker heartbeat to supervisor ASAP when
            // launching so
            // that the supervisor knows it's running (and can move on)
            heartbeatFn.run();

            // launch heartbeat threads immediately so that slow-loading tasks
            // don't
            // cause the worker to timeout to the supervisor
            AsyncLoopThread heartbeatFnThread =
                new AsyncLoopThread(heartbeatFn, true, new DefaultKillFn(),
                    Thread.MAX_PRIORITY, true, "worker-local-heartbeat-thread");

            // Refresh Connections
            RefreshConnections refreshConnections =
                new RefreshConnections(workerData);
            AsyncLoopThread refreshConnectionsThread =
                new AsyncLoopThread(refreshConnections, true,
                    new DefaultKillFn(), Thread.MAX_PRIORITY, true,
                    "worker-refresh-connections-thread");

            // Refresh Storm Active
            RefreshStormActive refreshStormActive =
                new RefreshStormActive(workerData, conf);
            AsyncLoopThread refreshStormActiveThread =
                new AsyncLoopThread(refreshStormActive, true,
                    new DefaultKillFn(), Thread.MAX_PRIORITY, true,
                    "worker-refresh-storm-active-thread");

            // Worker Receive Thread
            WorkerReceiveThreadShutdown receiveThreadShutdown =
                new WorkerReceiveThreadShutdown(workerData);
            Shutdownable workerReceiveThreadShutdown =
                receiveThreadShutdown.launchReceiveThread();

            // shutdown executor callbacks
            List<ExecutorShutdown> shutdownExecutors = createExecutors();
            workerData.setShutdownExecutors(shutdownExecutors);

            // Worker Executors HeartBeats
            RunnableCallback whr = new WorkerHeartbeatRunable(workerData);
            whr.run();
            AsyncLoopThread whrThread =
                new AsyncLoopThread(whr, true, new DefaultKillFn(),
                    Thread.MAX_PRIORITY, true, "worker-heartbeat-thread");

            // Transfer Tuples
            TransferTuplesHandler transferTuples =
                new TransferTuplesHandler(workerData);
            AsyncLoopThread transferThread =
                new AsyncLoopThread(transferTuples, false, new DefaultKillFn(1,
                    "Async loop died!"), Thread.NORM_PRIORITY, true,
                    "worker-transfer-tuples-thread");

            AsyncLoopThread[] threads =
                { heartbeatFnThread, refreshConnectionsThread,
                    refreshStormActiveThread, whrThread, transferThread };

            return new WorkerShutdown(workerData, shutdownExecutors,
                workerReceiveThreadShutdown, threads);
          }
        });
  }

  private List<ExecutorShutdown> createExecutors() throws Exception {
    List<ExecutorShutdown> shutdownExecutors =
        new ArrayList<ExecutorShutdown>();
    Set<ExecutorInfo> executors = workerData.getExecutors();
    for (ExecutorInfo e : executors) {
      ExecutorShutdown t = Executor.mkExecutorShutdownDameon(workerData, e);
      shutdownExecutors.add(t);
    }
    return shutdownExecutors;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.worker#override-login-config-with-system-property")
  private Map overrideLoginConfigWithSystemProperty(Map conf) {
    String loginConfFile =
        System.getProperty("java.security.auth.login.config");
    if (loginConfFile != null) {
      conf.put("java.security.auth.login.config", loginConfFile);
    }
    return conf;
  }

  /**
   * TODO: should worker even take the storm-id as input? this should be
   * deducable from cluster state (by searching through assignments) what about
   * if there's inconsistency in assignments? -> but nimbus should guarantee
   * this consistency
   * 
   * @param conf
   * @param sharedMqContext
   * @param stormId
   * @param assignmentId
   * @param port
   * @param workerId
   * @return
   * @throws Exception
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-worker")
  public static WorkerShutdown mkWorker(Map conf, IContext sharedMqContext,
      String stormId, String assignmentId, int port, String workerId)
      throws Exception {
    Object[] objs =
        { stormId, assignmentId, String.valueOf(port), workerId,
            conf.toString() };
    LOG.info("Launching worker for {} on {}:{} with id {} and conf {}", objs);
    Worker w =
        new Worker(conf, sharedMqContext, stormId, assignmentId, port, workerId);
    return w.execute();
  }

  /**
   * adds the user supplied function as a shutdown hook for cleanup. Also adds a
   * function that sleeps for a second and then sends kill -9 to process to
   * avoid any zombie process in case cleanup function hangs.
   */
  @ClojureClass(className = "backtype.storm.util#add-shutdown-hook-with-force-kill-in-1-sec")
  private static void addShutdownHookWithForceKillInOneSec() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        if (worker != null) {
          worker.shutdown();
        }
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          CoreUtil.sleepSecs(1);
          Runtime.getRuntime().halt(20);
        } catch (InterruptedException e) {
          LOG.error(CoreUtil.stringifyError(e));
        }
      }
    });
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.worker#-main")
  public static void main(String[] args) {
    if (args.length != 4) {
      LOG.error("args length should be 4!");
      return;
    }

    String stormId = args[0];
    String assignmentId = args[1];
    String portStr = args[2];
    String workerId = args[3];
    Map conf = Utils.readStormConfig();
    ConfigUtil.validateDistributedMode(conf);
    try {
      worker =
          mkWorker(conf, null, stormId, assignmentId,
              Integer.parseInt(portStr), workerId);
      addShutdownHookWithForceKillInOneSec();
    } catch (Exception e) {
      Object[] eobjs =
          { stormId, portStr, workerId, CoreUtil.stringifyError(e) };
      LOG.error(
          "Failed to create worker topologyId:{},port:{},workerId:{} for {}. ",
          eobjs);
      System.exit(1);
    }
  }
}
