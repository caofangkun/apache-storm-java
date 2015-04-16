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
package org.apache.storm.daemon.nimbus;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.scheduler.INimbus;
import backtype.storm.security.auth.ThriftServer;
import backtype.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.daemon.nimbus")
public class NimbusServer {
  private static final Logger LOG = LoggerFactory.getLogger(NimbusServer.class);
  private NimbusData data;
  private ServiceHandler serviceHandler;
  private ThriftServer server;

  @ClojureClass(className = "backtype.storm.daemon.nimbus#main")
  public static void main(String[] args) {
    NimbusServer instance = new NimbusServer();
    INimbus iNimbus = new StandaloneNimbus();
    try {
      instance.launch(iNimbus);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
      e.printStackTrace();
      System.exit(-1);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#-launch")
  private void launch(INimbus inimbus) throws Exception {
    Map conf = Utils.readStormConfig();
    Map authConf = ConfigUtil.readYamlConfig("storm-cluster-auth.yaml", false);
    if (authConf != null) {
      conf.putAll(authConf);
    }
    launchServer(conf, inimbus);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#launch-server!")
  public ServiceHandler launchServer(final Map conf, final INimbus inimbus)
      throws Exception {
    ConfigUtil.validateDistributedMode(conf);
    addShutdownHookWithForceKillInOneSec();
    LOG.info("Starting Nimbus server...");
    serviceHandler = new ServiceHandler(conf, inimbus);
    server =
        new ThriftServer(conf, new Nimbus.Processor<Iface>(serviceHandler),
            ThriftConnectionType.NIMBUS);
    server.serve();
    while (!data.getStormClusterState().leaderExisted()) {
      Thread.sleep(10000);
    }
    return serviceHandler;
  }

  public StormClusterState getStormClusterState() {
    return data.getStormClusterState();
  }

  public ServiceHandler getServiceHandler() {
    return serviceHandler;
  }

  /**
   * adds the user supplied function as a shutdown hook for cleanup. Also adds a
   * function that sleeps for a second and then sends kill -9 to process to
   * avoid any zombie process in case cleanup function hangs.
   */
  @ClojureClass(className = "backtype.storm.util#add-shutdown-hook-with-force-kill-in-1-sec")
  private void addShutdownHookWithForceKillInOneSec() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        if (serviceHandler != null) {
          serviceHandler.shutdown();
        }
        if (server != null) {
          server.stop();
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

  public void cleanUpWhenMasterChanged() {
    try {
      if (serviceHandler != null) {
        serviceHandler.shutdown();
        serviceHandler = null;
      }
      if (server != null) {
        server.stop();
        server = null;
      }
      if (data != null) {
        ScheduledExecutorService scheduExec = data.getScheduExec();
        if (scheduExec != null)
          scheduExec.shutdown();
        ScheduledExecutorService new_scheduExec =
            Executors.newScheduledThreadPool(NimbusData.SCHEDULE_THREAD_NUM);
        data.setScheduExec(new_scheduExec);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      System.exit(-1);
    }
  }

}
