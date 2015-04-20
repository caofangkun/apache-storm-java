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
package org.apache.storm.daemon.nimbus.threads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.curator.framework.CuratorFramework;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.nimbus.NimbusData;
import org.apache.storm.daemon.nimbus.NimbusServer;
import org.apache.storm.daemon.nimbus.NimbusUtils;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.AuthorizationException;

public class FollowerRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory
      .getLogger(FollowerRunnable.class);

  private NimbusData data;
  private NimbusServer server;
  private LeaderSelector leaderSelector;
  private int sleepTime;
  private CuratorFramework zkobj;
  private boolean state = true;
  private RunnableCallback callback;

  public FollowerRunnable(NimbusServer server, NimbusData data,
      LeaderSelector leaderSelector, int sleepTime) {
    this.data = data;
    this.server = server;
    this.leaderSelector = leaderSelector;
    this.sleepTime = sleepTime;
    callback = new RunnableCallback() {
      private static final long serialVersionUID = 1L;

      @Override
      public void run() {
        check();
      }
    };
  }

  @Override
  public void run() {
    LOG.info("Follower Thread starts!");
    while (state) {
      try {
        Thread.sleep(sleepTime);
        if (leaderSelector.hasLeadership())
          continue;
        check();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    zkobj.close();
    LOG.info("Follower Thread has closed!");
  }

  public void clean() {
    state = false;
  }

  private void check() {
    this.server.cleanUpWhenMasterChanged();
    StormClusterState clusterState = data.getStormClusterState();
    try {

      Set<String> code_ids = NimbusUtils.codeIds(data.getConf());
      Set<String> assignments_ids = clusterState.assignments(callback);

      List<String> done_ids = new ArrayList<String>();
      for (String id : code_ids) {
        if (assignments_ids.contains(id)) {
          done_ids.add(id);
        }
      }

      for (String id : done_ids) {
        assignments_ids.remove(id);
        code_ids.remove(id);
      }

      for (String topologyId : code_ids) {
        deleteLocalTopology(topologyId);
      }

      for (String id : assignments_ids) {
        Assignment assignment = clusterState.assignmentInfo(id, null);
        downloadCodeFromMaster(assignment, id);
      }
    } catch (IOException e) {
      LOG.error("Get stormdist dir error!");
      e.printStackTrace();
      return;
    } catch (Exception e) {
      LOG.error("Check error!");
      e.printStackTrace();
      return;
    }
  }

  private void deleteLocalTopology(String topologyId) throws IOException {
    String dir_to_delete =
        ConfigUtil.masterStormdistRoot(data.getConf(), topologyId);
    try {
      CoreUtil.rmr(dir_to_delete);
      LOG.info("delete:" + dir_to_delete + "successfully!");
    } catch (IOException e) {
      LOG.error("delete:" + dir_to_delete + "fail!");
      e.printStackTrace();
    }
  }

  private void downloadCodeFromMaster(Assignment assignment, String topologyId)
      throws IOException, TException, AuthorizationException {
    try {
      String localRoot =
          ConfigUtil.masterStormdistRoot(data.getConf(), topologyId);
      String masterCodeDir = assignment.getMasterCodeDir();
      CoreUtil.downloadCodeFromMaster(data.getConf(), localRoot, masterCodeDir);
    } catch (TException e) {
      // TODO Auto-generated catch block
      LOG.error(e + " downloadStormCode failed " + "stormId:" + topologyId
          + "masterCodeDir:" + assignment.getMasterCodeDir());
      throw e;
    }
    LOG.info("Finished downloading code for topology id " + topologyId
        + " from " + assignment.getMasterCodeDir());
  }

}
