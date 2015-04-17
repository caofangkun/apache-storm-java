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
package org.apache.storm.cluster;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.storm.ClojureClass;
import org.apache.storm.curator.framework.CuratorFramework;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.zk.DefaultWatcherCallBack;
import org.apache.storm.zk.WatcherCallBack;
import org.apache.storm.zk.Zookeeper;
import org.apache.storm.zookeeper.CreateMode;
import org.apache.storm.zookeeper.KeeperException;
import org.apache.storm.zookeeper.Watcher.Event.EventType;
import org.apache.storm.zookeeper.Watcher.Event.KeeperState;
import org.apache.storm.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state")
public class DistributedClusterState implements ClusterState {
  private static Logger LOG = LoggerFactory
      .getLogger(DistributedClusterState.class);
  private Zookeeper zkobj = new Zookeeper();
  private CuratorFramework zk;
  private WatcherCallBack watcher;
  private ConcurrentHashMap<String, ClusterStateCallback> callbacks;
  private AtomicBoolean active;

  @SuppressWarnings("rawtypes")
  public DistributedClusterState(Map conf) throws Exception {
    this(conf, null, null);
  }

  @SuppressWarnings("rawtypes")
  public DistributedClusterState(Map conf, Map authConf, List<ACL> acls)
      throws Exception {

    int stormZookeeperPort =
        CoreUtil.parseInt(conf.get(Config.STORM_ZOOKEEPER_PORT), 2181);
    List<String> stormZookeeperServers =
        CoreUtil.parseList(conf.get(Config.STORM_ZOOKEEPER_SERVERS),
            Arrays.asList("localhost"));
    String stormZookeeperRoot =
        CoreUtil.parseString(conf.get(Config.STORM_ZOOKEEPER_ROOT), "/storm");

    CuratorFramework _zk =
        zkobj.mkClient(conf, stormZookeeperServers, stormZookeeperPort, "",
            new DefaultWatcherCallBack(), conf);
    zkobj.mkdirs(_zk, stormZookeeperRoot, acls);
    _zk.close();

    callbacks = new ConcurrentHashMap<String, ClusterStateCallback>();
    active = new AtomicBoolean(true);
    watcher = new WatcherCallBack() {
      @SuppressWarnings("unchecked")
      @Override
      public void execute(KeeperState state, EventType type, String path) {
        if (active.get()) {
          if (!(state.equals(KeeperState.SyncConnected))) {
            LOG.warn("Received event " + state + ":" + type + ":" + path
                + " with disconnected Zookeeper.");
          } else {
            LOG.info("Received event " + state + ":" + type + ":" + path);
          }
          if (!type.equals(EventType.None)) {
            for (ClusterStateCallback callback : callbacks.values()) {
              callback.execute(type, path);
            }
          }
        }
      }
    };
    zk =
        zkobj.mkClient(conf, stormZookeeperServers, stormZookeeperPort,
            stormZookeeperRoot, watcher, conf);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#register")
  public String register(ClusterStateCallback callback) {
    String id = CoreUtil.uuid();
    this.callbacks.put(id, callback);
    return id;
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#unregister")
  public ClusterStateCallback unregister(String id) {
    return this.callbacks.remove(id);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#set-ephemeral-node")
  public void setEphemeralNode(String path, byte[] data, List<ACL> acls)
      throws Exception {
    zkobj.mkdirs(zk, CoreUtil.parentPath(path), acls);
    if (zkobj.exists(zk, path, false)) {
      try {
        zkobj.setData(zk, path, data);
      } catch (KeeperException.NoNodeException e) {
        LOG.warn(
            "Ephemeral node disappeared between checking for existing and setting data",
            e);
        zkobj.createNode(zk, path, data, CreateMode.EPHEMERAL, acls);
      }

    } else {
      zkobj.createNode(zk, path, data, CreateMode.EPHEMERAL, acls);
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#create-sequential")
  public void createSequential(String path, byte[] data, List<ACL> acls)
      throws Exception {
    zkobj.createNode(zk, path, data, CreateMode.PERSISTENT_SEQUENTIAL, acls);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#set-data")
  public void setData(String path, byte[] data, List<ACL> acls)
      throws Exception {
    // note: this does not turn off any existing watches
    if (zkobj.exists(zk, path, false)) {
      zkobj.setData(zk, path, data);
    } else {
      zkobj.mkdirs(zk, ServerUtils.parentPath(path), acls);
      zkobj.createNode(zk, path, data, CreateMode.PERSISTENT, acls);
    }
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#delete-node")
  public void deleteNode(String path) throws Exception {
    zkobj.deletereRcursive(zk, path);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#get-data")
  public byte[] getData(String path, boolean watch) throws Exception {
    return zkobj.getData(zk, path, watch);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#get-data-with-version")
  public DataInfo getDataWithVersion(String path, boolean watch)
      throws Exception {
    return zkobj.getDataWithVersion(zk, path, watch);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#get-version")
  public int getVersion(String path, boolean watch) throws Exception {
    return zkobj.getVersion(zk, path, watch);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#get-children")
  public Set<String> getChildren(String path, boolean watch) throws Exception {
    return zkobj.getChildren(zk, path, watch);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#mkdirs")
  public void mkdirs(String path, List<ACL> acls) throws Exception {
    zkobj.mkdirs(zk, path, acls);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#exists-node?")
  public boolean existsNode(String path, boolean watch) throws Exception {
    return zkobj.existsNode(zk, path, watch);
  }

  @Override
  @ClojureClass(className = "backtype.storm.cluster#mk-distributed-cluster-state#close")
  public void close() {
    this.active.set(false);
    zk.close();
  }

  @Override
  public LeaderSelector mkLeaderSelector(String path,
      LeaderSelectorListener listener) {
    return zkobj.mkLeaderSelector(zk, path, listener);
  }

  @Override
  public boolean isClosed() {
    return !(this.active.get());
  }
}
