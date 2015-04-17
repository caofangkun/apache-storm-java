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
package org.apache.storm.zk;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.DataInfo;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import src.jvm.backtype.storm.Config;
import src.jvm.backtype.storm.utils.Utils;
import src.jvm.backtype.storm.utils.ZookeeperAuthInfo;

@ClojureClass(className = "backtype.storm.zookeeper")
public class Zookeeper {

  private static Logger LOG = LoggerFactory.getLogger(Zookeeper.class);

  @SuppressWarnings("rawtypes")
  public CuratorFramework mkClient(Map conf, List<String> servers, Object port,
      String root) {
    return mkClient(conf, servers, port, root, new DefaultWatcherCallBack(),
        null);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.zookeeper#mk-client")
  public CuratorFramework mkClient(Map conf, List<String> servers, Object port,
      String root, final WatcherCallBack watcher, Map authConf) {
    CuratorFramework fk = null;
    if (authConf == null) {
      fk = Utils.newCurator(conf, servers, port, root);
    } else if (authConf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME) == null) {
      fk = Utils.newCurator(conf, servers, port, root);
    } else {
      fk =
          Utils.newCurator(conf, servers, port, root, new ZookeeperAuthInfo(
              authConf));
    }

    fk.getCuratorListenable().addListener(new CuratorListener() {
      @Override
      public void eventReceived(CuratorFramework _fk, CuratorEvent e)
          throws Exception {

        if (e.getType().equals(CuratorEventType.WATCHED)) {
          WatchedEvent event = e.getWatchedEvent();

          watcher.execute(event.getState(), event.getType(), event.getPath());
        }

      }
    });

    // fk.getUnhandledErrorListenable().addListener(new UnhandledErrorListener()
    // {
    // @Override
    // public void unhandledError(String msg, Throwable error) {
    // String errmsg =
    // "Unrecoverable Zookeeper error, halting process: " + msg;
    // LOG.error(errmsg, error);
    // Utils.halt_process(1, "Unrecoverable Zookeeper error");
    //
    // }
    // });

    fk.start();

    return fk;
  }

  @ClojureClass(className = "backtype.storm.zookeeper#create-node")
  public String createNode(CuratorFramework zk, String path, byte[] data,
      org.apache.zookeeper.CreateMode mode, List<ACL> acls)
      throws RuntimeException {
    String result = null;
    try {
      String npath = CoreUtil.normalizePath(path);
      result = zk.create().withMode(mode).withACL(acls).forPath(npath, data);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.zookeeper#create-node")
  public String createNode(CuratorFramework zk, String path, byte[] data,
      List<ACL> acls) throws Exception {
    return createNode(zk, path, data, CreateMode.PERSISTENT, acls);
  }

  @ClojureClass(className = "backtype.storm.zookeeper#exists-node?")
  public boolean existsNode(CuratorFramework zk, String path, boolean watch)
      throws RuntimeException {
    Stat stat = null;
    try {
      if (watch) {
        stat = zk.checkExists().watched().forPath(CoreUtil.normalizePath(path));
      } else {
        stat = zk.checkExists().forPath(CoreUtil.normalizePath(path));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return stat != null;
  }

  @ClojureClass(className = "backtype.storm.zookeeper#delete-node")
  public void deleteNode(CuratorFramework zk, String path) throws Exception {
    deleteNode(zk, path, false);
  }

  @ClojureClass(className = "backtype.storm.zookeeper#delete-node")
  public void deleteNode(CuratorFramework zk, String path, boolean isForce)
      throws RuntimeException {
    try {
      zk.delete().forPath(CoreUtil.normalizePath(path));
    } catch (KeeperException.NoNodeException e) {
      if (!isForce) {
        throw new RuntimeException(e);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @ClojureClass(className = "backtype.storm.zookeeper#mkdirs")
  public void mkdirs(CuratorFramework zk, String path, List<ACL> acls)
      throws RuntimeException {

    String npath = CoreUtil.normalizePath(path);

    if (npath.equals("/") || existsNode(zk, npath, false)) {
      return;
    }
    mkdirs(zk, CoreUtil.parentPath(npath), acls);
    createNode(zk, npath, CoreUtil.barr((byte) 7), CreateMode.PERSISTENT, acls);
  }

  @ClojureClass(className = "backtype.storm.zookeeper#get-data")
  public byte[] getData(CuratorFramework zk, String path, boolean watch)
      throws RuntimeException {
    String npath = CoreUtil.normalizePath(path);
    byte[] buffer = null;
    try {
      if (existsNode(zk, npath, watch)) {
        if (watch) {
          buffer = zk.getData().watched().forPath(npath);
        } else {
          buffer = zk.getData().forPath(npath);
        }
      }
    } catch (KeeperException.NoNodeException e) {
      // this is fine b/c we still have a watch from the successful exists call
      LOG.error("zookeeper getdata for path" + path, e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return buffer;
  }

  @ClojureClass(className = "backtype.storm.zookeeper#get-data-with-version")
  public DataInfo getDataWithVersion(CuratorFramework zk, String path,
      boolean watch) throws RuntimeException {
    Stat stats = new Stat();
    String npath = CoreUtil.normalizePath(path);
    byte[] data = null;
    try {
      if (existsNode(zk, npath, watch)) {
        if (watch) {
          data = zk.getData().storingStatIn(stats).watched().forPath(npath);
        } else {
          data = zk.getData().storingStatIn(stats).forPath(npath);
        }
      }
      return new DataInfo(data, stats.getVersion());
    } catch (KeeperException.NoNodeException e) {
      // this is fine b/c we still have a watch from the successful exists call
      LOG.error("zookeeper getdata for path" + path, e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @ClojureClass(className = "backtype.storm.zookeeper#get-version")
  public int getVersion(CuratorFramework zk, String path, boolean watch)
      throws Exception {
    String npath = CoreUtil.normalizePath(path);
    Stat stat = null;
    if (watch) {
      stat = zk.checkExists().watched().forPath(npath);
    } else {
      stat = zk.checkExists().forPath(npath);
    }
    if (stat != null) {
      return stat.getVersion();
    }
    return 0;
  }

  @ClojureClass(className = "backtype.storm.zookeeper#get-children")
  public Set<String> getChildren(CuratorFramework zk, String path, boolean watch)
      throws Exception {

    String npath = CoreUtil.normalizePath(path);
    List<String> paths = null;
    if (watch) {
      paths = zk.getChildren().watched().forPath(npath);

    } else {
      paths = zk.getChildren().forPath(npath);
    }
    return new HashSet<String>(paths);
  }

  @ClojureClass(className = "backtype.storm.zookeeper#set-data")
  public Stat setData(CuratorFramework zk, String path, byte[] data)
      throws Exception {
    String npath = CoreUtil.normalizePath(path);
    Stat stat = zk.setData().forPath(npath, data);
    return stat;
  }

  @ClojureClass(className = "backtype.storm.zookeeper#exists")
  public boolean exists(CuratorFramework zk, String path, boolean watch)
      throws Exception {
    return existsNode(zk, path, watch);
  }

  @ClojureClass(className = "backtype.storm.zookeeper#delete-recursive")
  public void deletereRcursive(CuratorFramework zk, String path)
      throws Exception {

    String npath = CoreUtil.normalizePath(path);
    if (existsNode(zk, npath, false)) {
      Set<String> children = new HashSet<String>();
      try {
        children = getChildren(zk, npath, false);
      } catch (KeeperException.NoNodeException e) {
        children = new HashSet<String>();
      }
      for (String child : children) {
        String childFullPath = CoreUtil.fullPath(npath, child);
        deletereRcursive(zk, childFullPath);
      }
      deleteNode(zk, npath, true);
    }
  }

  public LeaderSelector mkLeaderSelector(CuratorFramework client,
      String leaderPath, LeaderSelectorListener listener) {
    return new LeaderSelector(client, leaderPath, listener);
  }
}
