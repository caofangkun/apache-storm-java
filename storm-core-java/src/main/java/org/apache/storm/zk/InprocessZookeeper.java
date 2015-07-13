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

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.NetWorkUtils;
import org.apache.storm.zookeeper.server.NIOServerCnxnFactory;
import org.apache.storm.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

@ClojureClass(className = "backtype.storm.zookeeper#mk-inprocess-zookeeper")
public class InprocessZookeeper {

  private static final Logger LOG = LoggerFactory
      .getLogger(InprocessZookeeper.class);
  private NIOServerCnxnFactory zkFactory;
  private int zkport;
  private String stormLocalDir;
  @SuppressWarnings("rawtypes")
  private Map conf;

  @SuppressWarnings("rawtypes")
  public InprocessZookeeper(Map conf) {
    this.conf = conf;
    this.zkport =
        CoreUtil.parseInt(conf.get(Config.STORM_ZOOKEEPER_PORT), 2182);
    this.stormLocalDir =
        CoreUtil.parseString(conf.get(Config.STORM_LOCAL_DIR), "storm-local");
  }

  @SuppressWarnings("unchecked")
  public void start() throws IOException, InterruptedException {
    LOG.info("Starting up embedded Zookeeper server");
    File localfile = new File(stormLocalDir + "/zookeeper.data");
    ZooKeeperServer zkServer = new ZooKeeperServer(localfile, localfile, 2000);
    try {
      zkFactory = new NIOServerCnxnFactory();
      zkport = NetWorkUtils.assignAvailableServerPort(zkport);
      zkFactory.configure(new InetSocketAddress(zkport), -1);
      conf.put(Config.STORM_ZOOKEEPER_PORT, zkport);
    } catch (BindException e) {
      throw new IOException("Fail to find a port for Zookeeper server to bind",
          e);
    }
    LOG.info("Starting inprocess zookeeper at port {} and dir {} ", zkport,
        stormLocalDir);
    zkFactory.startup(zkServer);
  }

  public int port() {
    return zkport;
  }

  public void stop() {
    LOG.info("shutdown embedded zookeeper server with port " + zkport);
    zkFactory.shutdown();
    try {
      FileUtils.deleteDirectory(new File(stormLocalDir));
    } catch (IOException e) {
      LOG.warn("Failed to delete directory: {} for {}", stormLocalDir,
          CoreUtil.stringifyError(e));
    }
    zkFactory = null;
  }
}