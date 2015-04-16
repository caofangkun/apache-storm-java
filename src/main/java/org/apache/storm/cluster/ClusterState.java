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

import java.util.List;
import java.util.Set;

import org.apache.storm.ClojureClass;
import org.apache.storm.zookeeper.data.ACL;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.cluster#ClusterState")
public interface ClusterState {
  @ClojureClass(className = "backtype.storm.cluster#ClusterState#set-ephemeral-node")
  public void setEphemeralNode(String path, byte[] data, List<ACL> acls)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#delete-node")
  public void deleteNode(String path) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#create-sequential")
  public void createSequential(String path, byte[] data, List<ACL> acls)
      throws Exception;

  // if node does not exist, create persistent with this data
  @ClojureClass(className = "backtype.storm.cluster#ClusterState#set-data")
  public void setData(String path, byte[] data, List<ACL> acls)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#get-data")
  public byte[] getData(String path, boolean watch) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#get-version")
  public int getVersion(String path, boolean watch) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#get-data-with-version")
  public DataInfo getDataWithVersion(String path, boolean watch)
      throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#get-children")
  public Set<String> getChildren(String path, boolean watch) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#mk-dirs")
  public void mkdirs(String path, List<ACL> acls) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#exists-node?")
  public boolean existsNode(String path, boolean watch) throws Exception;

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#close")
  public void close();

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#register")
  public String register(ClusterStateCallback callback);

  @ClojureClass(className = "backtype.storm.cluster#ClusterState#unregister")
  public ClusterStateCallback unregister(String id);

  public LeaderSelector mkLeaderSelector(String path,
      LeaderSelectorListener listener) throws Exception;

  public boolean isClosed();
}