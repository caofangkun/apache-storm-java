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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.zookeeper.ZooDefs;
import org.apache.storm.zookeeper.data.ACL;
import org.apache.storm.zookeeper.data.Id;
import org.apache.storm.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.cluster")
public class Cluster {
  private static Logger LOG = LoggerFactory.getLogger(Cluster.class);
  public static final String ZK_SEPERATOR = "/";

  public static final String MASTER_ROOT = "nimbus";
  public static final String UI_ROOT = "ui";

  public static final String MASTER_LOCK_ROOT = "masterlock";

  public static final String ASSIGNMENTS_ROOT = "assignments";
  public static final String CODE_ROOT = "code";
  public static final String STORMS_ROOT = "storms";
  public static final String SUPERVISORS_ROOT = "supervisors";
  public static final String WORKERBEATS_ROOT = "workerbeats";
  public static final String ERRORS_ROOT = "errors";
  public static final String CREDENTIALS_ROOT = "credentials";

  public static final String USER_ROOT = "users";

  public static final String ASSIGNMENTS_SUBTREE;
  public static final String STORMS_SUBTREE;
  public static final String SUPERVISORS_SUBTREE;
  public static final String WORKERBEATS_SUBTREE;
  public static final String ERRORS_SUBTREE;
  public static final String CREDENTIALS_SUBTRE;

  public static final String USER_SUBTREE;

  public static final String MASTER_SUBTREE;
  public static final String UI_SUBTREE;
  public static final String MASTER_LOCK_SUBTREE;

  static {
    ASSIGNMENTS_SUBTREE = ZK_SEPERATOR + ASSIGNMENTS_ROOT;
    STORMS_SUBTREE = ZK_SEPERATOR + STORMS_ROOT;
    SUPERVISORS_SUBTREE = ZK_SEPERATOR + SUPERVISORS_ROOT;
    ERRORS_SUBTREE = ZK_SEPERATOR + ERRORS_ROOT;
    CREDENTIALS_SUBTRE = ZK_SEPERATOR + CREDENTIALS_ROOT;
    MASTER_SUBTREE = ZK_SEPERATOR + MASTER_ROOT;
    UI_SUBTREE = ZK_SEPERATOR + UI_ROOT;

    MASTER_LOCK_SUBTREE = ZK_SEPERATOR + MASTER_LOCK_ROOT;
    USER_SUBTREE = ZK_SEPERATOR + USER_ROOT;
    WORKERBEATS_SUBTREE = ZK_SEPERATOR + WORKERBEATS_ROOT;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.cluster#mk-topo-only-acls")
  public static List<ACL> mkTopoOnlyAcls(Map topoConf) {
    List<ACL> topoAcls = null;
    try {
      String payload =
          CoreUtil.parseString(
              topoConf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD), null);
      if (Utils.isZkAuthenticationConfiguredTopology(topoConf)) {
        topoAcls = new ArrayList<ACL>();
        topoAcls.add(ZooDefs.Ids.CREATOR_ALL_ACL.get(0));
        topoAcls.add(new ACL(ZooDefs.Perms.READ, new Id("digest",
            DigestAuthenticationProvider.generateDigest(payload))));
      }
    } catch (NoSuchAlgorithmException e) {
      LOG.error(CoreUtil.stringifyError(e));
    }
    return topoAcls;
  }

  /**
   * Supervisors Path
   * 
   * @param id supervisorId
   * @return /supervisors/{supervisorId}
   */
  @ClojureClass(className = "backtype.storm.cluster#supervisor-path")
  public static String supervisorPath(String id) {
    return SUPERVISORS_SUBTREE + ZK_SEPERATOR + id;
  }

  /**
   * Assignments Path
   * 
   * @param id stormId
   * @return /assignments/{id}
   */
  @ClojureClass(className = "backtype.storm.cluster#assignment-path")
  public static String assignmentPath(String id) {
    return ASSIGNMENTS_SUBTREE + ZK_SEPERATOR + id;
  }

  /**
   * Storms Path
   * 
   * @param id stormId
   * @return /storms/{id}
   */
  @ClojureClass(className = "backtype.storm.cluster#storm-path")
  public static String stormPath(String id) {
    return STORMS_SUBTREE + ZK_SEPERATOR + id;
  }

  /**
   * Workerbeats Path
   * 
   * @param stormId
   * @return /workerbeats/{stormId}
   */
  @ClojureClass(className = "backtype.storm.cluster#workerbeat-storm-root")
  public static String workerbeatStormRoot(String stormId) {
    return WORKERBEATS_SUBTREE + ZK_SEPERATOR + stormId;
  }

  /**
   * Workerbeat Path
   * 
   * @param stormId
   * @param node worker's host name
   * @param port worker's port
   * @return /workerbeats/{stormId}/{node}-{port}
   * 
   */
  @ClojureClass(className = "backtype.storm.cluster#workerbeat-path")
  public static String workerbeatPath(String stormId, String node, Integer port) {
    return workerbeatStormRoot(stormId) + ZK_SEPERATOR + node + "-" + port;
  }

  /**
   * Error Storm Root Path
   * 
   * @param stormId storm id
   * @return /errors/{stormId}
   */
  @ClojureClass(className = "backtype.storm.cluster#error-storm-root")
  public static String errorStormRoot(String stormId) {
    return ERRORS_SUBTREE + ZK_SEPERATOR + stormId;
  }

  /**
   * Error Path
   * 
   * @param stormId storm id
   * @param componentId component id
   * @return /errors/{stormId}/{componentId}
   * @throws UnsupportedEncodingException
   */
  @ClojureClass(className = "backtype.storm.cluster#error-path")
  public static String errorPath(String stormId, String componentId)
      throws UnsupportedEncodingException {
    return errorStormRoot(stormId) + ZK_SEPERATOR
        + URLEncoder.encode(componentId, "UTF-8");
  }
  
  @ClojureClass(className = "backtype.storm.cluster#ast-error-path-seg")
  private final static String lastErrorPathSeg = "last-error";
  
  @ClojureClass(className = "backtype.storm.cluster#last-error-path")
  public static String lastErrorPath(String stormId, String componentId)
      throws UnsupportedEncodingException {
    return errorStormRoot(stormId) + ZK_SEPERATOR
        + URLEncoder.encode(componentId, "UTF-8") + "-" + lastErrorPathSeg;
  }

  @ClojureClass(className = "backtype.storm.cluster#credentials-path")
  public static String credentialsPath(String stormId) {
    return CREDENTIALS_SUBTRE + ZK_SEPERATOR + stormId;
  }

  public static String user_bean_path(String uid) {
    return USER_SUBTREE + ZK_SEPERATOR + uid;
  }

  public static String user_bean_root() {
    return USER_SUBTREE;
  }

  /**
   * Deserialize byte array to Object
   * 
   * @param data
   * @return Object
   */
  @ClojureClass(className = "backtype.storm.cluster#maybe-deserialize")
  public static <T> T maybeDeserialize(byte[] data, Class<T> clazz) {
    if (data == null) {
      return null;
    }
    return Utils.deserialize(data, clazz);
  }
}
