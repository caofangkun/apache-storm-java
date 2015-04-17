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
package org.apache.storm.config;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.EvenSampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.ConfigValidation;
import backtype.storm.ConfigValidation.FieldValidator;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.Utils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class ConfigUtil {
  private final static Logger LOG = LoggerFactory.getLogger(ConfigUtil.class);
  public final static String RESOURCES_SUBDIR = "resources";
  public static final String FILE_SEPERATEOR = CoreUtil.filePathSeparator();

  @ClojureClass(className = "backtype.storm.config#clojure-config-name")
  public static String clojureConfigName(String name) {
    return name.toUpperCase().replace("_", "-");
  }

  @ClojureClass(className = "backtype.storm.config#ALL-CONFIGS")
  public static List<Object> All_CONFIGS() {
    List<Object> rtn = new ArrayList<Object>();
    Config config = new Config();
    Class<?> ConfigClass = config.getClass();
    Field[] fields = ConfigClass.getFields();
    for (int i = 0; i < fields.length; i++) {
      try {
        Object obj = fields[i].get(null);
        rtn.add(obj);
      } catch (IllegalArgumentException e) {
        LOG.error(e.getMessage(), e);
      } catch (IllegalAccessException e) {
        LOG.error(e.getMessage(), e);
      }
    }
    return rtn;
  }

  public static HashMap<String, Object> getClassFields(Class<?> cls)
      throws IllegalArgumentException, IllegalAccessException {
    java.lang.reflect.Field[] list = cls.getDeclaredFields();
    HashMap<String, Object> rtn = new HashMap<String, Object>();
    for (java.lang.reflect.Field f : list) {
      String name = f.getName();
      rtn.put(name, f.get(null).toString());
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.config#get-FieldValidator")
  private static FieldValidator getFieldValidator(final Object klass) {
    FieldValidator fieldValidator = new ConfigValidation.FieldValidator() {

      @Override
      public void validateField(String name, Object v)
          throws IllegalArgumentException {
        if (!(v == null) && klass.getClass().isInstance(v)) {
          throw new IllegalArgumentException("field " + name + " '" + v
              + "' must be a  '" + klass.getClass().getName() + "'");
        }

      }
    };
    return fieldValidator;
  }

  /**
   * Create a mapping of config-string -> validator Config fields must have a
   * SCHEMA field defined
   * 
   * @return
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  @ClojureClass(className = "backtype.storm.config#CONFIG-SCHEMA-MAP")
  public static Map<Object, FieldValidator> configSchemaMap()
      throws SecurityException, NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException {
    Config config = new Config();
    Class<?> configClass = config.getClass();
    Field[] fields = configClass.getFields();
    Map<Object, FieldValidator> result = new HashMap<Object, FieldValidator>();
    for (Field field : fields) {

      String configString = field.getName();
      Pattern p = Pattern.compile(".*_SCHEMA$");
      Matcher m = p.matcher(configString);
      if (!m.matches()) {
        Object valid = configClass.getField(configString + "_SCHEMA").get(null);
        if (null != valid) {
          Object key = field.get(null);
          FieldValidator fieldValidator = getFieldValidator(valid);
          result.put(key, fieldValidator);
        }
      }
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.config#cluster-mode")
  public static String clusterMode(Map<Object, Object> conf) {
    return CoreUtil.parseString(conf.get(Config.STORM_CLUSTER_MODE),
        "distributed");
  }

  @ClojureClass(className = "backtype.storm.config#local-mode?")
  public static boolean isLocalMode(Map<Object, Object> conf) {
    String mode =
        CoreUtil
            .parseString(conf.get(Config.STORM_CLUSTER_MODE), "distributed");
    if (mode.equals("local")) {
      return true;
    } else if (mode.equals("distributed")) {
      return false;
    } else {
      throw new IllegalArgumentException("Illegal cluster mode in conf: "
          + mode);
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.config#sampling-rate")
  public static Integer samplingRate(Map conf) {
    return (int) (1 / Double.parseDouble(String.valueOf(conf
        .get(Config.TOPOLOGY_STATS_SAMPLE_RATE))));
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.config#mk-stats-sampler")
  public static EvenSampler mkStatsSampler(Map conf) {
    return new EvenSampler(samplingRate(conf));
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.config#read-default-config")
  public static Map readDefaultConfig() {
    return Utils.readDefaultConfig();
  }

  @ClojureClass(className = "backtype.storm.config#validate-configs-with-schemas")
  public static void validateConfigsWithSchemas(Map<String, Object> conf)
      throws SecurityException, IllegalArgumentException, NoSuchFieldException,
      IllegalAccessException {
    Map<Object, FieldValidator> configSchemaMap = configSchemaMap();
    for (Map.Entry<String, Object> entry : conf.entrySet()) {
      String k = entry.getKey();
      Object v = entry.getValue();
      FieldValidator schema = configSchemaMap.get(k);
      if (schema != null) {
        schema.validateField(k, v);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.config#read-storm-config")
  public static Map readStormConfig() {
    return Utils.readStormConfig();
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.config#read-yaml-config")
  public static Map readYamlConfig(String name, boolean mustExist) {
    return Utils.findAndReadConfigFile(name, mustExist);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.config#read-yaml-config")
  public static Map readYamlConfig(String name) {
    return Utils.findAndReadConfigFile(name, true);
  }

  @ClojureClass(className = "backtype.storm.config#master-local-dir")
  public static String masterLocalDir(Map<String, Object> conf)
      throws IOException {
    String ret =
        String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPERATEOR
            + "nimbus";
    if (!CoreUtil.existsFile(ret)) {
      FileUtils.forceMkdir(new File(ret));
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.config#master-stormdist-root")
  public static String masterStormdistRoot(Map<String, Object> conf)
      throws IOException {
    String ret = stormdist_path(masterLocalDir(conf));
    if (!CoreUtil.existsFile(ret)) {
      FileUtils.forceMkdir(new File(ret));
    }
    return ret;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.config#master-stormdist-root")
  public static String masterStormdistRoot(Map conf, String stormId)
      throws IOException {
    return masterStormdistRoot(conf) + FILE_SEPERATEOR + stormId;
  }

  @ClojureClass(className = "backtype.storm.config#master-stormjar-path")
  public static String masterStormjarPath(String stormroot) {
    return stormroot + FILE_SEPERATEOR + "stormjar.jar";
  }

  @ClojureClass(className = "backtype.storm.config#master-stormcode-path")
  public static String masterStormcodePath(String stormroot) {
    return stormroot + FILE_SEPERATEOR + "stormcode.ser";
  }

  @ClojureClass(className = "backtype.storm.config#master-stormconf-path")
  public static String masterStormconfPath(String stormroot) {
    return stormroot + FILE_SEPERATEOR + "stormconf.ser";
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.config#master-inbox")
  public static String masterInbox(Map conf) throws IOException {
    String ret = masterLocalDir(conf) + FILE_SEPERATEOR + "inbox";
    if (!CoreUtil.existsFile(ret)) {
      FileUtils.forceMkdir(new File(ret));
    }
    return ret;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.config#master-inimbus-dir")
  public static String masterInimbusDir(Map conf) throws IOException {
    String ret = masterLocalDir(conf) + FILE_SEPERATEOR + "inimbus";
    if (!CoreUtil.existsFile(ret)) {
      FileUtils.forceMkdir(new File(ret));
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.config#supervisor-local-dir")
  private static String supervisorLocalDir(Map<Object, Object> conf)
      throws IOException {
    String ret =
        String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPERATEOR
            + "supervisor";
    if (!CoreUtil.existsFile(ret)) {
      FileUtils.forceMkdir(new File(ret));
    }
    return ret;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.config#supervisor-isupervisor-dir")
  public static String supervisorIsupervisorDir(Map conf) throws IOException {
    return supervisorLocalDir(conf) + FILE_SEPERATEOR + "isupervisor";
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.config#supervisor-stormdist-root")
  public static String supervisorStormdistRoot(Map conf) throws IOException {
    String ret = stormdist_path(supervisorLocalDir(conf));
    FileUtils.forceMkdir(new File(ret));
    return ret;
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.config#supervisor-stormdist-root")
  public static String supervisorStormdistRoot(Map conf, String stormId)
      throws IOException {
    return supervisorStormdistRoot(conf) + FILE_SEPERATEOR + stormId;
  }

  @ClojureClass(className = "backtype.storm.config#supervisor-stormjar-path")
  public static String supervisorStormjarPath(String stormroot) {
    return stormroot + FILE_SEPERATEOR + "stormjar.jar";
  }

  @ClojureClass(className = "backtype.storm.config#supervisor-stormcode-path")
  public static String supervisorStormcodePath(String stormroot) {
    return stormroot + FILE_SEPERATEOR + "stormcode.ser";
  }

  @ClojureClass(className = "backtype.storm.config#supervisor-stormconf-path")
  public static String supervisorStormconfPath(String stormroot) {
    return stormroot + FILE_SEPERATEOR + "stormconf.ser";
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.config#supervisor-stormconf-path")
  public static String supervisorTmpDir(Map conf) throws IOException {
    String ret = supervisorLocalDir(conf) + FILE_SEPERATEOR + "tmp";
    if (!CoreUtil.existsFile(ret)) {
      FileUtils.forceMkdir(new File(ret));
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.config#supervisor-storm-resources-path")
  public static String supervisorStormResourcesPath(String stormroot) {
    return stormroot + FILE_SEPERATEOR + RESOURCES_SUBDIR;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.config#supervisor-state")
  public static LocalState supervisorState(Map conf) throws IOException {
    LocalState localState = null;
    try {
      String localstateDir =
          supervisorLocalDir(conf) + FILE_SEPERATEOR + "localstate";
      if (!CoreUtil.existsFile(localstateDir)) {
        FileUtils.forceMkdir(new File(localstateDir));
      }
      localState = new LocalState(localstateDir);
    } catch (IOException e) {
      LOG.error("Failed to create supervisor LocalState", e);
      throw e;
    }

    return localState;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.config#read-supervisor-storm-conf")
  public static Map readSupervisorStormConf(Map conf, String stormId)
      throws IOException {
    String stormRoot = supervisorStormdistRoot(conf, stormId);
    String confPath = supervisorStormconfPath(stormRoot);
    Map supervisorConf =
        Utils.javaDeserialize(
            FileUtils.readFileToByteArray(new File(confPath)), Map.class);
    if (supervisorConf != null) {
      conf.putAll(supervisorConf);
    }
    return conf;
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.config#read-supervisor-topology")
  public static StormTopology readSupervisorTopology(Map conf, String stormId)
      throws IOException {
    String stormRoot = supervisorStormdistRoot(conf, stormId);
    String topologyPath = supervisorStormcodePath(stormRoot);
    return Utils.deserialize(
        FileUtils.readFileToByteArray(new File(topologyPath)),
        StormTopology.class);
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.config#worker-user-root")
  public static String workerUserRoot(Map conf) throws IOException {
    String ret =
        CoreUtil.parseString(conf.get(Config.STORM_LOCAL_DIR), "storm-local")
            + FILE_SEPERATEOR + "workers-users";
    if (!CoreUtil.existsFile(ret)) {
      FileUtils.forceMkdir(new File(ret));
    }
    return ret;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.config#worker-user-file")
  public static String workerUserFile(Map conf, String workerId)
      throws IOException {
    return workerUserRoot(conf) + FILE_SEPERATEOR + workerId;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.config#get-worker-user")
  public static String getWorkerUser(Map conf, String workerId) {
    LOG.info("GET worker-user " + workerId);
    String workerUser = null;
    try {
      String workerUserFile = workerUserFile(conf, workerId);
      workerUser = FileUtils.readFileToString(new File(workerUserFile));
    } catch (IOException e) {
      LOG.warn("Failed to get worker user for " + workerId + ".");
    }
    return workerUser;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.config#set-worker-user!")
  public static void setWorkerUser(Map conf, String workerId, String user)
      throws IOException {
    LOG.info("SET worker-user " + workerId + " " + user);
    File file = new File(workerUserFile(conf, workerId));
    file.getParentFile().mkdirs();
    FileUtils.writeStringToFile(file, user);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.config#remove-worker-user!")
  public static void removeWorkerUser(Map conf, String workerId)
      throws IOException {
    LOG.info("REMOVE worker-user " + workerId);
    File file = new File(workerUserFile(conf, workerId));
    file.delete();
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.config#worker-root")
  public static String workerRoot(Map conf) throws IOException {
    String stormLocalDir =
        CoreUtil.parseString(conf.get(Config.STORM_LOCAL_DIR), "storm-local");
    return stormLocalDir + FILE_SEPERATEOR + "workers";
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.config#worker-root")
  public static String workerRoot(Map conf, String id) throws IOException {
    return workerRoot(conf) + FILE_SEPERATEOR + id;
  }

  @ClojureClass(className = "backtype.storm.config#worker-pids-root")
  public static String workerPidsRoot(Map<Object, Object> conf, String id)
      throws IOException {
    String ret = workerRoot(conf, id) + FILE_SEPERATEOR + "pids";
    if (!CoreUtil.existsFile(ret)) {
      FileUtils.forceMkdir(new File(ret));
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.config#worker-pids-path")
  public static String workerPidPath(Map<Object, Object> conf, String id,
      String pid) throws IOException {
    String ret = workerPidsRoot(conf, id) + FILE_SEPERATEOR + pid;
    return ret;
  }

  @ClojureClass(className = "backtype.storm.config#worker-heartbeats-root")
  public static String workerHeartbeatsRoot(Map<Object, Object> conf, String id)
      throws IOException {
    String ret = workerRoot(conf, id) + FILE_SEPERATEOR + "heartbeats";
    if (!CoreUtil.existsFile(ret)) {
      FileUtils.forceMkdir(new File(ret));
    }
    return ret;
  }

  /**
   * workers heartbeat here with pid and timestamp if supervisor stops receiving
   * heartbeat, it kills and restarts the process in local mode, keep a global
   * map of ids to threads for simulating process management
   * 
   * @param conf
   * @param id
   * @return
   * @throws IOException
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.config#worker-state")
  public static LocalState workerState(Map conf, String id) throws IOException {
    String path = workerHeartbeatsRoot(conf, id);
    LocalState rtn = new LocalState(path);
    return rtn;
  }

  @SuppressWarnings({ "rawtypes" })
  public static boolean isDistributedMode(Map conf) {
    String mode =
        CoreUtil
            .parseString(conf.get(Config.STORM_CLUSTER_MODE), "distributed");
    if (mode != null) {
      if (mode.equals("local")) {
        return false;
      }
      if (mode.equals("distributed")) {
        return true;
      }
    }
    throw new IllegalArgumentException("Illegal cluster mode in conf:" + mode);
  }

  /**
   * validate whether the mode is distributed
   * 
   * @param conf
   */
  @ClojureClass(className = "backtype.storm.daemon.common#validate-distributed-mode!")
  public static void validateDistributedMode(Map<Object, Object> conf) {
    if (isLocalMode(conf)) {
      throw new IllegalArgumentException("Cannot start server in local mode!");
    }
  }

  public static String stormdist_path(String stormroot) {
    return stormroot + FILE_SEPERATEOR + "stormdist";
  }
}
