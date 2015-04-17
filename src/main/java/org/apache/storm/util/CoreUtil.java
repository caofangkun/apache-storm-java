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
package org.apache.storm.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.apache.storm.ClojureClass;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.util.thread.BaseCallback;
import org.apache.storm.util.thread.KillFn;
import org.apache.storm.util.thread.RunnableCallback;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import src.jvm.backtype.storm.utils.MutableInt;
import src.jvm.backtype.storm.utils.Time;
import src.jvm.backtype.storm.utils.Utils;
import clojure.lang.Sequential;

public class CoreUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CoreUtil.class);

  /**
   * Wraps an exception in a RuntimeException if needed
   * 
   * @param e Exception e
   * @return RuntimeException
   */
  @ClojureClass(className = "backtype.storm.util#wrap-in-runtime")
  public static RuntimeException wrapInRuntime(Exception e) {
    return new RuntimeException(e);
  }

  /**
   * get file path separator
   * 
   * @return file path separator string
   */
  @ClojureClass(className = "backtype.storm.util#file-path-separator")
  public static String filePathSeparator() {
    return System.getProperty("file.separator");
  }

  /**
   * get class path separator
   * 
   * @return class path separator string eg : "/" or "\"
   */
  @ClojureClass(className = "backtype.storm.util#class-path-separator")
  public static String classPathSeparator() {
    return System.getProperty("path.separator");
  }

  /**
   * Add classpath to paths
   * 
   * @param classpath
   * @param paths
   * @return new classpath string
   */
  @ClojureClass(className = "backtype.storm.util#add-to-classpath")
  public static String addToClasspath(String classpath, String[] paths) {
    if (paths == null || paths.length == 0) {
      return classpath;
    }
    String sep = classPathSeparator();
    for (String path : paths) {
      classpath += sep + path;
    }
    return classpath;
  }

  /**
   * check if file exists
   * 
   * @param path
   * @return true if file exits
   */
  @ClojureClass(className = "backtype.storm.util#exists-file?")
  public static boolean existsFile(String path) {
    return (new File(path)).exists();
  }

  /**
   * rmr path
   * 
   * @param path
   * @throws IOException
   */
  @ClojureClass(className = "backtype.storm.util#rmr")
  public static void rmr(String path) throws IOException {
    LOG.debug("Rmr path " + path);
    if (existsFile(path)) {
      try {
        FileUtils.forceDelete(new File(path));
      } catch (FileNotFoundException e) {
        LOG.warn(path + " does not exits!");
      }
    }
  }

  /**
   * Removes file or directory at the path. Not recursive. Throws exception on
   * failure
   * 
   * @param path the string path to delete
   */
  @ClojureClass(className = "backtype.storm.util#rmpath")
  public static void rmpath(String path) {
    LOG.debug("Removing path " + path);
    if (existsFile(path)) {
      boolean isdelete = (new File(path)).delete();
      if (!isdelete) {
        throw new RuntimeException("Failed to delete " + path);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.util#local-mkdirs")
  public static void localMkdirs(String path) throws IOException {
    LOG.debug("Making dirs at {}", path);
    FileUtils.forceMkdir(new File(path));
  }

  /**
   * Check if process exits
   * 
   * @param processId
   * @return true if process exists, else return false
   */
  public static Boolean isProcessExists(String processId) {
    String path = File.separator + "proc" + File.separator + processId;
    return existsFile(path);
  }

  /**
   * Touching file at path
   * 
   * @param path
   * @throws IOException
   */
  @ClojureClass(className = "backtype.storm.util#touch")
  public static void touch(String path) throws IOException {
    LOG.debug("Touching file at" + path);
    boolean success = (new File(path)).createNewFile();
    if (!success) {
      throw new RuntimeException("Failed to touch " + path);
    }
  }

  /**
   * Read Dir Contexts
   * 
   * @param dir
   * @return
   */
  @ClojureClass(className = "backtype.storm.util#read-dir-contents")
  public static Set<String> readDirContents(String dir) {
    Set<String> rtn = new HashSet<String>();
    if (existsFile(dir)) {
      File[] list = (new File(dir)).listFiles();
      for (File f : list) {
        rtn.add(f.getName());
      }
    }
    return rtn;
  }

  /**
   * get current classpath
   * 
   * @return current classpath string
   */
  @ClojureClass(className = "backtype.storm.util#current-classpath")
  public static String currentClasspath() {
    return System.getProperty("java.class.path");
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.util#to-json")
  public static String to_json(Map m) {
    if (m == null) {
      return null;
    } else {
      return JSONValue.toJSONString(m);
    }
  }

  @ClojureClass(className = "backtype.storm.util#from-json")
  public static Object from_json(String json) {
    if (json == null) {
      return null;
    } else {
      return JSONValue.parse(json);
    }
  }

  @ClojureClass(className = "backtype.storm.util#multi-set")
  public static <V> HashMap<V, Integer> multiSet(List<V> list) {
    // Returns a map of elem to count
    HashMap<V, Integer> rtn = new HashMap<V, Integer>();
    for (V v : list) {
      int cnt = 1;
      if (rtn.containsKey(v)) {
        cnt += rtn.get(v);
      }
      rtn.put(v, cnt);
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#exit-process!")
  public static void exitProcess(int val, String msg) {
    LOG.info("Exit process: " + msg);
    Runtime.getRuntime().exit(val);
  }

  @ClojureClass(className = "backtype.storm.util#zip-contains-dir?")
  public static boolean zipContainsDir(String zipfile, String target) {

    Enumeration<? extends ZipEntry> entries = null;
    try {
      ZipFile zipFile = new ZipFile(zipfile);
      entries = zipFile.entries();
      while (entries != null && entries.hasMoreElements()) {
        ZipEntry ze = entries.nextElement();
        String name = ze.getName();
        if (name.startsWith(target + "/")) {
          return true;
        }
      }
    } catch (IOException e) {
      LOG.error(e + "zipContainsDir error");
    }

    return false;
  }

  @ClojureClass(className = "backtype.storm.util#url-encode")
  public static String urlEncode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.warn("UnsupportedEncodingException: {}", stringifyError(e));
    }
    return s;
  }

  @ClojureClass(className = "backtype.storm.util#url-decode")
  public static String urlDecode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.warn("UnsupportedEncodingException: {}", stringifyError(e));
    }
    return s;
  }

  public static RunnableCallback getDefaultKillfn() {

    return new KillFn();
  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.util#filter-val")
  public static <K, V> HashMap<K, V> filterVal(RunnableCallback fn,
      Map<K, V> amap) {
    HashMap<K, V> rtn = new HashMap<K, V>();

    for (Entry<K, V> entry : amap.entrySet()) {
      V value = entry.getValue();
      Object result = fn.execute(value);

      if ((Boolean) result == true) {
        rtn.put(entry.getKey(), value);
      }
    }
    return rtn;
  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.util#filter-key")
  public static <K, V> Map<K, V> filterKey(BaseCallback afn, Map<K, V> amap) {
    Map<K, V> result = new HashMap<K, V>();
    for (Map.Entry<K, V> entry : amap.entrySet()) {
      K k = entry.getKey();
      V v = entry.getValue();
      if ((Boolean) afn.execute(k) == true) {
        result.put(k, v);
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.util#map-key")
  public static <K, V> Map<Object, V> mapKey(BaseCallback afn, Map<K, V> amap) {
    Map<Object, V> result = new HashMap<Object, V>();
    for (Map.Entry<K, V> entry : amap.entrySet()) {
      K k = entry.getKey();
      V v = entry.getValue();
      Object obj = afn.execute(k);
      result.put(obj, v);
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.util#process-pid")
  public static String processPid() {
    // Gets the pid of this JVM. Hacky because Java doesn't provide a real
    // way to do this.
    String name = ManagementFactory.getRuntimeMXBean().getName();
    String[] split = name.split("@");
    if (split.length != 2) {
      throw new RuntimeException("Got unexpected process name: " + name);
    }

    return split[0];
  }

  @ClojureClass(className = "backtype.storm.util#exec-command!")
  public static void execCommand(String command) throws ExecuteException,
      IOException {
    String[] cmdlist = command.split(" ");
    CommandLine cmd = new CommandLine(cmdlist[0]);
    for (int i = 1; i < cmdlist.length; i++) {
      cmd.addArgument(cmdlist[i]);
    }

    DefaultExecutor exec = new DefaultExecutor();
    exec.execute(cmd);
  }

  @ClojureClass(className = "backtype.storm.util#extract-dir-from-jar")
  public static void extractDirFromJar(String jarpath, String dir,
      String destdir) {
    // Extra dir from the jar to destdir
    String cmd = "unzip -qq " + jarpath + " " + dir + "/** -d " + destdir;
    try {
      execCommand(cmd);
    } catch (Exception e) {
      LOG.warn("Error when trying to extract " + dir + " from " + jarpath
          + " IOException" + "by cmd:" + cmd + "!" + e.getMessage());
    }
  }

  public static void sleepMs(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {

    }
  }

  public static void ensureProcessKilled(Integer pid) {
    // in this function, just kill the process 5 times
    // make sure the process be killed definitely
    for (int i = 0; i < 5; i++) {
      try {
        execCommand("kill -9 " + pid);
        LOG.info("kill -9 process " + pid);
        sleepMs(100);
      } catch (ExecuteException e) {
        LOG.info("Error when trying to kill " + pid
            + ". Process is probably already dead. ");
      } catch (Exception e) {
        LOG.info("Error when trying to kill " + pid + ".Exception ", e);
      }
    }
  }

  public static void processKilled(Integer pid) {
    try {
      execCommand("kill " + pid);
      LOG.info("kill process " + pid);
    } catch (ExecuteException e) {
      LOG.info("Error when trying to kill " + pid
          + ". Process is probably already dead. ");
    } catch (Exception e) {
      LOG.info("Error when trying to kill " + pid + ".Exception ", e);
    }
  }

  private static final int SIG_KILL = 9;
  private static final int SIG_TERM = 15;

  @ClojureClass(className = "backtype.storm.util#send-signal-to-process")
  public static void sendSignalToProcess(Integer pid, int signum) {
    try {
      execCommand("kill -" + signum + " " + pid);
    } catch (Throwable e) {
      if (Utils.exceptionCauseIsInstanceOf(ExecuteException.class, e)) {
        LOG.info(
            "Error when trying to kill {}. Process is probably already dead.",
            pid);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.util#force-kill-process")
  public static void forceKillProcess(Integer pid) {
    sendSignalToProcess(pid, SIG_KILL);
  }

  @ClojureClass(className = "backtype.storm.util#kill-process-with-sig-term")
  public static void killProcessWithSigTerm(Integer pid) {
    sendSignalToProcess(pid, SIG_TERM);
  }

  @ClojureClass(className = "backtype.storm.util#launch-process")
  public static java.lang.Process launchProcess(String command,
      Map<String, String> environment) throws IOException {
    String[] cmdlist = (new String("nohup " + command)).split(" ");
    ArrayList<String> buff = new ArrayList<String>();
    for (String tok : cmdlist) {
      if (!tok.isEmpty()) {
        buff.add(tok);
      }
    }

    ProcessBuilder builder = new ProcessBuilder(buff);
    Map<String, String> processEvn = builder.environment();
    for (Entry<String, String> entry : environment.entrySet()) {
      processEvn.put(entry.getKey(), entry.getValue());
    }
    builder.redirectErrorStream(true);
    return builder.start();
  }

  // @@@ supervisor don't use this function
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void downloadCodeFromMaster(Map conf, String localRoot,
      String masterCodeDir)
      throws backtype.storm.generated.AuthorizationException, IOException,
      TException {
    FileUtils.forceMkdir(new File(localRoot));

    String localStormjarPath = ConfigUtil.masterStormjarPath(localRoot);
    String masterStormjarPath = ConfigUtil.masterStormjarPath(masterCodeDir);
    Utils.downloadFromMaster(conf, masterStormjarPath, localStormjarPath);

    String localStormcodePath = ConfigUtil.masterStormcodePath(localRoot);
    String masterStormcodePath = ConfigUtil.masterStormcodePath(masterCodeDir);
    Utils.downloadFromMaster(conf, masterStormcodePath, localStormcodePath);

    String localStormConfPath = ConfigUtil.masterStormconfPath(localRoot);
    String masterStormConfPath = ConfigUtil.masterStormconfPath(masterCodeDir);
    Utils.downloadFromMaster(conf, masterStormConfPath, localStormConfPath);
  }

  @ClojureClass(className = "backtype.storm.util#acquire-random-range-id")
  public static <T> T acquireRandomRangeId(MutableInt curr, List<T> choices,
      Random rand) {
    if (curr.increment() >= choices.size()) {
      curr.set(0);
      Collections.shuffle(choices, rand);
    }
    return choices.get(curr.get());
  }

  @ClojureClass(className = "backtype.storm.util#partition-fixed")
  public static List<List<Integer>> partitionFixed(int maxNumChunks,
      List<Integer> aseq) {
    if (maxNumChunks == 0) {
      return null;
    }
    int total = aseq.size();
    int baseSize = total / maxNumChunks;
    int remainder = total % maxNumChunks;
    List<List<Integer>> result = new ArrayList<List<Integer>>();
    int totalIndex = 0;
    for (int i = 0; i < maxNumChunks; i++) {
      int count = baseSize;
      if (remainder > 0) {
        count += 1;
        remainder -= 1;
      }
      List<Integer> chunk = new ArrayList<Integer>();
      while (count > 0) {
        chunk.add(aseq.get(totalIndex));
        totalIndex += 1;
        count -= 1;
      }
      result.add(chunk);
    }

    return result;
  }

  @ClojureClass(className = "backtype.storm.util#join-maps")
  public static <K, V> HashMap<K, V> joinMaps(Map<K, V>... maps) {
    Set<K> allKeys = new HashSet<K>();
    for (Map<K, V> m : maps) {
      allKeys.addAll(m.keySet());
    }
    Map<K, V> ret = new HashMap<K, V>();
    for (K k : allKeys) {
      for (Map<K, V> m : maps) {
        ret.put(k, m.get(k));
      }
    }
    return (HashMap<K, V>) ret;
  }

  @ClojureClass(className = "backtype.storm.util#integer-divided")
  public static TreeMap<Integer, Integer> integerDivided(int sum, int numPieces) {
    int base = sum / numPieces;
    int numInc = sum % numPieces;
    int numBases = numPieces - numInc;
    TreeMap<Integer, Integer> ret = new TreeMap<Integer, Integer>();
    ret.put(base, numBases);
    if (numInc != 0) {
      ret.put(base + 1, numInc);
    }
    return ret;
  }

  @ClojureClass(className = "backtype.storm.util#tokenize-path")
  public static List<String> tokenizePath(String path) {
    String[] toks = path.split("/");
    java.util.ArrayList<String> rtn = new ArrayList<String>();
    for (String str : toks) {
      if (!str.isEmpty()) {
        rtn.add(str);
      }
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#toks->path")
  public static String toksToPath(List<String> toks) {
    StringBuffer buff = new StringBuffer();
    buff.append("/");
    int size = toks.size();
    for (int i = 0; i < size; i++) {
      buff.append(toks.get(i));
      if (i < (size - 1)) {
        buff.append("/");
      }

    }
    return buff.toString();
  }

  public static String localTempPath() {
    return System.getProperty("java.io.tmpdir") + "/" + uuid();
  }

  @ClojureClass(className = "backtype.storm.util#parent-path")
  public static String parentPath(String path) {
    List<String> toks = tokenizePath(path);
    int size = toks.size();
    if (size > 0) {
      toks.remove(size - 1);
    }
    return toksToPath(toks);
  }

  @ClojureClass(className = "backtype.storm.util#full-path")
  public static String fullPath(String parent, String name) {
    return normalizePath(parent + "/" + name);
  }

  @ClojureClass(className = "backtype.storm.util#normalize-path")
  public static String normalizePath(String path) {
    String rtn = toksToPath(tokenizePath(path));
    return rtn;
  }

  @ClojureClass(className = "http://clojuredocs.org/clojure_core/clojure.core/select-keys")
  public static <K, V> Map<K, V> select_keys(Map<K, V> all, Set<K> filter) {
    // user=> (select-keys {:a 1 :b 2} [:a])
    // {:a 1}
    Map<K, V> filterMap = new HashMap<K, V>();

    for (Entry<K, V> entry : all.entrySet()) {
      if (filter.contains(entry.getKey())) {
        filterMap.put(entry.getKey(), entry.getValue());
      }
    }
    return filterMap;
  }

  @ClojureClass(className = "backtype.storm.util#select-keys-pred")
  public static <K, V> Map<K, V> select_keys_pred(Set<K> filter, Map<K, V> all) {
    Map<K, V> filterMap = new HashMap<K, V>();

    for (Entry<K, V> entry : all.entrySet()) {
      if (!filter.contains(entry.getKey())) {
        filterMap.put(entry.getKey(), entry.getValue());
      }
    }

    return filterMap;
  }

  @ClojureClass(className = "backtype.storm.util#barr")
  public static byte[] barr(byte v) {
    byte[] byteArray = new byte[1];
    byteArray[0] = v;

    return byteArray;
  }

  @ClojureClass(className = "backtype.storm.util#reverse-map")
  public static <K, V> HashMap<V, List<K>> reverse_map(Map<K, V> map) {
    // "{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"
    HashMap<V, List<K>> rtn = new HashMap<V, List<K>>();
    if (map == null) {
      return rtn;
    }
    for (Entry<K, V> entry : map.entrySet()) {
      K key = entry.getKey();
      V val = entry.getValue();
      List<K> list = rtn.get(val);
      if (list == null) {
        list = new ArrayList<K>();
        rtn.put(entry.getValue(), list);
      }
      list.add(key);

    }

    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#map-diff")
  public static <K, V> HashMap<K, V> map_diff(Map<K, V> m1, Map<K, V> m2) {
    // "Returns mappings in m2 that aren't in m1"
    Map<K, V> ret = new HashMap<K, V>();
    for (Entry<K, V> entry : m2.entrySet()) {
      K key = entry.getKey();
      V val = entry.getValue();
      if (!m1.containsKey(key)) {
        ret.put(key, val);
      } else if (m1.containsKey(key) && !m1.get(key).equals(val)) {
        ret.put(key, val);
      }
    }
    return (HashMap<K, V>) ret;

  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.util#collectify")
  public static Collection<Object> collectify(Object obj) {
    if (obj instanceof Sequential || obj instanceof Collection) {
      return (Collection<Object>) obj;
    }
    return Arrays.asList(obj);
  }

  @ClojureClass(className = "backtype.storm.util#local-hostname")
  public static String localHostname() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }

  @ClojureClass(className = "backtype.storm.util#uuid")
  public static String uuid() {
    return UUID.randomUUID().toString();
  }

  @ClojureClass(className = "backtype.storm.util#sleep-secs")
  public static void sleepSecs(long secs) throws InterruptedException {
    if (secs > 0) {
      Time.sleep(1000L * secs);
    }
  }

  @ClojureClass(className = "backtype.storm.util#sleep-until-secs")
  public static void sleepUntilSecs(long targetSecs)
      throws InterruptedException {
    Time.sleepUntil(targetSecs * 1000);
  }

  @ClojureClass(className = "backtype.storm.util#secs-to-millis-long")
  public static long secsToMillisLong(int secs) {
    return (long) (1000L * secs);
  }

  @ClojureClass(className = "backtype.storm.util#secs-to-millis-long")
  public static long secsToMillisLong(double secs) {
    return (long) (1000L * secs);
  }

  @ClojureClass(className = "backtype.storm.util#current-time-secs")
  public static int current_time_secs() {
    return Time.currentTimeSecs();
  }

  @ClojureClass(className = "backtype.storm.util#time-delta")
  public static int timeDelta(int timeSecs) {
    return current_time_secs() - timeSecs;
  }

  @ClojureClass(className = "backtype.storm.util#time-delta-ms")
  public static long time_delta_ms(long time_ms) {
    return System.currentTimeMillis() - time_ms;
  }

  @ClojureClass(className = "backtype.storm.util#interleave-all")
  public static <T> List<T> interleaveAll(List<List<T>> nodeList) {
    if (null != nodeList && nodeList.size() > 0) {
      List<T> first = new ArrayList<T>();
      List<List<T>> rest = new ArrayList<List<T>>();
      for (List<T> node : nodeList) {
        if (null != node && node.size() > 0) {
          first.add(node.get(0));
          rest.add(node.subList(1, node.size()));
        }
      }
      List<T> interleaveRest = interleaveAll(rest);
      if (null != interleaveRest) {
        first.addAll(interleaveRest);
      }
      return first;
    }
    return null;
  }

  @ClojureClass(className = "backtype.storm.util#uptime-computer")
  public static int uptimeComputer() {
    return timeDelta(current_time_secs());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @ClojureClass(className = "http://clojuredocs.org/clojure_core/clojure.core/bit-xor")
  public static Long bit_xor(Object a, Object b) {
    // Bitwise exclusive or
    Long rtn = 0l;

    if (a instanceof Long && b instanceof Long) {
      rtn = ((Long) a) ^ ((Long) b);
      return rtn;
    } else if (b instanceof Set) {
      Long bs = bit_xor_vals_sets((Set) b);
      return bit_xor(a, bs);
    } else if (a instanceof Set) {
      Long as = bit_xor_vals_sets((Set) a);
      return bit_xor(as, b);
    } else {
      Long ai = Long.parseLong(String.valueOf(a));
      Long bi = Long.parseLong(String.valueOf(b));
      rtn = ai ^ bi;
      return rtn;
    }
  }

  public static <T> Long bit_xor_vals_sets(java.util.Set<T> vals) {
    Long rtn = 0l;
    for (T n : vals) {
      rtn = bit_xor(rtn, n);
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#bit-xor-vals")
  public static Long bit_xor_vals(Object... vals) {
    Long rtn = 0l;
    for (Object n : vals) {
      rtn = bit_xor(rtn, n);
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#bit-xor-vals")
  public static <T> Long bit_xor_vals(java.util.List<T> vals) {
    Long rtn = 0l;
    for (T n : vals) {
      rtn = bit_xor(rtn, n);
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#any-intersection")
  public static List<String> anyIntersection(List<String> list) {

    List<String> rtn = new ArrayList<String>();
    Set<String> idSet = new HashSet<String>();

    for (String id : list) {
      if (idSet.contains(id)) {
        rtn.add(id);
      } else {
        idSet.add(id);
      }
    }

    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#stringify-error")
  public static String stringifyError(Throwable error) {
    StringWriter result = new StringWriter();
    PrintWriter printer = new PrintWriter(result);
    error.printStackTrace(printer);
    printer.close();
    return result.toString();
  }

  /**
   * Return a set that is the first set without elements of the remaining sets
   * 
   * see https
   * ://google-collections.googlecode.com/svn/trunk/javadoc/com/google/
   * common/collect/Sets.html#difference(java.util.Set, java.util.Set)
   */
  @ClojureClass(className = "http://clojuredocs.org/clojure.set/difference")
  public static <T> Set<T> set_difference(Set<T> keySet, Set<T> keySet2) {
    return Sets.difference(keySet, keySet2);
  }

  public static Object add(Object oldValue, Object newValue) {
    if (oldValue == null) {
      return newValue;
    }
    if (oldValue instanceof Long) {
      if (newValue == null) {
        return (Long) oldValue;
      } else {
        return (Long) oldValue + Long.valueOf(String.valueOf(newValue));
      }
    } else if (oldValue instanceof Integer) {
      if (newValue == null) {
        return (Integer) oldValue;
      } else {
        return (Integer) oldValue + Integer.valueOf(String.valueOf(newValue));
      }
    } else if (oldValue instanceof Double) {
      if (newValue == null) {
        return (Double) oldValue;
      } else {
        return (Double) oldValue + Double.valueOf(String.valueOf(newValue));
      }
    } else if (oldValue instanceof Float) {
      if (newValue == null) {
        return (Float) oldValue;
      } else {
        return (Float) oldValue + Float.valueOf(String.valueOf(newValue));
      }
    } else {
      return null;
    }
  }

  public static <V1, V2> Object multiply(V1 value1, V2 value2) {
    Object ret = null;
    if (null == value1 && null == value2) {
      ret = 0;
    } else if (null == value1) {
      ret = value2;
    } else if (null == value2) {
      ret = value1;
    } else {
      if (value1 instanceof Integer) {
        if (value2 instanceof Integer) {
          ret = (Integer) value1 * (Integer) value2;
        } else if (value2 instanceof Long) {
          ret = (Integer) value1 * (Long) value2;
        } else if (value2 instanceof Float) {
          ret = (Integer) value1 * (Float) value2;
        } else if (value2 instanceof Double) {
          ret = (Integer) value1 * (Double) value2;
        } else {
          ret = 0;
        }
      } else if (value1 instanceof Long) {
        if (value2 instanceof Integer) {
          ret = (Long) value1 * (Integer) value2;
        } else if (value2 instanceof Long) {
          ret = (Long) value1 * (Long) value2;
        } else if (value2 instanceof Float) {
          ret = (Long) value1 * (Float) value2;
        } else if (value2 instanceof Double) {
          ret = (Long) value1 * (Double) value2;
        } else {
          ret = 0L;
        }
      } else if (value1 instanceof Float) {
        if (value2 instanceof Integer) {
          ret = (Float) value1 * (Integer) value2;
        } else if (value2 instanceof Long) {
          ret = (Float) value1 * (Long) value2;
        } else if (value2 instanceof Float) {
          ret = (Float) value1 * (Float) value2;
        } else if (value2 instanceof Double) {
          ret = (Float) value1 * (Double) value2;
        } else {
          ret = 0.0f;
        }
      } else if (value1 instanceof Double) {
        if (value2 instanceof Integer) {
          ret = (Double) value1 * (Integer) value2;
        } else if (value2 instanceof Long) {
          ret = (Double) value1 * (Long) value2;
        } else if (value2 instanceof Float) {
          ret = (Double) value1 * (Float) value2;
        } else if (value2 instanceof Double) {
          ret = (Double) value1 * (Double) value2;
        } else {
          ret = 0.0d;
        }
      } else {
        return 0;
      }
    }
    return ret;
  }

  public static <V> Object divide(V dividend, V divisor) {
    if (null == divisor) {
      throw new IllegalArgumentException("divisor can't be null");
    } else if (null == dividend) {
      return 0;
    } else {
      if (dividend instanceof Integer) {
        if (Integer.valueOf(String.valueOf(divisor)) == 0) {
          throw new IllegalArgumentException("divisor can't be 0");
        }

        return (Double) dividend / Integer.valueOf(String.valueOf(divisor));
      } else if (dividend instanceof Long) {
        if (Long.valueOf(String.valueOf(divisor)) == 0L) {
          throw new IllegalArgumentException("divisor can't be 0");
        }

        return (Double) dividend / Long.valueOf(String.valueOf(divisor));
      } else if (dividend instanceof Float) {
        if (Float.valueOf(String.valueOf(divisor)).equals(0.0)) {
          throw new IllegalArgumentException("divisor can't be 0");
        }

        return (Float) dividend / Float.valueOf(String.valueOf(divisor));
      } else if (dividend instanceof Double) {
        if (Double.valueOf(String.valueOf(divisor)).equals(0.0)) {
          throw new IllegalArgumentException("divisor can't be 0");
        }
        return (Double) dividend / Double.valueOf(String.valueOf(divisor));
      } else {
        return 0;
      }
    }
  }

  public static Object mergeList(List<Object> list) {
    Object ret = null;

    for (Object value : list) {
      ret = add(ret, value);
    }

    return ret;
  }

  @SuppressWarnings("rawtypes")
  public static List<Object> mergeList(List<Object> result, Object add) {
    if (add instanceof Collection) {
      for (Object o : (Collection) add) {
        result.add(o);
      }
    } else if (add instanceof Set) {
      for (Object o : (Collection) add) {
        result.add(o);
      }
    } else {
      result.add(add);
    }

    return result;
  }

  /**
   * merger with for map
   * 
   * @param list
   * @return
   */
  @SuppressWarnings("unchecked")
  @ClojureClass(className = "http://clojuredocs.org/clojure.core/merge-with")
  public static <K, V> Map<K, V> mergeWith(List<Map<K, V>> list) {
    Map<K, V> ret = new HashMap<K, V>();

    for (Map<K, V> listEntry : list) {
      if (listEntry == null) {
        continue;
      }
      for (Entry<K, V> mapEntry : listEntry.entrySet()) {
        K key = mapEntry.getKey();
        V value = mapEntry.getValue();

        V retValue = (V) add(ret.get(key), value);

        ret.put(key, retValue);
      }
    }

    return ret;
  }

  /**
   * merge with for map
   * 
   * @param maps
   * @return
   */
  @ClojureClass(className = "http://clojuredocs.org/clojure.core/merge-with")
  public static <K, V> Map<K, V> mergeWith(Map<K, V>... maps) {
    return mergeWith(Arrays.asList(maps));
  }

  /**
   * Returns a new set containing all elements that are contained in both given
   * 
   * @see https
   *      ://google-collections.googlecode.com/svn/trunk/javadoc/com/google/
   *      common/collect/Sets.html#intersection(java.util.Set, java.util.Set)
   */
  @SuppressWarnings("unchecked")
  @ClojureClass(className = "http://clojuredocs.org/clojure.set/intersection")
  public static <E> Set<E> SetIntersection(final Set<? extends E> set1,
      final Set<? extends E> set2) {
    return (Set<E>) Sets.intersection(set1, set2);
  }

  /**
   * Returns a lazy sequence of the elements of coll with duplicates removed
   * 
   * @param <T>
   */
  @ClojureClass(className = "http://clojuredocs.org/clojure.core/distinct")
  public static <T> List<T> distinctList(List<T> input) {
    List<T> retList = new ArrayList<T>();

    for (T object : input) {
      if (retList.contains(object)) {
        continue;
      } else {
        retList.add(object);
      }

    }

    return retList;
  }

  public static Long parseLong(Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof String) {
      return Long.valueOf(String.valueOf(o));
    } else if (o instanceof Integer) {
      Integer value = (Integer) o;
      return Long.valueOf((Integer) value);
    } else if (o instanceof Long) {
      return (Long) o;
    } else {
      throw new RuntimeException("Invalid value " + o.getClass().getName()
          + " " + o);
    }
  }

  public static Float parseFloat(Object o, float defaultValue) {
    if (o == null) {
      return defaultValue;
    }

    if (o instanceof String) {
      return Float.valueOf(String.valueOf(o));
    } else if (o instanceof Integer) {
      Integer value = (Integer) o;
      return Float.valueOf((Integer) value);
    } else if (o instanceof Long) {
      Long value = (Long) o;
      return Float.valueOf((Long) value);
    } else if (o instanceof Float) {
      return (Float) o;
    } else {
      return defaultValue;
    }
  }

  public static Long parseLong(Object o, long defaultValue) {

    if (o == null) {
      return defaultValue;
    }

    if (o instanceof String) {
      return Long.valueOf(String.valueOf(o));
    } else if (o instanceof Integer) {
      Integer value = (Integer) o;
      return Long.valueOf((Integer) value);
    } else if (o instanceof Long) {
      return (Long) o;
    } else {
      return defaultValue;
    }
  }

  public static Integer parseInt(Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof String) {
      return Integer.parseInt(String.valueOf(o));
    } else if (o instanceof Long) {
      long value = (Long) o;
      return Integer.valueOf((int) value);
    } else if (o instanceof Integer) {
      return (Integer) o;
    } else {
      throw new RuntimeException("Invalid value " + o.getClass().getName()
          + " " + o);
    }
  }

  public static Integer parseInt(Object o, Integer defaultValue) {
    if (o == null) {
      return defaultValue;
    }
    if (o instanceof String) {
      return Integer.parseInt(String.valueOf(o));
    } else if (o instanceof Long) {
      long value = (Long) o;
      return Integer.valueOf((int) value);
    } else if (o instanceof Integer) {
      return (Integer) o;
    } else {
      return defaultValue;
    }
  }

  public static boolean parseBoolean(Object o, boolean defaultValue) {
    if (o == null) {
      return defaultValue;
    }

    if (o instanceof String) {
      return Boolean.valueOf((String) o);
    } else if (o instanceof Boolean) {
      return (Boolean) o;
    } else {
      return defaultValue;
    }
  }

  public static String parseString(Object o, String defaultValue) {
    if (o == null) {
      return defaultValue;
    }
    return String.valueOf(o);
  }

  @SuppressWarnings("unchecked")
  public static <K> List<K> parseList(Object o, List<K> defaultValue) {
    if (o == null) {
      return defaultValue;
    }
    return (List<K>) o;
  }

  public static <K, V> V mapValue(Map<K, V> map, K key) {
    return null != map ? map.get(key) : null;
  }

  @ClojureClass(className = "backtype.storm.util#logs-rootname")
  private static String logsRootname(String stormId, Integer port) {
    return stormId + "-worker-" + port;
  }

  @ClojureClass(className = "backtype.storm.util#logs-filename")
  public static String logsFilename(String stormId, Integer port) {
    return logsRootname(stormId, port) + ".log";
  }

  @ClojureClass(className = "backtype.storm.util#logs-metadata-filename")
  public static String logsMetadataFilename(String stormId, Integer port) {
    return logsRootname(stormId, port) + ".yaml";
  }

  @ClojureClass(className = "backtype.storm.util#worker-log-filename-pattern")
  // TODO
  public static Pattern workerLogFilenamePattern = Pattern.compile("");

  @ClojureClass(className = "backtype.storm.util#get-log-metadata-file")
  public static String getLogMetadataFile(String fname) {
    // TODO
    return fname;
  }

  @ClojureClass(className = "backtype.storm.util#get-log-metadata-file")
  public String getLogMetadataFile(String id, Integer port) throws IOException {
    String LOG_DIR =
        new File(System.getProperty("storm.home") + "logs").getCanonicalPath();
    return LOG_DIR + "metadata" + logsMetadataFilename(id, port);
  }

}
