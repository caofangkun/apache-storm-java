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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.cluster.StormZkClusterState;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.nimbus.transitions.StateTransitions;
import org.apache.storm.daemon.worker.executor.ExecutorCache;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.zookeeper.data.ACL;

import backtype.storm.Config;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.nimbus.ITopologyValidator;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.security.INimbusCredentialPlugin;
import backtype.storm.security.auth.AuthUtils;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ICredentialsRenewer;
import backtype.storm.utils.Utils;

import com.esotericsoftware.minlog.Log;

@ClojureClass(className = "backtype.storm.daemon.nimbus#nimbus-data")
public class NimbusData implements Serializable {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("rawtypes")
  private Map conf;
  private List<ACL> acls = null;
  private StormClusterState stormClusterState;
  private ConcurrentHashMap<String, Map<ExecutorInfo, ExecutorCache>> executorHeartbeatsCache;
  private FileCacheMap<Object, Object> downloaders;
  private FileCacheMap<Object, Object> uploaders;
  private int startTime;
  private ITopologyValidator validator;
  private ScheduledExecutorService scheduExec;
  private AtomicInteger submittedCount;
  private Object submitLock;
  private Object credUpdateLock;
  private StateTransitions statusTransition;
  public static final int SCHEDULE_THREAD_NUM = 8;
  private final INimbus inimubs;
  private final IAuthorizer authorizationHandler;
  private final IAuthorizer impersonationAuthorizationHandler;
  private final IScheduler scheduler;
  private Map<String, String> idToSchedStatus;
  private Collection<ICredentialsRenewer> credRenewers;
  private Collection<INimbusCredentialPlugin> nimbusAutocredPlugins;

  @SuppressWarnings("rawtypes")
  public NimbusData(Map conf, INimbus inimbus) throws Exception {
    this.conf = conf;
    this.inimubs = inimbus;
    this.authorizationHandler =
        Common.mkAuthorizationHandler(
            (String) conf.get(Config.NIMBUS_AUTHORIZER), conf);
    this.impersonationAuthorizationHandler =
        Common.mkAuthorizationHandler(
            (String) conf.get(Config.NIMBUS_IMPERSONATION_AUTHORIZER), conf);
    this.submittedCount = new AtomicInteger(0);
    if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
      this.acls = NimbusUtils.nimbusZKAcls();
    }
    this.stormClusterState = new StormZkClusterState(conf, acls);
    this.submitLock = new Object();
    this.credUpdateLock = new Object();
    this.executorHeartbeatsCache =
        new ConcurrentHashMap<String, Map<ExecutorInfo, ExecutorCache>>();
    this.downloaders = new FileCacheMap<Object, Object>(conf);
    this.uploaders = new FileCacheMap<Object, Object>(conf);
    this.scheduExec = Executors.newScheduledThreadPool(SCHEDULE_THREAD_NUM);
    this.statusTransition = new StateTransitions(this);
    this.startTime = CoreUtil.current_time_secs();
    this.validator = NimbusUtils.mkTopologyValidator(conf);
    this.scheduler = NimbusUtils.mkScheduler(conf, inimbus);
    this.idToSchedStatus = new HashMap<String, String>();
    this.credRenewers = AuthUtils.GetCredentialRenewers(conf);
    this.nimbusAutocredPlugins = AuthUtils.getNimbusAutoCredPlugins(conf);
  }

  public int uptime() {
    return (CoreUtil.current_time_secs() - startTime);
  }

  @SuppressWarnings("rawtypes")
  public Map getConf() {
    return conf;
  }

  @SuppressWarnings("rawtypes")
  public void setConf(Map conf) {
    this.conf = conf;
  }

  public StormClusterState getStormClusterState() {
    return stormClusterState;
  }

  public void setStormClusterState(StormClusterState stormClusterState) {
    this.stormClusterState = stormClusterState;
  }

  public ConcurrentHashMap<String, Map<ExecutorInfo, ExecutorCache>> getExecutorHeartbeatsCache() {
    return this.executorHeartbeatsCache;
  }

  public void setExecutorHeartbeatsCache(
      ConcurrentHashMap<String, Map<ExecutorInfo, ExecutorCache>> executorCache) {
    this.executorHeartbeatsCache = executorCache;
  }

  public FileCacheMap<Object, Object> getDownloaders() {
    return downloaders;
  }

  public void setDownloaders(FileCacheMap<Object, Object> downloaders) {
    this.downloaders = downloaders;
  }

  public IAuthorizer getImpersonationAuthorizationHandler() {
    return impersonationAuthorizationHandler;
  }

  public Collection<INimbusCredentialPlugin> getNimbusAutocredPlugins() {
    return nimbusAutocredPlugins;
  }

  public void setNimbusAutocredPlugins(
      Collection<INimbusCredentialPlugin> nimbusAutocredPlugins) {
    this.nimbusAutocredPlugins = nimbusAutocredPlugins;
  }

  public Map<String, String> getIdToSchedStatus() {
    return idToSchedStatus;
  }

  public void setIdToSchedStatus(Map<String, String> idToSchedStatus) {
    this.idToSchedStatus = idToSchedStatus;
  }

  public FileCacheMap<Object, Object> getUploaders() {
    return uploaders;
  }

  public void setUploaders(FileCacheMap<Object, Object> uploaders) {
    this.uploaders = uploaders;
  }

  public int getStartTime() {
    return startTime;
  }

  public void setStartTime(int startTime) {
    this.startTime = startTime;
  }

  public IAuthorizer getAuthorizationHandler() {
    return authorizationHandler;
  }

  public Collection<ICredentialsRenewer> getCredRenewers() {
    return credRenewers;
  }

  public void setCredRenewers(Collection<ICredentialsRenewer> credRenewers) {
    this.credRenewers = credRenewers;
  }

  public AtomicInteger getSubmittedCount() {
    return submittedCount;
  }

  public void setSubmittedCount(AtomicInteger submittedCount) {
    this.submittedCount = submittedCount;
  }

  public Object getSubmitLock() {
    return submitLock;
  }

  public Object getCredUpdateLock() {
    return credUpdateLock;
  }

  public void setCredUpdateLock(Object credUpdateLock) {
    this.credUpdateLock = credUpdateLock;
  }

  public ScheduledExecutorService getScheduExec() {
    return scheduExec;
  }

  public void setScheduExec(ScheduledExecutorService ses) {
    this.scheduExec = ses;
  }

  public StateTransitions getStatusTransition() {
    return statusTransition;
  }

  public void cleanup() {
    try {
      stormClusterState.disconnect();
    } catch (Exception e) {
      Log.error("Error when disconnect StormClusterState for "
          + CoreUtil.stringifyError(e));
    }
  }

  public INimbus getInimubs() {
    return inimubs;
  }

  public IScheduler getScheduler() {
    return scheduler;
  }

  public ITopologyValidator getValidator() {
    return validator;
  }

  public void setValidator(ITopologyValidator validator) {
    this.validator = validator;
  }
}
