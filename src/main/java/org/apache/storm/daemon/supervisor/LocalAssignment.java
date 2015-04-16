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
package org.apache.storm.daemon.supervisor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.storm.ClojureClass;

import backtype.storm.generated.ExecutorInfo;

@ClojureClass(className = "backtype.storm.daemon.supervisor#LocalAssignment")
public class LocalAssignment implements Serializable {
  private static final long serialVersionUID = 1L;

  private String stormId;
  private Set<ExecutorInfo> executors;

  /**
   * used as part of a map from port to this
   * 
   * @param stormId
   * @param executors
   */
  public LocalAssignment(String stormId, Set<ExecutorInfo> executors) {
    this.stormId = stormId;
    this.executors = new HashSet<ExecutorInfo>(executors);
  }

  public String getStormId() {
    return stormId;
  }

  public void setStormId(String topologyId) {
    this.stormId = topologyId;
  }

  public Set<ExecutorInfo> getExecutors() {
    return executors;
  }

  public void setExecutors(Set<ExecutorInfo> taskIds) {
    this.executors = new HashSet<ExecutorInfo>(taskIds);
  }

  @Override
  public boolean equals(Object localAssignment) {
    if (localAssignment instanceof LocalAssignment
        && ((LocalAssignment) localAssignment).getStormId().equals(stormId)
        && ((LocalAssignment) localAssignment).getExecutors().equals(executors)) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.executors.hashCode() + this.stormId.hashCode();
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this,
        ToStringStyle.SHORT_PREFIX_STYLE);
  }

}
