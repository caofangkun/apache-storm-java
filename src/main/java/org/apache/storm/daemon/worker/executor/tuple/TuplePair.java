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
package org.apache.storm.daemon.worker.executor.tuple;

import org.apache.storm.ClojureClass;

@ClojureClass(className = "backtype.storm.daemon.executor#mk-executor-transfer-fn#[task tuple]"
    + "backtype.storm.messaging.loader#mk-receive-thread#[task message]")
public class TuplePair {
  private Integer outTask;
  private Object outTuple;// byte[] or TupleImpl

  public TuplePair(Integer outTask, Object outTuple) {
    this.outTask = outTask;
    this.outTuple = outTuple;
  }

  public Integer getOutTask() {
    return outTask;
  }

  public void setOutTask(Integer outTask) {
    this.outTask = outTask;
  }

  public Object getOutTuple() {
    return outTuple;
  }

  public void setOutTuple(Object outTuple) {
    this.outTuple = outTuple;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("taskId:").append(outTask).append(";tuple:").append(outTuple);

    return sb.toString();
  }
}
