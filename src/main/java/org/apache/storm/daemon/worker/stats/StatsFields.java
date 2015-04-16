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
package org.apache.storm.daemon.worker.stats;

import org.apache.storm.ClojureClass;

@ClojureClass(className = "backtype.storm.stats#COMMON-FIELDS, SPOUT-FIELDS,BOLT-FIELDS")
public enum StatsFields {
  // common:0 bolt 1 spout 2 bolt&spout 3

  emitted(0), transferred(0), acked(3), failed(3), complete_latencies(2), process_latencies(
      1), executed(1), execute_latencies(1);

  private StatsFields(int flag) {
    this.flag = flag;
  }

  private int flag;

  public int getFlag() {
    return flag;
  }

}
