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
package org.apache.storm.daemon.nimbus.transitions;

import backtype.storm.ClojureClass;

import com.tencent.jstorm.cluster.StormStatus;
import com.tencent.jstorm.utils.thread.BaseCallback;

@ClojureClass(className = "backtype.storm.daemon.nimbus#state-transitions#active")
public class ActiveTransitionCallback extends BaseCallback {

  @Override
  public <T> Object execute(T... args) {

    return new StormStatus(StatusType.active);
  }

}