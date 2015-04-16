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
package org.apache.storm.daemon.worker.stats.incval;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.thread.RunnableCallback;

@ClojureClass(className = "backtype.storm.stats#counter-extract")
public class CounterExtractor extends RunnableCallback {

  private static final long serialVersionUID = 1L;

  @SuppressWarnings("unchecked")
  @Override
  public <T> Object execute(T... args) {
    Map<Object, Long> v = null;

    if (args != null && args.length > 0) {
      v = (Map<Object, Long>) args[0];
    }

    if (v == null) {
      v = new HashMap<Object, Long>();
    }
    return v;
  }
}
