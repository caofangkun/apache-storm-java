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
package org.apache.storm.daemon.worker.executor.bolt;

import backtype.storm.ClojureClass;
import backtype.storm.tuple.TupleImpl;

import com.tencent.jstorm.utils.ServerUtils;

/**
 * 
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class BoltUtils {

  @ClojureClass(className = "backtype.storm.daemon.executor#tuple-time-delta!")
  public static Long tupleTimeDelta(TupleImpl tuple) {
    Long ms = tuple.getProcessSampleStartTime();
    if (ms != null) {
      return ServerUtils.time_delta_ms(ms);
    } else {
      return null;
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#tuple-execute-time-delta!")
  public static Long tupleExecuteTimeDelta(TupleImpl tuple) {
    Long ms = tuple.getExecuteSampleStartTime();
    if (ms != null) {
      return ServerUtils.time_delta_ms(ms);
    } else {
      return null;
    }
  }

}
