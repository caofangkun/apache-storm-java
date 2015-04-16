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
package org.apache.storm.daemon.worker.executor.spout.strateges;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.ISpoutWaitStrategy;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class AdaptationWaitStrategy implements ISpoutWaitStrategy {

  private final static Logger LOG = LoggerFactory
      .getLogger(AdaptationWaitStrategy.class);

  private long sleepMillis;

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map conf) {
    this.sleepMillis =
        ((Number) conf.get(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS))
            .longValue();
  }

  @Override
  public void emptyEmit(long streak) {
    try {
      long curMills = sleepMillis + streak;
      LOG.info("Sleep millis {} ... ", curMills);
      Thread.sleep(curMills);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
