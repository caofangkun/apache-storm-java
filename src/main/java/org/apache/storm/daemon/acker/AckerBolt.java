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
package org.apache.storm.daemon.acker;

import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.guava.collect.Lists;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.MutableObject;
import backtype.storm.utils.RotatingMap;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.acker#mk-acker-bolt")
public class AckerBolt implements IBolt {
  private static final Logger LOG = LoggerFactory.getLogger(AckerBolt.class);
  private static final long serialVersionUID = 1L;

  public static final String ACKER_COMPONENT_ID = "__acker";
  public static final String ACKER_INIT_STREAM_ID = "__ack_init";
  public static final String ACKER_ACK_STREAM_ID = "__ack_ack";
  public static final String ACKER_FAIL_STREAM_ID = "__ack_fail";

  public static final int TIMEOUT_BUCKET_NUM = 2;

  private long lastRotate = System.currentTimeMillis();
  private long rotateTime;
  private MutableObject outputCollectorObject = new MutableObject();
  private MutableObject pendingObject = new MutableObject();

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    outputCollectorObject.setObject(collector);
    pendingObject.setObject(new RotatingMap<Object, AckObject>(
        TIMEOUT_BUCKET_NUM));
    this.rotateTime =
        1000L * CoreUtil.parseInt(
            stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    RotatingMap<Object, AckObject> pending =
        (RotatingMap<Object, AckObject>) pendingObject.getObject();
    String streamId = tuple.getSourceStreamId();

    if (streamId.equals(Constants.SYSTEM_TICK_STREAM_ID)) {
      pending.rotate();
    } else {
      Object id = tuple.getValue(0);
      OutputCollector outputCollector =
          (OutputCollector) outputCollectorObject.getObject();
      AckObject curr = pending.get(id);

      if (streamId.equals(AckerBolt.ACKER_INIT_STREAM_ID)) {
        if (curr == null) {
          curr = new AckObject();
          curr.val = tuple.getLong(1);
          curr.spout_task = tuple.getInteger(2);
          pending.put(id, curr);
        } else {
          // bolt's ack first come
          curr.updateAck(tuple.getValue(1));
          curr.spout_task = tuple.getInteger(2);
        }
      } else if (streamId.equals(AckerBolt.ACKER_ACK_STREAM_ID)) {
        if (curr != null) {
          curr.updateAck(tuple.getValue(1));
        } else {
          // two case
          // one is timeout
          // the other is bolt's ack first come
          curr = new AckObject();
          curr.val = tuple.getLong(1);
          pending.put(id, curr);
        }
      } else if (streamId.equals(AckerBolt.ACKER_FAIL_STREAM_ID)) {
        if (curr == null) {
          // do nothing
          // already timeout, should go fail
          return;
        }
        curr.failed = true;
      }

      pending.put(id, curr);

      if (curr != null && curr.spout_task != null) {
        if (curr.val == 0) {
          pending.remove(id);
          outputCollector.emitDirect(curr.spout_task, ACKER_ACK_STREAM_ID,
              Lists.newArrayList(id));
        } else if (curr.failed) {
          pending.remove(id);
          outputCollector.emitDirect(curr.spout_task, ACKER_FAIL_STREAM_ID,
              Lists.newArrayList(id));
        }
      }
      outputCollector.ack(tuple);
    }

    long now = System.currentTimeMillis();
    if (now - lastRotate > rotateTime) {
      lastRotate = now;
      Map<Object, AckObject> tmp = pending.rotate();
      LOG.info("Acker's timeout item size:{}", tmp.size());
    }
  }

  @Override
  public void cleanup() {
    LOG.info("Successfully cleanup");
  }
}
