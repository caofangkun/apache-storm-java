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
package org.apache.storm.daemon.worker.messaging.zeroMq;

import java.net.Socket;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.netty.Context;
import backtype.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.messaging.zmq#ZMQContext")
public class MQContext implements IContext, ZMQContextQuery {
  private final static Logger LOG = LoggerFactory.getLogger(MQContext.class);
  private org.zeromq.ZMQ.Context context;
  private int lingerMs;
  private boolean isLocal;
  private int hwm;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void prepare(Map stormConf) {
    int numThreads = Utils.getInt(stormConf.get(Config.ZMQ_THREADS), 1);
    lingerMs = Utils.getInt(stormConf.get(Config.ZMQ_LINGER_MILLIS), 5000);
    hwm = Utils.getInt(stormConf.get(Config.ZMQ_HWM), 0);
    isLocal = ConfigUtils.clusterMode(stormConf).equals("local");
    context = ZeroMq.context(numThreads);
    LOG.info("MQContext prepare done...");
  }

  @Override
  public IConnection bind(String stormId, int port) {
    String bindUrl = this.getBindZmqUrl(isLocal, port);
    Socket socket = ZeroMq.socket(context, ZeroMq.pull);
    socket = ZeroMq.set_hwm(socket, hwm);
    socket = ZeroMq.bind(socket, bindUrl);
    LOG.info("Create zmq receiver {}", bindUrl);
    return new ZMQRecvConnection(socket);
  }

  @Override
  public IConnection connect(String stormId, String host, int port) {
    String connectUrl = this.getConnectZmqUrl(isLocal, host, port);
    Socket socket = ZeroMq.socket(context, ZeroMq.push);
    socket = ZeroMq.set_hwm(socket, hwm);
    socket = ZeroMq.set_linger(socket, lingerMs);
    socket = ZeroMq.connect(socket, connectUrl);
    LOG.info("Create zmq sender {}", connectUrl);
    return new ZMQSendConnection(socket);
  }

  public void term() {
    LOG.info("ZMQ context terminates ");
    context.term();
  }

  @ClojureClass(className = "backtype.storm.messaging.zmq#get-bind-zmq-url")
  private String getBindZmqUrl(boolean isLocal, int port) {
    if (isLocal) {
      return "ipc://" + port + ".ipc";
    } else {
      return "tcp://*:" + port;
    }
  }

  @ClojureClass(className = "backtype.storm.messaging.zmq#get-connect-zmq-url")
  private String getConnectZmqUrl(boolean isLocal, String host, int port) {
    if (isLocal) {
      return "ipc://" + port + ".ipc";
    } else {
      return "tcp://" + host + ":" + port;
    }
  }

  @Override
  public Context zmqContext() {
    return context;
  }
}
