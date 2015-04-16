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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.zeromq.ZMQ.Socket;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

public class ZMQSendConnection implements IConnection {
  private org.zeromq.ZMQ.Socket socket;
  private boolean closed = false;

  public ZMQSendConnection(Socket _socket) {
    socket = _socket;
  }

  @Override
  public void send(int taskId, byte[] payload) {
    TaskMessage msg = new TaskMessage(taskId, payload);
    List<TaskMessage> wrapper = new ArrayList<TaskMessage>(1);
    wrapper.add(msg);
    send(wrapper.iterator());
  }

  @Override
  public void send(Iterator<TaskMessage> msgs) {
    if (null == msgs || !msgs.hasNext()) {
      return;
    }
    while (msgs.hasNext()) {
      List<TaskMessage> ret = new ArrayList<TaskMessage>();
      TaskMessage message = msgs.next();
      byte[] bytes = mkPacket(message);
      ZeroMq.send(socket, bytes);
    }
  }

  public byte[] mkPacket(TaskMessage msg) {
    return msg.serialize().array();
  }

  @Override
  public void close() {
    socket.close();
    closed = true;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public Iterator<TaskMessage> recv(int flags, int receiverId) {
    throw new UnsupportedOperationException(
        "Client connection should not receive any messages");
  }
}
