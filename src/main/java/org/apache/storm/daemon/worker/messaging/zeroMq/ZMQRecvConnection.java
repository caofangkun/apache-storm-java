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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.zeromq.ZMQ.Socket;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

public class ZMQRecvConnection implements IConnection {
  private Socket socket;
  private boolean closed = false;

  public ZMQRecvConnection(Socket _socket) {
    socket = _socket;
  }

  boolean closing = false;
  List<TaskMessage> closeMessage = Arrays.asList(new TaskMessage(-1, null));

  @Override
  public Iterator<TaskMessage> recv(int flags, int receiverId) {
    if (closing) {
      return closeMessage.iterator();
    }

    byte[] packet = ZeroMq.recv(socket, flags);
    if (packet == null) {
      return null;
    }

    TaskMessage msg = parsePacket(packet);
    int task = msg.task();
    if (task == -1) {
      closing = true;
      return closeMessage.iterator();
    }

    List<TaskMessage> wrapper = Arrays.asList(msg);
    return wrapper.iterator();
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
  public void send(int taskId, byte[] payload) {
    throw new UnsupportedOperationException(
        "Server connection should not send any messages");
  }

  public TaskMessage parsePacket(byte[] packet) {
    ByteBuffer bb = ByteBuffer.wrap(packet);
    short port = bb.getShort();
    byte[] msg = new byte[packet.length - 2];
    bb.get(msg);
    return new TaskMessage((int) port, msg);
  }

  @Override
  public void send(Iterator<TaskMessage> msgs) {
    throw new UnsupportedOperationException(
        "Server connection should not send any messages");
  }
}
