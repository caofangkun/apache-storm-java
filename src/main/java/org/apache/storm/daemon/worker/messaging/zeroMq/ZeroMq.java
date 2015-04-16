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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * Wrapper zeroMQ interface
 */
public class ZeroMq {
  private static Logger LOG = LoggerFactory.getLogger(ZeroMq.class);

  public static Context context(int threads) {
    try {
      return ZMQ.context(threads);
    } catch (UnsatisfiedLinkError e) {
      LOG.error("Failed to create zeroMQ context", e);
      throw new RuntimeException(e);
    }
  }

  public static int sndmore = ZMQ.SNDMORE;
  public static int req = ZMQ.REQ;
  public static int rep = ZMQ.REP;
  public static int xreq = ZMQ.XREQ;
  public static int xrep = ZMQ.XREP;
  public static int pub = ZMQ.PUB;
  public static int sub = ZMQ.SUB;
  public static int pair = ZMQ.PAIR;
  public final static int push = ZMQ.PUSH;
  public final static int pull = ZMQ.PULL;

  public static byte[] barr(Short v) {
    byte[] byteArray = new byte[Short.SIZE / 8];
    for (int i = 0; i < byteArray.length; i++) {
      int off = (byteArray.length - 1 - i) * 8;
      byteArray[i] = (byte) ((v >> off) & 0xFF);
    }
    return byteArray;
  }

  public static byte[] barr(Integer v) {
    byte[] byteArray = new byte[Integer.SIZE / 8];
    for (int i = 0; i < byteArray.length; i++) {
      int off = (byteArray.length - 1 - i) * 8;
      byteArray[i] = (byte) ((v >> off) & 0xFF);
    }
    return byteArray;
  }

  public static Socket socket(Context context, int type) {
    return context.socket(type);
  }

  public static Socket set_linger(Socket socket, long linger_ms) {
    socket.setLinger(linger_ms);
    return socket;
  }

  public static Socket set_hwm(Socket socket, long hwm) {
    socket.setHWM(hwm);
    return socket;
  }

  public static Socket bind(Socket socket, String url) {
    socket.bind(url);
    return socket;
  }

  public static Socket connect(Socket socket, String url) {
    socket.connect(url);
    return socket;
  }

  public static Socket subscribe(Socket socket, byte[] topic) {
    socket.subscribe(topic);
    return socket;
  }

  public static Socket subscribe(Socket socket) {
    byte[] topic = {};
    return subscribe(socket, topic);
  }

  public static Socket unsubscribe(Socket socket, byte[] topic) {
    socket.unsubscribe(topic);
    return socket;
  }

  public static Socket unsubscribe(Socket socket) {
    byte[] topic = {};
    return unsubscribe(socket, topic);
  }

  public static Socket send(Socket socket, byte[] message, int flags) {
    socket.send(message, flags);
    return socket;
  }

  public static Socket send(Socket socket, byte[] message) {
    return send(socket, message, ZMQ.NOBLOCK);
  }

  public static byte[] recv(Socket socket, int flags) {
    return socket.recv(flags);
  }

  public static byte[] recv(Socket socket) {
    return recv(socket, 0);
  }

  public static boolean hasRecvMore(Socket socket) {
    return socket.hasReceiveMore();
  }

}
