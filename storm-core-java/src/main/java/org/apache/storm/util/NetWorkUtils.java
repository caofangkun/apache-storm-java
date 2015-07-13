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
package org.apache.storm.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class NetWorkUtils {
  private static Logger LOG = LoggerFactory.getLogger(NetWorkUtils.class);

  private static Map<String, String> hostToResolved =
      new HashMap<String, String>();

  public static String hostname() throws UnknownHostException {
    String hostname = null;
    hostname = InetAddress.getLocalHost().getCanonicalHostName();
    return hostname;
  }

  /**
   * Check whether the port is available to binding
   * 
   * @param port
   * @return -1 means not available, others means available
   * @throws IOException
   */
  public static int tryPort(int port) throws IOException {
    ServerSocket socket = new ServerSocket(port);
    int rtn = socket.getLocalPort();
    socket.close();
    return rtn;
  }

  /**
   * get one available port
   * 
   * @return -1 means failed, others means one availablePort
   */
  public static int getAvailablePort() {
    return availablePort(0);
  }

  /**
   * Check whether the port is available to binding
   * 
   * @param prefered
   * @return -1 means not available, others means available
   */
  public static int availablePort(int prefered) {
    int rtn = -1;
    try {
      rtn = tryPort(prefered);
    } catch (IOException e) {

    }
    return rtn;
  }

  public static boolean isPortBind(int port) {
    try {
      bindPort("0.0.0.0", port);
      bindPort(InetAddress.getLocalHost().getHostAddress(), port);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static void bindPort(String host, int port) throws IOException {
    Socket s = new Socket();
    s.bind(new InetSocketAddress(host, port));
    s.close();
  }

  public static int assignServerPort(int port) throws IOException {
    // allocate an available server port if port_in_conf<=0
    if (port > 0 && !isPortBind(port)) {
      throw new PortAlreadyUsedException("port:" + port
          + " has already been used!");
    }
    return assignAvailableServerPort(port);
  }

  public static int assignAvailableServerPort(int port) throws IOException {
    // allocate an available server port if port_in_conf<=0
    if (port > 0 && isPortBind(port)) {
      return port;
    }
    ServerSocket ss = new ServerSocket(0);
    int portAssigned = ss.getLocalPort();
    ss.close();
    LOG.debug("Grabbed port number " + portAssigned
        + " instead of the configured port " + port);
    return portAssigned;
  }

  public static InetSocketAddress createSocketAddr(String target)
      throws MalformedURLException {
    return createSocketAddr(target, 8080);
  }

  public static InetSocketAddress createSocketAddr(String target,
      int defaultPort) throws MalformedURLException {
    if (target == null) {
      throw new IllegalArgumentException("Target address cannot be null.");
    }
    int colonIndex = target.indexOf(':');
    if (colonIndex < 0 && defaultPort == -1) {
      throw new RuntimeException("Not a host:port pair: " + target);
    }
    String hostname;
    int port = -1;
    if (!target.contains("/")) {
      if (colonIndex == -1) {
        hostname = target;
      } else {
        // must be the old style <host>:<port>
        hostname = target.substring(0, colonIndex);
        port = Integer.parseInt(target.substring(colonIndex + 1));
      }
    } else {
      // a new uri
      URL addr = new URL(target);
      hostname = addr.getHost();
      port = addr.getPort();
    }
    if (port == -1) {
      port = defaultPort;
    }
    if (getStaticResolution(hostname) != null) {
      hostname = getStaticResolution(hostname);
    }
    return new InetSocketAddress(hostname, port);
  }

  public static String getStaticResolution(String host) {
    synchronized (hostToResolved) {
      return hostToResolved.get(host);
    }
  }
}
