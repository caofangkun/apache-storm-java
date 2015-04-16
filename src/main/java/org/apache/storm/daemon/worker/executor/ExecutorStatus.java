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
package org.apache.storm.daemon.worker.executor;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class ExecutorStatus {
  // task is alive, and it will run BaseExecutor's run
  public static final byte RUN = 0;
  // task is alive, but it won't run BaseExecutor's run
  public static final byte PAUSE = 1;
  // task is shutdown
  public static final byte SHUTDOWN = 2;
  private byte status = ExecutorStatus.RUN;

  public byte getStatus() {
    return status;
  }

  public void setStatus(byte status) {
    this.status = status;
  }

  public boolean isRun() {
    return status == ExecutorStatus.RUN;
  }

  public boolean isShutdown() {
    return status == ExecutorStatus.SHUTDOWN;
  }

  public boolean isPause() {
    return status == ExecutorStatus.PAUSE;
  }

  public String toString() {
    if (isRun()) {
      return "RUN";
    }
    if (isShutdown()) {
      return "SHUTDOWN";
    }
    if (isPause()) {
      return "PAUSE";
    }
    return "UNKNOWN";
  }
}
