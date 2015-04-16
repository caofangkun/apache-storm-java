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
package org.apache.storm.counter;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public enum TaskCounter {
  SPOUT_EMIT_CNT, SPOUT_EMIT_TPS, SPOUT_TRANSFER_FAILED, SPOUT_EMIT_DIRECT_CNT, SPOUT_EMIT_DIRECT_TPS, BOLT_EMIT_CNT, BOLT_EMIT_TPS, BOLT_TRANSFER_FAILED, BOLT_EMIT_DIRECT_CNT, BOLT_EMIT_DIRECT_TPS, BOLT_ACK_CNT, BOLT_ACK_TPS
}
