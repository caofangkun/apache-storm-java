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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.storm.daemon.worker.executor.grouping.GroupingType;
import org.apache.storm.thrift.Thrift;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.generated.Grouping;
import backtype.storm.generated.Grouping._Fields;
import backtype.storm.generated.NullStruct;

public class ThriftTest extends TestCase {
  private Grouping grouping;

  @BeforeClass
  public void setUp() {
    _Fields setField = Grouping._Fields.FIELDS;
    List value = new ArrayList();
    value.add("str");
    value.add(3);
    grouping = new Grouping(setField, value);
  }

  @Test
  public void testGroupingType() {
    GroupingType type = Thrift.groupingType(grouping);
    Assert.assertEquals(GroupingType.fields, type);
  }

  @Test
  public void testFieldGrouping() {
    List<String> fields = Thrift.fieldGrouping(grouping);
    Assert.assertEquals(2, fields.size());
    Assert.assertTrue(fields.contains("str"));
    Assert.assertTrue(fields.contains(3));
  }

  @Test
  public void testMkGrouping() {
    Grouping grouping = Thrift.mkAllGrouping();
    Assert.assertEquals(new NullStruct(), grouping.get_all());
    grouping = Thrift.mkNoneGrouping();
    Assert.assertEquals(new NullStruct(), grouping.get_none());
    grouping = Thrift.mkDirectGrouping();
    Assert.assertEquals(new NullStruct(), grouping.get_direct());
    List<String> fields = new ArrayList<String>();
    fields.add("test");
    fields.add("test2");
    grouping = Thrift.mkFieldsGrouping(fields);
    Assert.assertEquals(fields, grouping.get_fields());
    grouping = Thrift.mkGlobalGrouping();
    Assert.assertEquals(new ArrayList<String>(), grouping.get_fields());
    grouping = Thrift.mkLocalOrShuffleGrouping();
    Assert.assertEquals(new NullStruct(), grouping.get_local_or_shuffle());
  }

  @Test
  public void testIsGlobalGrouping() {
    Grouping grouping = Thrift.mkAllGrouping();
    Assert.assertFalse(Thrift.isGlobalGrouping(grouping));
    grouping = Thrift.mkGlobalGrouping();
    Assert.assertTrue(Thrift.isGlobalGrouping(grouping));
  }
}
