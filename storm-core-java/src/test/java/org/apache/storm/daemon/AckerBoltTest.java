package org.apache.storm.daemon;

import junit.framework.TestCase;

import org.apache.storm.daemon.acker.AckerBolt;
import org.apache.storm.daemon.worker.executor.bolt.BoltOutputCollector;
import org.junit.Test;
import org.mockito.Mockito;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.TupleImpl;

public class AckerBoltTest extends TestCase {

  @Test
  public void testAckerBolt() {
    TupleImpl tuple = Mockito.mock(TupleImpl.class);
    Mockito.when(tuple.getValue(0)).thenReturn(1);
    Mockito.when(tuple.getValue(1)).thenReturn(2);
    Mockito.when(tuple.getValue(2)).thenReturn(3);
    Mockito.when(tuple.getSourceStreamId()).thenReturn(
        AckerBolt.ACKER_INIT_STREAM_ID);
    AckerBolt ackerBolt = new AckerBolt();
    BoltOutputCollector collector = Mockito.mock(BoltOutputCollector.class);
    ackerBolt.prepare(null, null, new OutputCollector(collector));
    ackerBolt.execute(tuple);
  }

}
