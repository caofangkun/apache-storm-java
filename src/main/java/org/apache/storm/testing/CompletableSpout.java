package org.apache.storm.testing;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

public class CompletableSpout implements IRichSpout {

  private static final long serialVersionUID = 1L;

  @Override
  public void ack(Object arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void activate() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void deactivate() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void fail(Object arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void nextTuple() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

  public void startup() {
    // TODO Auto-generated method stub
    
  }

  public void cleanup() {
    // TODO Auto-generated method stub
    
  }

}
