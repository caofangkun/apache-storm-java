package org.apache.storm.util;

import java.util.Map;

import backtype.storm.Config;

public class CoreConfig extends Config {

  private static final long serialVersionUID = 1L;

  public static final String STORM_VIRTUAL_REAL_PORTS =
      "storm.virtual.real.ports";
  public static final Object STORM_VIRTUAL_REAL_PORTS_SCHEMA = Map.class;

  public static final String STORM_REAL_VIRTUAL_PORTS =
      "storm.virtual.real.ports";
  public static final Object STORM_REAL_VIRTUAL_PORTS_SCHEMA = Map.class;

}
