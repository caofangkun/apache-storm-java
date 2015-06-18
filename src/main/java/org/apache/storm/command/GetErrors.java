package org.apache.storm.command;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.GetInfoOptions;
import backtype.storm.generated.NumErrorsChoice;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.command.get-errors")
public class GetErrors {

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("    $STORM_HOME/bin/storm get-errors topology_name");
  }

  public static void main(String[] args) {
    if (args == null || args.length == 0) {
      System.out.println("Should privide TOPOLOGY_NAME at least!");
      printUsage();
      return;
    }
    GetInfoOptions opts = new GetInfoOptions();
    opts.set_num_err_choice(NumErrorsChoice.ONE);
    // TODO Auto-generated method stub

    String name = args[0];
    NimbusClient client = null;
    try {
      Map conf = Utils.readStormConfig();
      client = NimbusClient.getConfiguredClient(conf);
      ClusterSummary clusterInfo = client.getClient().getClusterInfo();
      String topoId = getTopologyId(name, clusterInfo);
      TopologyInfo topoInfo = null;
      if (topoId != null) {
        topoInfo = client.getClient().getTopologyInfoWithOpts(topoId, opts);
      }
      Map<String, String> info = new HashMap<String, String>();
      if (topoId == null || topoInfo == null) {
        // {"Failure" (str "No topologies running with name " name)}
        info.put("Failure", "No topologies running with name " + name);
      } else {
        String topologyName = topoInfo.get_name();
        Map<String, List<ErrorInfo>> topologyErrors = topoInfo.get_errors();

        info.put("Topology Name", topologyName);
        info.put("Comp-Errors",
            CoreUtil.to_json(getComponentErrors(topologyErrors)));
      }
      System.out.println(CoreUtil.to_json(info));
    } catch (Exception e) {
      System.out.println(CoreUtil.stringifyError(e));
      printUsage();
    } finally {
      if (client != null) {
        client.close();
      }
    }

  }

  @ClojureClass(className = "backtype.storm.command.get-errors#get-component-errors")
  private static Map<String, String> getComponentErrors(
      Map<String, List<ErrorInfo>> topologyErrors) {
    Map<String, String> componentError = new HashMap<String, String>();
    for (Map.Entry<String, List<ErrorInfo>> topologyError : topologyErrors
        .entrySet()) {
      String compName = topologyError.getKey();
      List<ErrorInfo> compErrors = topologyError.getValue();
      ErrorInfo lastestError = compErrors.get(0);
      if (lastestError != null) {
        componentError.put(compName, lastestError.get_error());
      }
    }
    return componentError;
  }

  @ClojureClass(className = "backtype.storm.command.get-errors#get-topology-id")
  private static String getTopologyId(String topologyName,
      ClusterSummary clusterInfo) {
    String topologyId = null;
    List<TopologySummary> topologies = clusterInfo.get_topologies();
    for (TopologySummary topology : topologies) {
      if (topology.get_name().equals(topologyName)) {
        topologyId = topology.get_id();
      }
    }
    return topologyId;
  }

}
