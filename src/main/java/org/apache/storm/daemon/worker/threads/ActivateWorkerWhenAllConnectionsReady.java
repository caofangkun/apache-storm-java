package org.apache.storm.daemon.worker.threads;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.ConnectionWithStatus;
import backtype.storm.messaging.ConnectionWithStatus.Status;
import backtype.storm.messaging.IConnection;
import backtype.storm.scheduler.WorkerSlot;

/**
 * we will wait all connections to be ready and then activate the spout/bolt
 * when the worker bootup
 *
 */

@ClojureClass(className = "backtype.storm.daemon.worker#activate-worker-when-all-connections-ready")
public class ActivateWorkerWhenAllConnectionsReady extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory
      .getLogger(ActivateWorkerWhenAllConnectionsReady.class);

  private WorkerData worker;

  public ActivateWorkerWhenAllConnectionsReady(WorkerData worker) {
    this.worker = worker;
  }

  @Override
  public void run() {
    if (allConnectionsReady(worker)) {
      LOG.info("All connections are ready for worker "
          + worker.getAssignmentId() + ":" + worker.getPort() + " with id "
          + worker.getWorkerId());
      worker.getWorkerActiveFlag().set(true);
    }
  }

  /**
   * all connections are ready
   * 
   * @return
   */
  @ClojureClass(className = "backtype.storm.daemon.worker#all-connections-ready")
  public boolean allConnectionsReady(WorkerData worker) {
    ConcurrentHashMap<WorkerSlot, IConnection> connections =
        worker.getCachedNodeportToSocket();
    for (IConnection connection : connections.values()) {
      if (!isConnectionReady(connection)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Check whether this messaging connection is ready to send data
   * 
   * @param connection
   * @return
   */
  @ClojureClass(className = "backtype.storm.daemon.worker#is-connection-ready")
  public boolean isConnectionReady(IConnection connection) {
    if (connection instanceof ConnectionWithStatus) {
      Status status = ((ConnectionWithStatus) connection).status();
      return status.equals(ConnectionWithStatus.Status.Ready);
    }
    return true;
  }
}
