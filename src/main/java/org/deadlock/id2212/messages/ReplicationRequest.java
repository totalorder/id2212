package org.deadlock.id2212.messages;

import java.util.Map;

public class ReplicationRequest {
  public Map<Integer, String> store;

  public ReplicationRequest() {}
  public ReplicationRequest(final Map<Integer, String> store) {
    this.store = store;
  }
}
