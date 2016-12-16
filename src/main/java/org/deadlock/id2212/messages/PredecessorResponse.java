package org.deadlock.id2212.messages;

import java.net.InetSocketAddress;
import java.util.UUID;

public class PredecessorResponse {
  public Integer predecessor;
  public InetSocketAddress address;

  public PredecessorResponse() {}
  public PredecessorResponse(final Integer predecessor, final InetSocketAddress address) {
    this.predecessor = predecessor;
    this.address = address;
  }
}
