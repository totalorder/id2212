package org.deadlock.id2212.messages;

import java.net.InetSocketAddress;
import java.util.UUID;

public class SuccessorResponse {
  public int successor;
  public InetSocketAddress address;

  public SuccessorResponse() {}
  public SuccessorResponse(final int successor, final InetSocketAddress address) {
    this.successor = successor;
    this.address = address;
  }
}
