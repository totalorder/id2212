package org.deadlock.id2212.messages;

import java.net.InetSocketAddress;
import java.util.UUID;

public class PredecessorNotification {
  public int uuid;
  public InetSocketAddress address;

  public PredecessorNotification() {}
  public PredecessorNotification(final int uuid, final InetSocketAddress address) {
    this.uuid = uuid;
    this.address = address;
  }
}
