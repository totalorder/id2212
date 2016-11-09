package org.deadlock.id2212.messages;

import java.net.InetSocketAddress;
import java.util.UUID;

public class PeerInfo {
  public UUID uuid;
  public InetSocketAddress address;

  public PeerInfo(final UUID uuid, final InetSocketAddress address) {
    this.uuid = uuid;
    this.address = address;
  }

  public String toString() {
    return String.format("PeerInfo{uuid=%s,address=%s}", uuid, address);
  }

  public PeerInfo() {}

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PeerInfo peerInfo = (PeerInfo) o;

    return uuid.equals(peerInfo.uuid);

  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }
}
