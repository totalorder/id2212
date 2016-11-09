package org.deadlock.id2212.messages;

import java.util.Set;

public class KnownPeers {
  public Set<PeerInfo> peerInfos;

  public KnownPeers(final Set<PeerInfo> peerInfos) {
    this.peerInfos = peerInfos;
  }

  public KnownPeers() {}


}
