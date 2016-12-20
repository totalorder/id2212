package org.deadlock.id2212.overlay.messages;

import java.net.InetSocketAddress;
import java.util.UUID;

public class PeerId {
  public int uuid;
  public InetSocketAddress listeningAddress;

  public PeerId(final int uuid, final InetSocketAddress listeningAddress){
    this.uuid = uuid;
    this.listeningAddress = listeningAddress;
  }

  public PeerId() {}
}
