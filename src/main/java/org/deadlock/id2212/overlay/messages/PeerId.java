package org.deadlock.id2212.overlay.messages;

import java.util.UUID;

public class PeerId {
  public int uuid;
  public int listeningPort;

  public PeerId(final int uuid, final int listeningPort){
    this.uuid = uuid;
    this.listeningPort = listeningPort;
  }

  public PeerId() {}
}
