package org.deadlock.id2212.overlay.messages;

import java.util.UUID;

public class PeerId {
  public UUID uuid;
  public int listeningPort;

  public PeerId(final UUID uuid, final int listeningPort){
    this.uuid = uuid;
    this.listeningPort = listeningPort;
  }

  public PeerId() {}
}
