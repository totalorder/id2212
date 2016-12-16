package org.deadlock.id2212.messages;

import java.util.UUID;

public class SuccessorRequest {
  public int uuid;

  public SuccessorRequest() {}
  public SuccessorRequest(final int uuid) {
    this.uuid = uuid;
  }
}
