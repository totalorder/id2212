package org.deadlock.id2212.messages;

public class SetKeyRequest {
  public int key;
  public String value;

  public SetKeyRequest() {}
  public SetKeyRequest(final int key, final String value) {
    this.key = key;
    this.value = value;
  }
}
