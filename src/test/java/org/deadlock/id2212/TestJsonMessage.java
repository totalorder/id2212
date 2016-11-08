package org.deadlock.id2212;

public class TestJsonMessage {
  public int id;
  public String text;

  public TestJsonMessage(int id, String text) {
    this.id = id;
    this.text = text;
  }

  public TestJsonMessage() {
  }

  @Override
  public boolean equals(Object otherUnknown) {
    final TestJsonMessage other = (TestJsonMessage) otherUnknown;
    return id == other.id && text.equals(other.text);
  }
}
