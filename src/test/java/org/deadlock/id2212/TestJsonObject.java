package org.deadlock.id2212;

public class TestJsonObject {
  public int id;
  public String text;

  public TestJsonObject(int id, String text) {
    this.id = id;
    this.text = text;
  }

  public TestJsonObject() {
  }

  @Override
  public boolean equals(Object otherUnknown) {
    final TestJsonObject other = (TestJsonObject) otherUnknown;
    return id == other.id && text.equals(other.text);
  }
}
