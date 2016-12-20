package org.deadlock.id2212.messages;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

public class RingProbe {
  public List<Integer> nodes = new ArrayList<>();

  public RingProbe() {
  }

  public String toString() {
    return "RingProbe{" + Joiner.on(" -> ").join(nodes) + "}";
  }
}
