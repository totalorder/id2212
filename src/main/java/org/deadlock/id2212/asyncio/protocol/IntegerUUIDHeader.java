package org.deadlock.id2212.asyncio.protocol;

import java.util.UUID;

public class IntegerUUIDHeader {
  public final int integer;
  public final UUID uuid;

  public IntegerUUIDHeader(final int integer, final UUID uuid) {
    this.integer = integer;
    this.uuid = uuid;
  }
}
