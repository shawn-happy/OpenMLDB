package com._4paradigm.openmldb.sdk;

import java.util.Arrays;

public enum TTLType {
  kAbsoluteTime,
  kRelativeTime,
  kLatestTime,
  kAbsAndLat,
  kAbsOrLat,
  ;

  public static TTLType toTTLType(String type) {
    return Arrays.stream(values()).filter(ttlType -> ttlType.name().equalsIgnoreCase(type))
            .findFirst().orElseThrow(
                    () -> new IllegalArgumentException(
                            String.format("The TTL Type Is Not Found: [%s]", type))
            );
  }
}
