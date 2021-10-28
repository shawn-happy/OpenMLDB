package com._4paradigm.openmldb.sdk;

import java.util.Arrays;

public enum DataType {
  kBool,
  kSmallInt,
  kInt,
  kBigInt,
  kFloat,
  kDouble,
  kDate,
  kTimestamp,
  kVarchar,
  kString,
  ;

  public static DataType toDataType(String type) {
    return Arrays.stream(values()).filter(dataType -> dataType.name().equalsIgnoreCase(type))
            .findFirst().orElseThrow(
                    () -> new IllegalArgumentException(
                            String.format("The Data Type Is Not Found: [%s]", type))
            );
  }

  public static String toSQLType(String type) {
    DataType dataType = toDataType(type);
    switch (dataType) {
      case kSmallInt:
        return "smallint";
      case kInt:
        return "int";
      case kBigInt:
        return "bigint";
      case kFloat:
        return "float";
      case kDouble:
        return "double";
      case kBool:
        return "bool";
      case kVarchar:
      case kString:
        return "string";
      case kTimestamp:
        return "timestamp";
      case kDate:
        return "date";
      default:
        throw new IllegalArgumentException(String.format("fesql can't get this type, [%s]", type));
    }
  }
}
