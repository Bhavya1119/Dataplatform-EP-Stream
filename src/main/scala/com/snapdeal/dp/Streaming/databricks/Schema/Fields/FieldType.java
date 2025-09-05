package com.snapdeal.dp.Streaming.databricks.Schema.Fields;

public enum FieldType {

    STRING("string"),
    BOOLEAN("boolean"),
    INTEGER("integer"),
    FLOAT("float"),
    LONG("long"),
    DOUBLE("double"),
    DATETIME("datetime"),
    ENUM("enum"),
    OBJECT("object"),
    ARRAY("array"),
    MAP("map"),
    STRUCT("struct"),
    RECORD("record");

    private final String value;

    FieldType(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static FieldType fromString(String text) {
        for (FieldType type : FieldType.values()) {

            if (type.value.equalsIgnoreCase(text)) {
                return type;
            }
        }
        return STRING;
    }
}
