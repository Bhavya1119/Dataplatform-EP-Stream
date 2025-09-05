package com.snapdeal.dp.Streaming.databricks.Schema.Utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.snapdeal.dp.Streaming.databricks.Exception.InvalidFieldTypeException;
import com.snapdeal.dp.Streaming.databricks.Schema.Fields.FieldType;
import com.snapdeal.dp.Streaming.databricks.Schema.SQLSchema;
import com.snapdeal.dp.Streaming.databricks.local.Entity.DBEvent;
import org.apache.avro.Schema;
import org.apache.spark.sql.avro.SchemaConverters;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.avro.SchemaConverters.toSqlType;


/**
 * Created by Bhavya Joshi
 * on 3/01/2025
 */


public class DatabricksSchemaConverter {

    public static final Set<String> STRICT_CASE = new HashSet<>(Arrays.asList("dpHour","dpDay","dpMonth","dpYear"));

    public static class Field
    {
        private final String columnName ;
        private final String dataType ;

        public Field(String columnName, String dataType){
            this.columnName = columnName;
            this.dataType = dataType;
        }

        @Override
        public String toString() {
            return String.format("Field(%s, %s)", columnName, dataType);
        }

        public String getColumnName() {
            return columnName;
        }

        public String getDataType() {
            return dataType;
        }
    }

    /**
     * Method that takes in an object of DBEvent , parses the schema
     * converts into Field(columnName, dataType)
     * @param databricksEvent
     * @return
     * returns a SQLSchema object
     */
    public SQLSchema convertToSQL(DBEvent databricksEvent){

        String schema = databricksEvent.getSchema();
        String eventKey = databricksEvent.getEventKey();

        Schema.Parser parser = new Schema.Parser();
        Schema parsedSchema = parser.parse(schema);
        String dpJsonSchema = convert(parsedSchema);
        List<Field> columns = parseJsonSchema(dpJsonSchema);
        return new SQLSchema(eventKey,columns);

    }

    private String convert(Schema avroSchema){
        SchemaConverters.SchemaType dpAvroSchema = toSqlType(avroSchema);
        return dpAvroSchema.dataType().prettyJson();
    }


    private List<Field> parseJsonSchema(String jsonSchema){
        JsonObject rootNode = new JsonParser().parse(jsonSchema).getAsJsonObject();
        return parseParent(rootNode);
    }

    public List<Field> parseParent(JsonObject schema) {
        List<Field> fields = new ArrayList<>();
        if (schema.has("fields")) {
            JsonArray jsonFields = schema.get("fields").getAsJsonArray();
            if(jsonFields != null) {
                for (JsonElement field : jsonFields) {
                    fields.add(processField(field.getAsJsonObject()));
                }
            }
        }
        return fields;
    }

    private Field processField(JsonObject field) {
        String fieldName = field.get("name").getAsString();
        JsonElement typeElement = field.get("type");

        if (typeElement != null && typeElement.isJsonPrimitive())
        {
            if(STRICT_CASE.contains(fieldName)){
                //strictly cast dpYear,dpMonth,dpDay to Integer
                return new Field(fieldName,FieldType.INTEGER.getValue());
            }
            //in case of primitive , just create a Field object of columnName and primitive type
            return new Field(fieldName, typeElement.getAsString());
        } else if (typeElement != null && typeElement.isJsonObject())
        {
            //in case of nested jsonObject inside the object , parse the complexType recursively
            JsonObject typeObj = typeElement.getAsJsonObject();
            String dataType = buildComplexDataType(typeObj);
            return new Field(fieldName, dataType);
        }

        /**
         *  this case will never happen as soon as data strictly adhere to avro Schema rules
         *  if not then put unknown so that we can filter out it in future  processing
          */
        return new Field(fieldName, "unknown");
    }

    private String buildComplexDataType(JsonObject rootObj) {
        if (!rootObj.has("type")) {
            return "unknown";
        }

        String type = rootObj.get("type").getAsString();
        FieldType fieldType = FieldType.fromString(type);
        switch (fieldType) {
            case MAP    : return buildMapType(rootObj);
            case ARRAY  : return buildArrayType(rootObj);
            case STRUCT : return buildStructType(rootObj);
            default:
                boolean isValidType = false;
                for (FieldType validType : FieldType.values()) {
                    if (validType.getValue().equals(type)) {
                        isValidType = true;
                        break;
                    }
                }
                // throw exception if fieldType is not in one of those defined in FieldType ENUM.
                if (!isValidType) {
                    throw new InvalidFieldTypeException(
                            String.format("Invalid field type '%s'. Field type must be one of: %s", type,
                                    Arrays.stream(FieldType.values()).map(FieldType::getValue)
                                            .collect(Collectors.joining(", "))));
                }
                return type;
        }
    }

    private String buildMapType(JsonObject mapObj) {
        String keyType = mapObj.get("keyType").getAsString();
        JsonElement valueTypeElement = mapObj.get("valueType");
        String valueType;

        if (valueTypeElement.isJsonObject()) {
            valueType = buildComplexDataType(valueTypeElement.getAsJsonObject());
        } else {
            valueType = valueTypeElement.getAsString();
        }


        return String.format("Map<%s,%s>", keyType, valueType);
    }

    private String buildArrayType(JsonObject arrayObj) {
        JsonElement elementTypeNode = arrayObj.get("elementType");
        String elementType;

        if (elementTypeNode.isJsonObject()) {
            elementType = buildComplexDataType(elementTypeNode.getAsJsonObject());
        } else {
            elementType = elementTypeNode.getAsString();
        }

        return String.format("Array<%s>", elementType);
    }

    private String buildStructType(JsonObject structObj) {
        if (!structObj.has("fields")) {
            return "Struct";
        }

        List<String> fieldStrings = new ArrayList<>();
        JsonArray fields = structObj.get("fields").getAsJsonArray();

        for (JsonElement fieldElement : fields) {
            JsonObject fieldObj = fieldElement.getAsJsonObject();
            String fieldName = fieldObj.get("name").getAsString();
            JsonElement fieldType = fieldObj.get("type");

            String typeStr;
            if (fieldType.isJsonObject()) {
                typeStr = buildComplexDataType(fieldType.getAsJsonObject());
            } else {
                typeStr = fieldType.getAsString();
            }

            fieldStrings.add(String.format("%s: %s", fieldName, typeStr));
        }

        return String.format("Struct<%s>", String.join(", ", fieldStrings));
    }

}

