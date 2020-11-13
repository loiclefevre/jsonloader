package com.oracle.jsonloader.command.mongodbbsontoajdosonloading;

import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonGenerator;
import org.bson.BSONCallback;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import javax.json.stream.JsonGenerator;
import javax.xml.stream.events.Characters;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.*;

// TODO: push vs pull?
public class MyBSONDecoderWithMetadata extends MyBSONDecoder {
    private final Map<String,Integer> maxLengths = new HashMap<>();
    private final Map<String,Set<String>> fieldsDataTypes = new HashMap<>();
    private final Set<String> cantIndex = new HashSet<>();

    public MyBSONDecoderWithMetadata(boolean outputOsonFormat) {
        super(outputOsonFormat);
    }

    private final StringBuilder path = new StringBuilder();

    protected String getJSONPath(final String name) {
        path.setLength(0);
        for (int i = 0; i < this.nameStack.size(); i++) {
            final String partName = nameStack.get(i);

            if(Character.isDigit(partName.charAt(0))) {
                continue;
            }

            if (i > 0) {
                path.append('.');
            }
            path.append(partName);
        }

        if(name != null) {
            if(path.length()>0) {
                path.append('.');
            }
            path.append(name);
        }

        return path.toString();
    }

    @Override
    public void arrayStart(String name) {
        final boolean insidearray = isInsideArray();
        String path = null;

        if(!insidearray) {
             path = getJSONPath(name);
        }

        this.nameStack.addLast(name);
        BSONObject o = this.create(true, this.nameStack);
        if (DEBUG) System.out.println(stacker() + "arrayStart: " + name);
        this.stack.getLast().put(name, o);
        this.stack.addLast(o);
        this.arrayStack.addLast(true);

        //System.out.println("start array: "+name);

        if (insidearray)
            gen.writeStartArray();
        else {
            gen.writeStartArray(name);

            if(fieldsDataTypes.containsKey(path)) {
                fieldsDataTypes.get(path).add("array");
            } else {
                final Set<String> dataTypes = new HashSet<>();
                dataTypes.add("array");
                fieldsDataTypes.put(path, dataTypes);
            }
        }
    }

    @Override
    public void gotDouble(final String name, final double value) {
        if (isInsideArray()) {
            gen.write(value);
        }
        else {
            gen.write(name, value);

            final String path = getJSONPath(name);

            if(isPartOfAnArrayPath()) {
                cantIndex.add(path);
            }

            if(fieldsDataTypes.containsKey(path)) {
                fieldsDataTypes.get(path).add("number");
            } else {
                final Set<String> dataTypes = new HashSet<>();
                dataTypes.add("number");
                fieldsDataTypes.put(path, dataTypes);
            }
        }
    }

    @Override
    public void gotInt(final String name, final int value) {
        if (isInsideArray())
            gen.write(value);
        else {
            gen.write(name, value);

            final String path = getJSONPath(name);

            if(isPartOfAnArrayPath()) {
                cantIndex.add(path);
            }

            if(fieldsDataTypes.containsKey(path)) {
                fieldsDataTypes.get(path).add("number");
            } else {
                final Set<String> dataTypes = new HashSet<>();
                dataTypes.add("number");
                fieldsDataTypes.put(path, dataTypes);
            }
        }
    }

    @Override
    public void gotLong(final String name, final long value) {
        if (isInsideArray())
            gen.write(value);
        else {
            gen.write(name, value);

            final String path = getJSONPath(name);

            if(isPartOfAnArrayPath()) {
                cantIndex.add(path);
            }

            if(fieldsDataTypes.containsKey(path)) {
                fieldsDataTypes.get(path).add("number");
            } else {
                final Set<String> dataTypes = new HashSet<>();
                dataTypes.add("number");
                fieldsDataTypes.put(path, dataTypes);
            }
        }
    }

    @Override
    public void gotDecimal128(final String name, final Decimal128 value) {
        if (isInsideArray())
            gen.write(value.bigDecimalValue());
        else {
            gen.write(name, value.bigDecimalValue());

            final String path = getJSONPath(name);

            if(isPartOfAnArrayPath()) {
                cantIndex.add(path);
            }

            if(fieldsDataTypes.containsKey(path)) {
                fieldsDataTypes.get(path).add("number");
            } else {
                final Set<String> dataTypes = new HashSet<>();
                dataTypes.add("number");
                fieldsDataTypes.put(path, dataTypes);
            }
        }
    }

    public Map<String, Integer> getMaxLengths() {
        return maxLengths;
    }

    public Map<String, Set<String>> getFieldsDataTypes() {
        return fieldsDataTypes;
    }

    public Set<String> getCantIndex() {
        return cantIndex;
    }

    @Override
    public void gotString(final String name, final String value) {
        //System.out.println(getJSONPath(name)+"."+name+": "+value.length());

        if (isInsideArray()) {
            gen.write(value);
        }
        else {
            gen.write(name, value);

            final String path = getJSONPath(name);

            //System.out.println(value.length()+" for "+path+" ["+value+"]");
            if(isPartOfAnArrayPath()) {
                cantIndex.add(path);
            }

            if(maxLengths.containsKey(path)) {
                maxLengths.put(path, Math.max(value.getBytes().length, maxLengths.get(path)));
            } else {
                maxLengths.put(path, value.getBytes().length);
            }

            if(fieldsDataTypes.containsKey(path)) {
                fieldsDataTypes.get(path).add("string");
            } else {
                final Set<String> dataTypes = new HashSet<>();
                dataTypes.add("string");
                fieldsDataTypes.put(path, dataTypes);
            }
        }
    }

    @Override
    public void gotBoolean(final String name, final boolean value) {
        if (isInsideArray())
            gen.write(value);
        else {
            gen.write(name, value);

            final String path = getJSONPath(name);

            //System.out.println(value.length()+" for "+path+" ["+value+"]");

            if(maxLengths.containsKey(path)) {
                maxLengths.put(path, 5); // "false"
            } else {
                maxLengths.put(path, 5); // "false"
            }

            if(fieldsDataTypes.containsKey(path)) {
                fieldsDataTypes.get(path).add("string");
            } else {
                final Set<String> dataTypes = new HashSet<>();
                dataTypes.add("string");
                fieldsDataTypes.put(path, dataTypes);
            }
        }
    }

    @Override
    public void gotObjectId(final String name, final ObjectId id) {
        if (oid == null && this.stack.size() == 1 && "_id".equals(name)) {
            oid = id.toString();
        }

        if (isInsideArray())
            gen.write(id.toString());
        else {
            gen.write(name, id.toString());

            final String path = getJSONPath(name);
            maxLengths.put(path, 24);
        }
    }
}
