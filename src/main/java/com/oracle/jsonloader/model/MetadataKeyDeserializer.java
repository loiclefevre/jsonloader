package com.oracle.jsonloader.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.oracle.jsonloader.model.MetadataKey;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class MetadataKeyDeserializer extends StdDeserializer<MetadataKey> {

    public MetadataKeyDeserializer() {
        this(null);
    }

    public MetadataKeyDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public MetadataKey deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {

        JsonNode mKeyNode = jp.getCodec().readTree(jp);
        MetadataKey mKey = new MetadataKey();

        for (Iterator<Map.Entry<String, JsonNode>> it = mKeyNode.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> f = it.next();

            //System.out.println(f.getKey());

            if("_fts".equals(f.getKey()) && "text".equals(f.getValue().textValue())) {
                mKey.text = true;
                break;
            }

            for (Iterator<String> iter = f.getValue().fieldNames(); iter.hasNext(); ) {
                String valueField = iter.next();
                switch(valueField) {
                    case "$numberDouble":
                        mKey.addIndexColumn(f.getKey(),f.getValue().get(valueField).asDouble() == 1.0);
                        break;

                    case "$numberInt":
                        mKey.addIndexColumn(f.getKey(),f.getValue().get(valueField).asInt() == 1);
                        break;

                }
            }
        }


        return mKey;
    }
}
