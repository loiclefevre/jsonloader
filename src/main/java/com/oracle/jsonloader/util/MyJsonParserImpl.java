package com.oracle.jsonloader.util;

import oracle.jdbc.driver.json.BufferPoolImpl;
import oracle.jdbc.driver.json.parser.JsonParserImpl;
import oracle.jdbc.driver.json.tree.OracleJsonTimestampImpl;
import oracle.sql.json.OracleJsonValue;

import java.io.InputStream;
import java.io.Reader;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

public class MyJsonParserImpl extends JsonParserImpl {
	public MyJsonParserImpl(Reader reader, BufferPoolImpl bufferPool) {
		super(reader, bufferPool);
	}

	public MyJsonParserImpl(InputStream inputStream, BufferPoolImpl bufferPool) {
		super(inputStream, bufferPool);
	}

	boolean lastStringIsTimeStamp = false;

	public Event next() {
		Event event = super.next();
		if (event == Event.VALUE_STRING) {
			final String value = super.getValue().toString();
			//System.out.println(value + " | " + value.length());
			// 2015-01-12T16:00:00Z
			if (value.length() == 22 && (value.charAt(1) == '2' || value.charAt(1) == '1')
					&& value.charAt(11) == 'T' && value.charAt(14) == ':' && value.charAt(20) == 'Z') {
				event = Event.VALUE_TIMESTAMP;
				lastStringIsTimeStamp = true;
			}
			else {
				lastStringIsTimeStamp = false;
			}
		}
		else {
			lastStringIsTimeStamp = false;
		}
		//System.out.println(event);
		return event;
	}

	public OracleJsonValue getValue() {
		OracleJsonValue value = super.getValue();
		if (lastStringIsTimeStamp) {
			value = new OracleJsonTimestampImpl(Instant.parse(value.toString().substring(1, 21)).atOffset(ZoneOffset.UTC).toLocalDateTime());
		}
		//System.out.println("getValue: " + value);
		return value;
	}
}
