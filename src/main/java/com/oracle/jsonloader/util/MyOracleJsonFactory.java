package com.oracle.jsonloader.util;

import oracle.jdbc.driver.json.BufferPoolImpl;
import oracle.jdbc.driver.json.OracleJsonExceptions;
import oracle.jdbc.driver.json.binary.*;
import oracle.jdbc.driver.json.parser.JsonParserImpl;
import oracle.jdbc.driver.json.tree.*;
import oracle.sql.*;
import oracle.sql.json.*;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;

public class MyOracleJsonFactory {
	private static final boolean DISABLE_GENERATOR_POOL = Boolean.getBoolean("oracle.sql.json.OracleJsonFactory.DISABLE_GENERATOR_POOL");
	private static final int DEFAULT_BUFFER_SIZE = 8192;
	private final OsonGeneratorImpl.OsonGeneratorStatePool generatorPool;
	private final BufferPoolImpl bufferPool;

	public MyOracleJsonFactory() {
		this.generatorPool = DISABLE_GENERATOR_POOL ? null : new OsonGeneratorImpl.OsonGeneratorStatePool();
		this.bufferPool = new BufferPoolImpl();
	}

	public OracleJsonParser createJsonBinaryParser(InputStream var1) throws OracleJsonException {
		byte[] var2 = readInputStream(var1);
		ByteBuffer var3 = ByteBuffer.wrap(var2);
		OsonContext var4 = new OsonContext(new OsonBuffer(var3));
		OsonParserImpl var5 = new OsonParserImpl(var4);
		var5.setCloseable(var1);
		return var5;
	}

	public OracleJsonParser createJsonTextParser(InputStream var1) throws OracleJsonException {
		return new MyJsonParserImpl(var1, this.bufferPool);
	}

	public OracleJsonParser createJsonTextParser(Reader var1) throws OracleJsonException {
		return new MyJsonParserImpl(var1, this.bufferPool);
	}

	public OracleJsonParser createJsonBinaryParser(ByteBuffer var1) throws OracleJsonException {
		OsonContext var2 = new OsonContext(new OsonBuffer(var1));
		return new OsonParserImpl(var2);
	}

	public OracleJsonValue createJsonBinaryValue(InputStream var1) throws OracleJsonException {
		byte[] var2 = readInputStream(var1);
		ByteBuffer var3 = ByteBuffer.wrap(var2);
		return this.createJsonBinaryValue(var3);
	}

	public OracleJsonValue createJsonTextValue(InputStream var1) throws OracleJsonException {
		OracleJsonParser var2 = this.createJsonTextParser(var1);
		Throwable var3 = null;

		OracleJsonValue var4;
		try {
			var2.next();
			var4 = var2.getValue();
		} catch (Throwable var13) {
			var3 = var13;
			throw var13;
		} finally {
			if (var2 != null) {
				if (var3 != null) {
					try {
						var2.close();
					} catch (Throwable var12) {
						var3.addSuppressed(var12);
					}
				} else {
					var2.close();
				}
			}

		}

		return var4;
	}

	public OracleJsonValue createJsonTextValue(Reader var1) throws OracleJsonException {
		OracleJsonParser var2 = this.createJsonTextParser(var1);
		Throwable var3 = null;

		OracleJsonValue var4;
		try {
			var2.next();
			var4 = var2.getValue();
		} catch (Throwable var13) {
			var3 = var13;
			throw var13;
		} finally {
			if (var2 != null) {
				if (var3 != null) {
					try {
						var2.close();
					} catch (Throwable var12) {
						var3.addSuppressed(var12);
					}
				} else {
					var2.close();
				}
			}

		}

		return var4;
	}

	public OracleJsonValue createJsonBinaryValue(ByteBuffer var1) throws OracleJsonException {
		OsonContext var2 = new OsonContext(new OsonBuffer(var1));
		int var3 = var2.getHeader().getTreeSegmentOffset();
		return (OracleJsonValue) OsonStructureImpl.getValueInternal(var3, var2.getFactory(), var2);
	}

	public final OracleJsonGenerator createJsonBinaryGenerator(OutputStream var1) {
		return new OsonGeneratorImpl(this.generatorPool, var1);
	}

	public OracleJsonGenerator createJsonTextGenerator(OutputStream var1) {
		return new JsonSerializerImpl(var1);
	}

	public OracleJsonGenerator createJsonTextGenerator(Writer var1) {
		return new JsonSerializerImpl(var1);
	}

	public OracleJsonObject createObject() {
		return new OracleJsonObjectImpl();
	}

	public OracleJsonArray createArray() {
		return new OracleJsonArrayImpl();
	}

	public OracleJsonObject createObject(OracleJsonObject var1) {
		return new OracleJsonObjectImpl(var1);
	}

	public OracleJsonArray createArray(OracleJsonArray var1) {
		return new OracleJsonArrayImpl(var1);
	}

	public OracleJsonString createString(String var1) {
		return new OracleJsonStringImpl(var1);
	}

	public OracleJsonDecimal createDecimal(BigDecimal var1) throws OracleJsonException {
		return new OracleJsonDecimalImpl(var1);
	}

	public OracleJsonDecimal createDecimal(int var1) {
		return new OracleJsonDecimalImpl(var1, OracleJsonDecimal.TargetType.INT);
	}

	public OracleJsonDecimal createDecimal(long var1) {
		return new OracleJsonDecimalImpl(var1, OracleJsonDecimal.TargetType.LONG);
	}

	public OracleJsonFloat createFloat(float var1) {
		return new OracleJsonFloatImpl(var1);
	}

	public OracleJsonDouble createDouble(double var1) {
		return new OracleJsonDoubleImpl(var1);
	}

	public OracleJsonBinary createBinary(byte[] var1) {
		return new OracleJsonBinaryImpl(var1, false);
	}

	public OracleJsonValue createBoolean(boolean var1) {
		return var1 ? OracleJsonValue.TRUE : OracleJsonValue.FALSE;
	}

	public OracleJsonValue createNull() {
		return OracleJsonValue.NULL;
	}

	public OracleJsonTimestamp createTimestamp(Instant var1) {
		return new OracleJsonTimestampImpl(var1);
	}

	public OracleJsonDate createDate(Instant var1) {
		return new OracleJsonDateImpl(var1);
	}

	public OracleJsonIntervalDS createIntervalDS(Duration var1) {
		return new OracleJsonIntervalDSImpl(var1);
	}

	public OracleJsonIntervalYM createIntervalYM(Period var1) {
		return new OracleJsonIntervalYMImpl(var1);
	}

	public OracleJsonValue createValue(Datum var1) {
		try {
			if (var1 instanceof CHAR) {
				return new OracleJsonStringImpl(var1.stringValue());
			} else if (var1 instanceof NUMBER) {
				return new OracleJsonDecimalImpl(var1.getBytes(), (OracleJsonDecimal.TargetType)null);
			} else if (var1 instanceof BINARY_DOUBLE) {
				return new OracleJsonDoubleImpl(var1.doubleValue());
			} else if (var1 instanceof BINARY_FLOAT) {
				return new OracleJsonFloatImpl(var1.floatValue());
			} else if (var1 instanceof RAW) {
				return new OracleJsonBinaryImpl(var1.getBytes(), false);
			} else if (var1 instanceof DATE) {
				return new OracleJsonDateImpl(var1.getBytes());
			} else if (var1 instanceof TIMESTAMP) {
				return new OracleJsonTimestampImpl(var1.getBytes());
			} else if (var1 instanceof INTERVALDS) {
				return new OracleJsonIntervalDSImpl(var1.getBytes());
			} else if (var1 instanceof INTERVALYM) {
				return new OracleJsonIntervalYMImpl(var1.getBytes());
			} else if (var1 instanceof OracleJsonDatum) {
				OracleJsonDatum var2 = (OracleJsonDatum)var1;
				return this.createJsonBinaryValue(ByteBuffer.wrap(var2.shareBytes()));
			} else {
				throw new UnsupportedOperationException();
			}
		} catch (SQLException var3) {
			throw new OracleJsonException(var3.getMessage(), var3);
		}
	}

	private static byte[] readInputStream(InputStream var0) throws OracleJsonException {
		try {
			byte[] var1 = new byte[DEFAULT_BUFFER_SIZE];
			ByteArrayOutputStream var3 = new ByteArrayOutputStream();

			int var2;
			while((var2 = var0.read(var1)) != -1) {
				var3.write(var1, 0, var2);
			}

			var0.close();
			return var3.toByteArray();
		} catch (IOException var4) {
			throw OracleJsonExceptions.IO.create(OracleJsonExceptions.ORACLE_FACTORY, var4, new Object[0]);
		}
	}
}
