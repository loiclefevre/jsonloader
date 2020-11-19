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
import java.io.*;
import java.util.*;

public class MyBSONDecoder extends BasicBSONDecoder implements BSONCallback {

    protected static boolean DEBUG = false;
    protected final boolean outputOsonFormat;

    protected Object root;
    protected final LinkedList<BSONObject> stack = new LinkedList<>();
    protected final LinkedList<String> nameStack = new LinkedList<>();
    protected final LinkedList<Boolean> arrayStack = new LinkedList<>();

    protected final OracleJsonFactory factory = new OracleJsonFactory();
    // oson output
    protected final ByteArrayOutputStream out = new ByteArrayOutputStream();
    protected JsonGenerator gen;
    protected int bsonLength;
    protected String oid;

    public MyBSONDecoder(boolean outputOsonFormat) {
        super();
        this.outputOsonFormat = outputOsonFormat;
    }

    @Override
    public BSONObject readObject(InputStream in) {
        throw new UnsupportedOperationException("readObject");
    }

    @Override
    public BSONObject readObject(byte[] bytes) {
        reset();
        bsonLength = bytes.length;
        this.decode(bytes, this);
        return (BSONObject) get();
    }

    public byte[] getOSONData() {
        return out.toByteArray();
    }

    public int getBsonLength() {
        return bsonLength;
    }

    public String getOid() {
        return oid;
    }

    public BSONObject create() {
        return new BasicBSONObject();
    }

    public BSONObject create(boolean array, List<String> path) {
        return array ? this.createList() : this.create();
    }

    protected BSONObject createList() {
        return new BasicBSONList();
    }

    @Override
    public void objectStart() {
        if (this.stack.size() > 0) {
            throw new IllegalStateException("Illegal object beginning in current context.");
        } else {
            if (DEBUG) System.out.println(stacker() + "objectStart");
            this.root = this.create(false, (List) null);
            this.stack.add((BSONObject) this.root);
            this.arrayStack.addLast(false);
            OracleJsonGenerator ogen = outputOsonFormat ? factory.createJsonBinaryGenerator(out) : factory.createJsonTextGenerator(out);
            gen = ogen.wrap(JsonGenerator.class);
            gen.writeStartObject();
        }
    }

    protected boolean isInsideArray() {
        return this.arrayStack.getLast();
    }

    protected boolean isPartOfAnArrayPath() {
        return arrayStack.contains(true);
    }

    @Override
    public void objectStart(String name) {
        this.nameStack.addLast(name);
        BSONObject o = this.create(false, this.nameStack);
        if (isInsideArray()) {
            if (DEBUG) System.out.println(stacker() + "objectStart (in array): " + name);
            this.stack.getLast().put(name, o);
            this.stack.addLast(o);
            this.arrayStack.addLast(false);
            gen.writeStartObject();
        } else {
            if (DEBUG) System.out.println(stacker() + "objectStart: " + name);
            this.stack.getLast().put(name, o);
            this.stack.addLast(o);
            this.arrayStack.addLast(false);
            gen.writeStartObject(name);
        }
    }

    protected String stacker() {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < this.stack.size(); i++) {
            s.append("\t");
        }
        return s.toString();
    }


    @Override
    public Object objectDone() {
        if (DEBUG) System.out.println(stacker() + "objectDone");
        BSONObject o = this.stack.removeLast();
        this.arrayStack.removeLast();
        gen.writeEnd();

        if (this.nameStack.size() > 0) {
            this.nameStack.removeLast();
        } else if (this.stack.size() > 0) {
            throw new IllegalStateException("Illegal object end in current context.");
        } else {
            if (DEBUG) System.out.println("OSON CLOSE");
            gen.close();
        }

        return o;
    }

    @Override
    public void reset() {
        this.root = null;
        this.stack.clear();
        this.nameStack.clear();
        this.arrayStack.clear();
        if (DEBUG) System.out.println("reset");
        out.reset();
        oid = null;
    }

    @Override
    public Object get() {
        return this.root;
    }

    @Override
    public BSONCallback createBSONCallback() {
        return null;
    }

    @Override
    public void arrayStart() {
        this.root = this.create(true, (List) null);
        if (DEBUG) System.out.println(stacker() + "arrayStart");
        this.stack.add((BSONObject) this.root);
    }

    @Override
    public void arrayStart(String name) {
        final boolean insidearray = isInsideArray();

        this.nameStack.addLast(name);
        BSONObject o = this.create(true, this.nameStack);
        if (DEBUG) System.out.println(stacker() + "arrayStart: " + name);
        this.stack.getLast().put(name, o);
        this.stack.addLast(o);
        this.arrayStack.addLast(true);

        //System.out.println("start array: "+name);

        if (insidearray)
            gen.writeStartArray();
        else
            gen.writeStartArray(name);
    }

    private boolean parentIsAnArray() {
        return this.arrayStack.get(this.arrayStack.size()-2);
    }

    @Override
    public Object arrayDone() {
        if (DEBUG) System.out.println("arrayDone -> using objectDone");
        Object o = this.objectDone();
        return o;
    }

    @Override
    public void gotNull(String name) {
        if (isInsideArray())
            gen.writeNull();
        else
            gen.writeNull(name);
    }

    @Override
    public void gotBoolean(final String name, final boolean value) {
        if (isInsideArray())
            gen.write(value);
        else
            gen.write(name, value);
    }

    @Override
    public void gotDouble(final String name, final double value) {
        if (isInsideArray())
            gen.write(value);
        else {
            gen.write(name, value);
        }
    }

    @Override
    public void gotInt(final String name, final int value) {
        if (isInsideArray())
            gen.write(value);
        else {
            gen.write(name, value);
        }
    }

    @Override
    public void gotLong(final String name, final long value) {
        if (isInsideArray())
            gen.write(value);
        else {
            gen.write(name, value);
        }
    }

    @Override
    public void gotDecimal128(final String name, final Decimal128 value) {
        if (isInsideArray())
            gen.write(value.bigDecimalValue());
        else {
            gen.write(name, value.bigDecimalValue());
        }
    }

    @Override
    public void gotDate(final String name, final long millis) {
        if (isInsideArray())
            gen.write(new Date(millis).toInstant().toString());
        else
            gen.write(name, new Date(millis).toInstant().toString());
    }

    @Override
    public void gotString(final String name, final String value) {
        if (isInsideArray())
            gen.write(value);
        else {
            gen.write(name, value);
        }
    }

    @Override
    public void gotSymbol(final String name, final String value) {
        if (isInsideArray())
            gen.write(value);
        else
            gen.write(name, value);
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
        }
    }

    @Override
    public void gotBinary(final String name, final byte type, final byte[] data) {
        if (isInsideArray())
            gen.write(hexa(data));
        else
            gen.write(name, hexa(data));
    }

    private static String hexa(final byte[] data) {
        final char[] result = new char[data.length * 2];

        int j = 0;
        for (int i = 0; i < data.length; i++) {
            final int x = data[i];
            int k = (x >> 4) & 0xF;
            result[j++] = (char) (k < 10 ? '0' + k : 'A' + k - 10);
            k = x & 0xF;
            result[j++] = (char) (k < 10 ? '0' + k : 'A' + k - 10);
        }

        return new String(result);
    }

    @Override
    public void gotUUID(final String name, final long part1, final long part2) {
        final UUID uuid = new UUID(part1, part2);
        if (isInsideArray())
            gen.write(uuid.toString());
        else
            gen.write(name, uuid.toString());
    }

    @Override
    public void gotCode(String name, String code) {
        throw new UnsupportedOperationException("Code");
    }

    @Override
    public void gotCodeWScope(String name, String code, Object scope) {
        throw new UnsupportedOperationException("CodeWScope");
    }

    @Override
    public void gotUndefined(String name) {
        throw new UnsupportedOperationException("Undefined");
    }

    @Override
    public void gotMinKey(String name) {
        throw new UnsupportedOperationException("MinKey");
    }

    @Override
    public void gotMaxKey(String name) {
        throw new UnsupportedOperationException("MaxKey");
    }

    @Override
    public void gotDBRef(String name, String namespace, ObjectId id) {
        throw new UnsupportedOperationException("DBRef");
    }

    @Override
    @Deprecated
    public void gotBinaryArray(String s, byte[] bytes) {
        throw new UnsupportedOperationException("BinaryArray");
    }

    @Override
    public void gotRegex(String name, String pattern, String flags) {
        throw new UnsupportedOperationException("Regex");
    }

    @Override
    public void gotTimestamp(String name, int time, int increment) {
        throw new UnsupportedOperationException("Timestamp");
    }

    protected void _put(String name, Object value) {
        this.cur().put(name, value);
    }

    protected BSONObject cur() {
        return this.stack.getLast();
    }


    protected String curName() {
        return this.nameStack.peekLast();
    }

    protected void setRoot(Object root) {
        this.root = root;
    }

    protected boolean isStackEmpty() {
        return this.stack.size() < 1;
    }


/*
    public static void main(String[] args) throws Throwable {
        //InputStream in = new FileInputStream(new File("test.bson"));
        //InputStream in = new FileInputStream(new File("autoedit_countdown_tag.bson"));
        InputStream in = new FileInputStream(new File("objectlabs-system.admin.collections.bson"));

        final MyBSONDecoder decoder = new MyBSONDecoder(true);
        MyBSONDecoder.DEBUG = true;

        while(true) {
            byte[] data = readNextBSONRawData(in);
            final BSONObject obj = decoder.readObject(data);
            System.out.println(obj.get("id"));
        }
    }


    private final static byte[] bsonDataSize = new byte[4];

    private static byte[] readNextBSONRawData(InputStream input) throws IOException {
        int readBytes = input.read(bsonDataSize, 0, 4);
        if (readBytes != 4) throw new EOFException();

        final int bsonSize = (bsonDataSize[0] & 0xff) |
                ((bsonDataSize[1] & 0xff) << 8) |
                ((bsonDataSize[2] & 0xff) << 16) |
                ((bsonDataSize[3] & 0xff) << 24);

        //System.out.println("bsonSize = "+bsonSize);

        final byte[] rawData = new byte[bsonSize];

        System.arraycopy(bsonDataSize, 0, rawData, 0, 4);

        for (int i = bsonSize - 4, off = 4; i > 0; off += readBytes) {
            readBytes = input.read(rawData, off, i);
            if (readBytes < 0) {
                throw new EOFException();
            }

            i -= readBytes;
        }

        return rawData;
    }
*/
}
