package com.oracle.jsonloader.command.mongodbbsontoajdosonloading;

import oracle.sql.NUMBER;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonGenerator;
import org.bson.BSONCallback;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class MyBSONDecoder extends BasicBSONDecoder implements BSONCallback {

    private static final boolean DEBUG = false;
    private final boolean outputOsonFormat;

    private Object root;
    private final LinkedList<BSONObject> stack = new LinkedList<>();
    private final LinkedList<String> nameStack = new LinkedList<>();
    private final LinkedList<Boolean> arrayStack = new LinkedList<>();

    private final OracleJsonFactory factory = new OracleJsonFactory();
    // oson output
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private OracleJsonGenerator gen;
    private int bsonLength;
    private String oid;

    public MyBSONDecoder(boolean outputOsonFormat) {
        super();
        this.outputOsonFormat = outputOsonFormat;
    }

    @Override
    public BSONObject readObject(InputStream in) {
        throw new UnsupportedOperationException();
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
            gen = outputOsonFormat ? factory.createJsonBinaryGenerator(out) :
                    factory.createJsonTextGenerator(out);
            gen.writeStartObject();
        }
    }

    private boolean isInsideArray() {
        return this.arrayStack.getLast();
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

    private String stacker() {
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
        this.nameStack.addLast(name);
        BSONObject o = this.create(true, this.nameStack);
        if (DEBUG) System.out.println(stacker() + "arrayStart: " + name);
        this.stack.getLast().put(name, o);
        this.stack.addLast(o);
        this.arrayStack.addLast(true);

        gen.writeStartArray(name);
    }

    @Override
    public Object arrayDone() {
        if (DEBUG) System.out.println("arrayDone -> using objectDone");
        Object o = this.objectDone();
        //this.arrayStack.removeLast();
        return o;
    }

    @Override
    public void gotNull(String name) {
        //this.cur().put(name, (Object) null);
        if (isInsideArray())
            gen.writeNull();
        else
            gen.writeNull(name);
    }

    public void gotUndefined(String name) {
        System.out.println("Warning: undefined");
    }

    public void gotMinKey(String name) {
        //this.cur().put(name, new MinKey());
        System.out.println("Warning: MinKey");
    }

    public void gotMaxKey(String name) {
        //this.cur().put(name, new MaxKey());
        System.out.println("Warning: MaxKey");
    }

    public void gotBoolean(String name, boolean value) {
        //this._put(name, value);
        if (isInsideArray())
            gen.write(value);
        else
            gen.write(name, value);
    }

    public void gotDouble(String name, double value) {
        //this._put(name, value);
        try {
            if (isInsideArray())
                gen.write(factory.createValue(new NUMBER(value)));
            else
                gen.write(name, factory.createValue(new NUMBER(value)));
        } catch (SQLException sqle) {
            throw new RuntimeException(sqle);
        }
    }

    public void gotInt(String name, int value) {
        //this._put(name, value);
        if (isInsideArray())
            gen.write(factory.createValue(new NUMBER(value)));
        else
            gen.write(name, factory.createValue(new NUMBER(value)));
    }

    public void gotLong(String name, long value) {
        //this._put(name, value);
        //System.out.println(isInsideArray() + " gotLong " + name + "=" + value);
        if (isInsideArray())
            gen.write(factory.createValue(new NUMBER(value)));
        else
            gen.write(name, factory.createValue(new NUMBER(value)));
    }

    public void gotDecimal128(String name, Decimal128 value) {
        //this._put(name, value);
        try {
            if (isInsideArray())
                gen.write(factory.createValue(new NUMBER(value.bigDecimalValue())));
            else
                gen.write(name, factory.createValue(new NUMBER(value.bigDecimalValue())));
        } catch (SQLException sqle) {
            throw new RuntimeException(sqle);
        }
    }

    public void gotDate(String name, long millis) {
        //this._put(name, new Date(millis));
        if (isInsideArray())
            gen.write(new Date(millis).toInstant());
        else
            gen.write(name, new Date(millis).toInstant());
    }

    public void gotRegex(String name, String pattern, String flags) {
        // TODO this._put(name, Pattern.compile(pattern, BSON.regexFlags(flags)));
        System.out.println("Warning: regex");
    }

    public void gotString(String name, String value) {
        //this._put(name, value);
//        System.out.println("gotString: " + name + ", " + value);
        if (isInsideArray())
            gen.write(value);
        else
            gen.write(name, value);
    }

    public void gotSymbol(String name, String value) {
        //this._put(name, value);
        //System.out.println("Warning: symbol");
        if (isInsideArray())
            gen.write(value);
        else
            gen.write(name, value);
    }

    public void gotTimestamp(String name, int time, int increment) {
        //this._put(name, new BSONTimestamp(time, increment));
        //BSONTimestamp timestamp  = new BSONTimestamp(time, increment);
        // Rem: for internal use of MongoDB only
        //gen.write(name, new Date((long) time * 1000L).toInstant());
        System.out.println("Warning: timestamp");
    }

    public void gotObjectId(String name, ObjectId id) {
        //this._put(name, id);
        //System.out.println("ObjectId: " + id.toString());
        if (oid == null && this.stack.size() == 1 && "_id".equals(name)) {
            oid = id.toString();
        }

        if (isInsideArray())
            gen.write(id.toString());
        else
            gen.write(name, id.toString());
    }

    public void gotDBRef(String name, String namespace, ObjectId id) {
//        this._put(name, (new BasicBSONObject("$ns", namespace)).append("$id", id));
        System.out.println("Warning: DBRef");
    }

    @Override
    @Deprecated
    public void gotBinaryArray(String s, byte[] bytes) {
        System.out.println("Warning: binary array");
    }

    public void gotBinary(String name, byte type, byte[] data) {
        /*if (type != 0 && type != 2) {
            this._put(name, new Binary(type, data));
        } else {
            this._put(name, data);
        }*/

        System.out.println("Warning: binary");
    }

    public void gotUUID(String name, long part1, long part2) {
        //this._put(name, new UUID(part1, part2));
        final UUID uuid = new UUID(part1, part2);
        //System.out.println("Warning: UUID");
        if (isInsideArray())
            gen.write(uuid.toString());
        else
            gen.write(name, uuid.toString());
    }

    public void gotCode(String name, String code) {
        //this._put(name, new Code(code));
        //System.out.println("Warning: code");
        if (isInsideArray())
            gen.write(code);
        else
            gen.write(name, code);
    }

    public void gotCodeWScope(String name, String code, Object scope) {
        //this._put(name, new CodeWScope(code, (BSONObject) scope));
        System.out.println("Warning: CodeWScope");
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
}
