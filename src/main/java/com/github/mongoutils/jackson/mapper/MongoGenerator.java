package com.github.mongoutils.jackson.mapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.base.GeneratorBase;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteResult;

public class MongoGenerator extends GeneratorBase {
    
    protected class BuilderContext {
        
        protected String name;
        protected BasicDBObject obj = new BasicDBObject();
        protected BuilderContext parent;
        
        public BuilderContext() {
        }
        
        public BuilderContext(BuilderContext parent) {
            this.parent = parent;
        }
        
        public BuilderContext endArray() {
            return parent;
        }
        
        public BuilderContext startArray() {
            ArrayBuilderContext ctx = new ArrayBuilderContext(this);
            if (name != null) {
                obj.append(name, ctx.objs);
            }
            return ctx;
        }
        
        public BuilderContext endObject() {
            return parent;
        }
        
        public BuilderContext startObject() {
            BuilderContext ctx = new BuilderContext(this);
            if (name != null) {
                obj.append(name, ctx.obj);
            }
            return ctx;
        }
        
        public void addValue(Object value) {
            if (name != null) {
                obj.append(name, value);
            }
        }
        
        @Override
        public String toString() {
            return getClass().getSimpleName() + " - " + (obj != null ? obj.toString() : null);
        }
        
    }
    
    protected class ArrayBuilderContext extends BuilderContext {
        
        protected List<Object> objs = new ArrayList<Object>();
        
        public ArrayBuilderContext(BuilderContext parent) {
            super(parent);
        }
        
        @Override
        public BuilderContext startArray() {
            ArrayBuilderContext ctx = new ArrayBuilderContext(this);
            objs.add(ctx.objs);
            return ctx;
        }
        
        @Override
        public BuilderContext startObject() {
            BuilderContext ctx = new BuilderContext(this);
            objs.add(ctx.obj);
            return ctx;
        }
        
        @Override
        public void addValue(Object value) {
            objs.add(value);
        }
        
    }
    
    protected DBCollection collection;
    protected WriteResult writeResult;
    protected BuilderContext rootCtx;
    protected BuilderContext currentCtx;
    protected Stack<Object> lastIds = new Stack<Object>();
    
    public MongoGenerator(DBCollection collection, ObjectCodec codec) {
        this(collection, codec, Feature.collectDefaults());
    }
    
    public MongoGenerator(DBCollection collection, ObjectCodec codec, int features) {
        super(features, codec);
        this.collection = collection;
        try {
            flush();
        } catch (IOException exception) {
        }
    }
    
    public DBCollection getCollection() {
        return collection;
    }
    
    public WriteResult getWriteResult() {
        return writeResult;
    }
    
    public Stack<Object> getLastIds() {
        return lastIds;
    }
    
    @Override
    public void flush() throws IOException {
        if (rootCtx != null) {
            writeResult = collection.save(rootCtx.obj);
            lastIds.push(rootCtx.obj.get("_id"));
        }
        rootCtx = null;
        currentCtx = null;
    }
    
    @Override
    protected void _releaseBuffers() {
    }
    
    @Override
    public void writeEndArray() throws IOException, JsonGenerationException {
        if (currentCtx != null) {
            currentCtx = currentCtx.endArray();
        }
    }
    
    @Override
    public void writeStartArray() throws IOException, JsonGenerationException {
        if (currentCtx != null) {
            currentCtx = currentCtx.startArray();
        } else {
            throw new RuntimeException("can't write array as root object");
        }
    }
    
    @Override
    public void writeEndObject() throws IOException, JsonGenerationException {
        if (currentCtx != null) {
            currentCtx = currentCtx.endObject();
        }
    }
    
    @Override
    public void writeStartObject() throws IOException, JsonGenerationException {
        if (currentCtx != null) {
            currentCtx = currentCtx.startObject();
        } else {
            rootCtx = new BuilderContext();
            currentCtx = rootCtx;
        }
    }
    
    @Override
    protected void _verifyValueWrite(String typeMsg) throws IOException, JsonGenerationException {
    }
    
    @Override
    public void writeFieldName(String name) throws IOException, JsonGenerationException {
        currentCtx.name = name;
    }
    
    @Override
    public void writeString(String text) throws IOException, JsonGenerationException {
        currentCtx.addValue(text);
    }
    
    @Override
    public void writeString(char[] text, int offset, int len) throws IOException, JsonGenerationException {
        currentCtx.addValue(new String(text, offset, len));
    }
    
    @Override
    public void writeRawUTF8String(byte[] text, int offset, int length) throws IOException, JsonGenerationException {
        currentCtx.addValue(new String(text, offset, length, "utf-8"));
    }
    
    @Override
    public void writeUTF8String(byte[] text, int offset, int length) throws IOException, JsonGenerationException {
        currentCtx.addValue(new String(text, offset, length, "utf-8"));
    }
    
    @Override
    public void writeRaw(String text) throws IOException, JsonGenerationException {
        currentCtx.addValue(text);
    }
    
    @Override
    public void writeRaw(String text, int offset, int len) throws IOException, JsonGenerationException {
        currentCtx.addValue(text.substring(offset, offset + len));
    }
    
    @Override
    public void writeRaw(char[] text, int offset, int len) throws IOException, JsonGenerationException {
        currentCtx.addValue(new String(text, offset, len));
    }
    
    @Override
    public void writeRaw(char c) throws IOException, JsonGenerationException {
        currentCtx.addValue(c);
    }
    
    @Override
    public void writeBinary(Base64Variant b64variant, byte[] data, int offset, int len) throws IOException,
            JsonGenerationException {
        currentCtx.addValue(Arrays.copyOfRange(data, offset, offset + len));
    }
    
    @Override
    public void writeNumber(int v) throws IOException, JsonGenerationException {
        currentCtx.addValue(v);
    }
    
    @Override
    public void writeNumber(long v) throws IOException, JsonGenerationException {
        currentCtx.addValue(v);
    }
    
    @Override
    public void writeNumber(BigInteger v) throws IOException, JsonGenerationException {
        currentCtx.addValue(v);
    }
    
    @Override
    public void writeNumber(double d) throws IOException, JsonGenerationException {
        currentCtx.addValue(d);
    }
    
    @Override
    public void writeNumber(float f) throws IOException, JsonGenerationException {
        currentCtx.addValue(f);
    }
    
    @Override
    public void writeNumber(BigDecimal dec) throws IOException, JsonGenerationException {
        currentCtx.addValue(dec);
    }
    
    @Override
    public void writeNumber(String encodedValue) throws IOException, JsonGenerationException,
            UnsupportedOperationException {
        currentCtx.addValue(encodedValue);
    }
    
    @Override
    public void writeBoolean(boolean state) throws IOException, JsonGenerationException {
        currentCtx.addValue(state);
    }
    
    @Override
    public void writeNull() throws IOException, JsonGenerationException {
        currentCtx.addValue(null);
    }
    
}
