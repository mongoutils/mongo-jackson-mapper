package com.github.mongoutils.jackson.mapper;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Array;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.bson.types.Binary;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.Base64Variants;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DBCursorReader extends Reader {
    
    protected DBCursor cursor;
    protected StringBuilder builder = new StringBuilder();
    protected int builderOffset = 0;
    protected Base64Variant b64variant = Base64Variants.getDefaultVariant();
    
    public DBCursorReader(DBCursor cursor) {
        this.cursor = cursor;
    }
    
    public DBCursorReader(DBCursor cursor, Base64Variant base64Variant) {
        this.cursor = cursor;
        b64variant = base64Variant;
    }
    
    @Override
    public void close() throws IOException {
    }
    
    @Override
    public int read(char[] buffer, int offset, int length) throws IOException {
        int remaining = length, tmp, copied = 0;
        
        while (remaining > 0) {
            if (builderOffset >= builder.length()) {
                fillBuffer();
                builderOffset = 0;
            }
            
            if (builder.length() == 0) {
                break;
            }
            
            tmp = Math.min(length, builder.length() - builderOffset);
            builder.getChars(builderOffset, builderOffset + tmp, buffer, offset);
            builderOffset += tmp;
            copied += tmp;
            remaining -= tmp;
        }
        
        if (copied == 0) {
            copied = -1;
        }
        
        return copied;
    }
    
    protected void fillBuffer() {
        DBObject next;
        
        builder.delete(0, builder.length());
        while (cursor.hasNext()) {
            if ((next = cursor.next()) != null) {
                stringify(next);
            }
        }
    }
    
    protected void stringify(DBObject obj) {
        Object field;
        Set<String> keys = obj.keySet();
        boolean filled = false;
        
        builder.append('{');
        for (String key : keys) {
            field = obj.get(key);
            if (field != null) {
                builder.append('"');
                builder.append(key);
                builder.append("\":");
                stringify(field);
                builder.append(',');
                filled = true;
            }
        }
        if (filled) {
            builder.delete(builder.length() - 1, builder.length());
        }
        builder.append('}');
    }
    
    @SuppressWarnings("unchecked")
    protected void stringify(Object obj) {
        boolean filled = false;
        CharSequence seq;
        char c;
        
        obj = Bytes.applyEncodingHooks(obj);
        
        if (obj instanceof CharSequence) {
            seq = (CharSequence) obj;
            
            builder.append('"');
            for (int i = 0; i < seq.length(); i++) {
                c = seq.charAt(i);
                if (c == '\\') {
                    builder.append("\\\\");
                } else if (c == '"') {
                    builder.append("\\\"");
                } else if (c == '\n') {
                    builder.append("\\n");
                } else if (c == '\r') {
                    builder.append("\\r");
                } else if (c == '\t') {
                    builder.append("\\t");
                } else if (c == '\b') {
                    builder.append("\\b");
                } else if (c < 32) {
                    continue;
                } else {
                    builder.append(c);
                }
            }
            builder.append('"');
        } else if (obj instanceof Boolean || obj instanceof Number) {
            builder.append(obj);
        } else if (obj instanceof Iterable) {
            builder.append('[');
            for (Object value : (Iterable<Object>) obj) {
                stringify(value);
                builder.append(',');
                filled = true;
            }
            if (filled) {
                builder.delete(builder.length() - 1, builder.length());
            }
            builder.append(']');
        } else if (obj.getClass().isArray()) {
            builder.append('[');
            for (int i = 0; i < Array.getLength(obj); i++) {
                stringify(Array.get(obj, i));
                builder.append(',');
                filled = true;
            }
            if (filled) {
                builder.delete(builder.length() - 1, builder.length());
            }
            builder.append(']');
        } else if (obj instanceof DBObject) {
            stringify((DBObject) obj);
        } else if (obj instanceof Map) {
            builder.append('{');
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) obj).entrySet()) {
                builder.append(entry.getKey());
                builder.append(':');
                stringify(entry.getValue());
                builder.append(',');
                filled = true;
            }
            if (filled) {
                builder.delete(builder.length() - 1, builder.length());
            }
            builder.append('}');
        } else if (obj instanceof byte[]) {
            builder.append(b64variant.encode((byte[]) obj));
        } else if (obj instanceof Binary) {
            builder.append(b64variant.encode(((Binary) obj).getData()));
        } else if (obj instanceof Date) {
            builder.append(((Date) obj).getTime());
        }
    }
    
}
