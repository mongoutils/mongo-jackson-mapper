package com.github.mongoutils.jackson.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.mongoutils.jackson.mapper.DBCursorReader;
import com.github.mongoutils.jackson.mapper.MongoGenerator;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

@RunWith(MockitoJUnitRunner.class)
public class MongoTest {
    
    @Mock(answer = Answers.RETURNS_MOCKS)
    DBCollection collection;
    @Mock
    WriteResult writeResult;
    @Captor
    ArgumentCaptor<DBObject> objectCaptor;
    @Mock
    WriteConcern writeConcern;
    @Mock
    DBCursor cursor;
    DBObject value;
    
    ObjectMapper mapper;
    MongoGenerator generator;
    
    @Before
    public void init() {
        value = new BasicDBObject();
        
        when(cursor.next()).thenReturn(value);
        when(cursor.hasNext()).thenReturn(true, false);
        
        when(collection.getWriteConcern()).thenReturn(writeConcern);
        when(collection.insert(any(DBObject.class))).thenReturn(writeResult);
        
        mapper = new ObjectMapper();
        generator = new MongoGenerator(collection, mapper);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void nestedObject() throws Exception {
        TestBean bean1, bean2;
        WriteResult lastResult;
        
        bean2 = new TestBean();
        bean2.setData(new byte[] { 4, 5, 6 });
        bean2.setName("2");
        
        bean1 = new TestBean();
        bean1.setData(new byte[] { 1, 2, 3 });
        bean1.setName("1");
        bean1.setNext(bean2);
        
        bean1.getBeans().add(new TestBean("3"));
        bean1.getBeans().add(new TestBean("4"));
        bean1.getBeans().add(new TestBean("5"));
        
        mapper.writeValue(generator, bean1);
        lastResult = generator.getWriteResult();
        
        verify(collection).insert(objectCaptor.capture(), eq(writeConcern));
        
        assertEquals("1", objectCaptor.getValue().get("name"));
        assertEquals(1, ((byte[]) objectCaptor.getValue().get("data"))[0]);
        assertEquals(2, ((byte[]) objectCaptor.getValue().get("data"))[1]);
        assertEquals(3, ((byte[]) objectCaptor.getValue().get("data"))[2]);
        
        assertEquals("2", ((DBObject) objectCaptor.getValue().get("next")).get("name"));
        assertEquals(4, ((byte[]) ((DBObject) objectCaptor.getValue().get("next")).get("data"))[0]);
        assertEquals(5, ((byte[]) ((DBObject) objectCaptor.getValue().get("next")).get("data"))[1]);
        assertEquals(6, ((byte[]) ((DBObject) objectCaptor.getValue().get("next")).get("data"))[2]);
        
        assertEquals("3", ((DBObject) ((List<Object>) objectCaptor.getValue().get("beans")).get(0)).get("name"));
        assertEquals("4", ((DBObject) ((List<Object>) objectCaptor.getValue().get("beans")).get(1)).get("name"));
        assertEquals("5", ((DBObject) ((List<Object>) objectCaptor.getValue().get("beans")).get(2)).get("name"));
        
        assertNotNull(lastResult);
    }
    
    @Test
    public void array() throws Exception {
        List<TestBean> array = new ArrayList<TestBean>();
        
        array.add(new TestBean("1"));
        array.add(new TestBean("2"));
        array.add(new TestBean("3"));
        
        try {
            mapper.writeValue(generator, array);
            fail();
        } catch (JsonMappingException exception) {
            assertTrue(exception.getCause() instanceof RuntimeException);
        } catch (Exception exception) {
            fail();
        }
    }
    
    @Test
    public void simpleRead() throws Exception {
        TestBean bean;
        
        value.put("name", "1");
        value.put("count", "123");
        
        bean = mapper.readValue(new DBCursorReader(cursor), TestBean.class);
        
        assertEquals("1", bean.getName());
        assertEquals(123, bean.getCount());
    }
    
}
