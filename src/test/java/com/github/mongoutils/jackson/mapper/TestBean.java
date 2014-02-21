package com.github.mongoutils.jackson.mapper;

import java.util.ArrayList;
import java.util.List;

public class TestBean {
    
    protected String name;
    protected int count;
    protected byte[] data;
    protected TestBean next;
    protected List<TestBean> beans = new ArrayList<TestBean>();
    
    public TestBean() {
    }
    
    public TestBean(String name) {
        this.name = name;
    }
    
    public int getCount() {
        return count;
    }
    
    public void setCount(int count) {
        this.count = count;
    }
    
    public TestBean getNext() {
        return next;
    }
    
    public void setNext(TestBean next) {
        this.next = next;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public byte[] getData() {
        return data;
    }
    
    public void setData(byte[] data) {
        this.data = data;
    }
    
    public List<TestBean> getBeans() {
        return beans;
    }
    
    public void setBeans(List<TestBean> beans) {
        this.beans = beans;
    }
    
}
