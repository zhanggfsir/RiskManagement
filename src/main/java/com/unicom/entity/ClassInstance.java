package com.unicom.entity;

public class ClassInstance {

    private Class<?> implClass ;
    private Object instance ;

    public Class<?> getImplClass() {
        return implClass;
    }

    public Object getInstance() {
        return instance;
    }

    public void setImplClass(Class<?> implClass) {
        this.implClass = implClass;
    }

    public void setInstance(Object instance) {
        this.instance = instance;
    }

    public ClassInstance(Class<?> implClass, Object instance) {
        this.implClass = implClass;
        this.instance = instance;
    }
}
