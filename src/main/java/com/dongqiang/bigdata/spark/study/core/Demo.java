package com.dongqiang.bigdata.spark.study.core;

class SubClass extends SuperClass
{	
    public String name = "SubClass";
    
    public void test() {
    	System.out.println("sub");
    }
}

class SuperClass
{
	
    public String name = "SuperClass";
    public void test() {
    	System.out.println("super");
    }
}

public class Demo
 {
    public static void main(String[] args) 
    {
        SuperClass clz = new SubClass();
        //你觉得这里输出什么?
        System.out.println(clz.name);
        clz.test();
    }
}

