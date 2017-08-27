package com.sum.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContext {
    
    private static JavaSparkContext sc = null;
    
    public static synchronized JavaSparkContext getContext(String name){
            if(sc == null){
                sc =  new JavaSparkContext(new SparkConf().setAppName(name).setMaster("local"));
            }
            return sc;
    }
}
