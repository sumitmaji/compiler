package com.sum.sparkImpl;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import com.sum.spark.Executor;

public class Aggregate01 extends Executor{
    private static transient Logger log = Logger.getLogger(Aggregate01.class);
    
    public Aggregate01() {
        super("Aggregate01");
    }
    
    @Override
    public void run() {
        JavaRDD<Marks> rdd = sc.parallelize(Arrays.asList(new Marks("Maths",55), new Marks("Science", 65)), 8);
        Integer total = rdd.aggregate(4, new Function2<Integer, Marks, Integer>() {
            @Override
            public Integer call(Integer arg0, Marks arg1) throws Exception {
                // TODO Auto-generated method stub
                log.info("The arg0 value in each partition: "+arg0);
                return arg0 + arg1.marks;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer arg0, Integer arg1) throws Exception {
                log.info("The arg0, arg1: "+arg0+","+arg1);
                return arg0 + arg1;
            }
        });
        
        log.info("The total is: "+ total);
    }
}
