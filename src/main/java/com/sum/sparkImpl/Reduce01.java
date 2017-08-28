package com.sum.sparkImpl;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import com.sum.spark.Executor;

public class Reduce01 extends Executor{
    private static transient Logger log = Logger.getLogger(Reduce01.class);
    
    public Reduce01() {
        super("Reduce01");
    }
    
    @Override
    public void run() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
        Integer sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            
            @Override
            public Integer call(Integer arg0, Integer arg1) throws Exception {
                return arg0 + arg1;
            }
        });
        
        log.info("The sum is: "+ sum);
    }
}
