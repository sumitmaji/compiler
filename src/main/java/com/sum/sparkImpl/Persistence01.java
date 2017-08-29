package com.sum.sparkImpl;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import com.sum.spark.Executor;

public class Persistence01 extends Executor{
    private static transient Logger log = Logger.getLogger(Persistence01.class);
    public Persistence01() {
        super("Persistence01");
    }
    
    @Override
    public void run() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5,6));
        JavaRDD<Integer> rdd1 = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer arg0) throws Exception {
                return arg0*arg0;
            }
        });
        
        log.info("The number of data: "+rdd1.count());
        log.info("The comma separation: "+ rdd1.collect().toString());
        
        rdd1.persist(StorageLevel.MEMORY_ONLY());
        log.info("The number of data: "+rdd1.count());
        log.info("The comma separation: "+ rdd1.collect().toString());
        
    }
}
