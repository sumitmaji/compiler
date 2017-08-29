package com.sum.sparkImpl;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.sum.spark.Executor;

public class CountByValue01 extends Executor{
    private static transient Logger log = Logger.getLogger(CountByValue01.class);
    public CountByValue01() {
        super("CountByValue01");
    }
    @Override
    public void run() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,3,4,5,5,6,6));
        log.info("The map is: "+ rdd.countByValue());
    }
}
