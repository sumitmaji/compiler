package com.sum.sparkImpl;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import com.sum.spark.Executor;

public class ForEach01 extends Executor{
    private static transient Logger log = Logger.getLogger(ForEach01.class);
    public ForEach01() {
        super("ForEach01");
    }
    @Override
    public void run() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7));
        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer arg0) throws Exception {
                log.info("The number is: "+ arg0);
            }
        });
    }
}
