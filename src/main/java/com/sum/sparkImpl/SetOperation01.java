package com.sum.sparkImpl;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import com.sum.spark.Executor;

/**
 * Union operation
 */
public class SetOperation01 extends Executor{
    private static transient Logger log = Logger.getLogger(SetOperation01.class);
    
    public SetOperation01() {
        super("SetOperation01");
    }
    
    @Override
    public void run() {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("This is Sumit", "I am great"));
        
        JavaRDD<String> words1 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String arg0) throws Exception {
                return Arrays.asList(arg0.split(" ")).iterator();
            }
        });
        
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("This is Greater Noida"));
        JavaRDD<String> words2 = rdd2.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String arg0) throws Exception {
                return Arrays.asList(arg0.split(" ")).iterator();
            }
        });
        
        JavaRDD<String> union = words1.union(words2);
        log.info("The Union data is: " + union.collect());
    }
}
