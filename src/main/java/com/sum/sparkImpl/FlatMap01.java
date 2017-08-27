package com.sum.sparkImpl;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.sum.spark.Executor;

public class FlatMap01 extends Executor{
    private static transient Logger log = Logger.getLogger(FlatMap01.class);
    
    public FlatMap01(){
        super("FlatMap01");
    }
    
    @Override
    public void run() {
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("This is Sumit", "I am great"));
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String arg0) throws Exception {
                return Arrays.asList(arg0.split(" ")).iterator();
            }
        });
        
        log.info("The data is: "+StringUtils.join(words.collect(),","));
        log.info("The first word is: "+ words.first());
    }
    
    
}
