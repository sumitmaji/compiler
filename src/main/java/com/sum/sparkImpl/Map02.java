package com.sum.sparkImpl;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.sum.spark.Executor;

public class Map02 extends Executor{
    private static transient Logger log = Logger.getLogger(Map02.class); 
    public Map02() {
        super("Map02");
    }
    
    @Override
    public void run() {
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("This is Sumit", "I am great"));
        JavaRDD<List<String>> wordsPerLine = lines.map(new Function<String, List<String>>() {
            @Override
            public List<String> call(String arg0) throws Exception {
                return Arrays.asList(arg0.split(" "));
            }
        });
        
        log.info("The words in first line is: "+ wordsPerLine.first());
    }
}
