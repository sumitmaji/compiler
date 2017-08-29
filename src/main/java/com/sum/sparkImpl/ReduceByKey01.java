package com.sum.sparkImpl;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.sum.spark.Executor;

public class ReduceByKey01 extends Executor{
    private static transient Logger log = Logger.getLogger(ReduceByKey01.class);
    
    public ReduceByKey01() {
        super("ReduceByKey01");
    }
    
    @Override
    public void run() {
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("This is Sumit", "This is Greater Noida"));
        JavaRDD<String> word = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String arg0) throws Exception {
                return Arrays.asList(arg0.split(" ")).iterator();
            }
        });
        
        JavaPairRDD<String, Integer> words = word.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String arg0) throws Exception {
                return new Tuple2<String, Integer>(arg0, 1);
            }
        });
        
        JavaPairRDD<String, Integer> wordCount = words.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer arg0, Integer arg1) throws Exception {
                return arg0 + arg1;
            }
        });
        
        log.info("The words details: "+ wordCount.collect());
    }
}
