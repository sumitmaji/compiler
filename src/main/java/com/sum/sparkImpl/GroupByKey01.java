package com.sum.sparkImpl;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.sum.spark.Executor;

public class GroupByKey01 extends Executor{
    private static transient Logger log = Logger.getLogger(GroupByKey01.class);
    public GroupByKey01() {
        super("GroupByKey01");
    }
    @Override
    public void run() {
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("This is Sumit", "This is Greater Noida"));
        JavaPairRDD<String, Integer> wordCount = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String arg0) throws Exception {
                return Arrays.asList(arg0.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String arg0) throws Exception {
                return new Tuple2<String, Integer>(arg0, 1);
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> arg0) throws Exception {
                Iterable<Integer> numbers = arg0._2;
                Integer sum = 0;
                for(Integer i : numbers){
                    sum += i; 
                }   
                return new Tuple2<String, Integer>(arg0._1, sum);
            }
        });

        log.info("The word count is: " + wordCount.collect());        
    }
}
