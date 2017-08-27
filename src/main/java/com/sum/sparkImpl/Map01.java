package com.sum.sparkImpl;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.commons.lang3.StringUtils;

import com.sum.context.SparkContext;
import com.sum.spark.Executor;

public class Map01 extends Executor {

    private static transient Logger log = Logger.getLogger(Map01.class);

    public Map01(){
        super("Map01");
    }

    @Override
    public void run() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        JavaRDD<Integer> rdd1 = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer arg0) throws Exception {
                return arg0 * arg0;
            }
        });

        log.info("The data is: " + StringUtils.join(rdd1.collect(), ","));
        log.info(rdd1.collect());
    }
}
