package com.sum.sparkImpl;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.sum.context.SparkContext;
import com.sum.spark.Executor;

public class Filter01 extends Executor {
    private static transient Logger           log = Logger.getLogger(Filter01.class);

    @Override
    public void execute() {
        try {

            JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
            JavaRDD<Integer> result = rdd.filter(new Function<Integer, Boolean>() {

                @Override
                public Boolean call(Integer arg0) throws Exception {
                    if (arg0 > 3)
                        return true;

                    return false;
                }
            });
            
            log.info("The output is: "+StringUtils.join(result.collect(),","));

        } finally {
            sc.close();
        }
    }
}
