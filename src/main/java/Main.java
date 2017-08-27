import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Main {
    
    private static transient Logger log = Logger.getLogger(Main.class);
        
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> rdd1 = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer arg0) throws Exception {
                return arg0*arg0;
            }
        });
        
        log.info("The data is: "+StringUtils.join(rdd1.collect(), ","));

    }
}
