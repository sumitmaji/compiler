package com.sum.sparkImpl;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import com.sum.spark.Executor;

public class Fold01 extends Executor{
    private static transient Logger log = Logger.getLogger(Fold01.class);
    
    public Fold01() {
        super("Fold01");
    }
    
    @Override
    public void run() {
        JavaRDD<Marks> rdd = sc.parallelize(Arrays.asList(new Marks("Maths", 55), new Marks("Science", 65)), 8);
        Marks extra = new Marks("Extra", 4);
        Marks result = rdd.fold(extra, new Function2<Marks, Marks, Marks>() {
            @Override
            public Marks call(Marks arg0, Marks arg1) throws Exception {
                return new Marks("total", arg0.marks + arg1.marks);
            }
        });
        
        log.info("The total maeks: "+result);
    }
}

class Marks implements Serializable{
    String subject;
    Integer marks;
    public Marks(String subject, Integer marks){
        this.subject = subject;
        this.marks = marks;
    }
    @Override
    public String toString() {
        return "Marks[ subject: "+this.subject+", marks: "+this.marks;
    }
}
