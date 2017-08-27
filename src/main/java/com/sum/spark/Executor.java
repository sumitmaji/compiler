package com.sum.spark;
import java.io.Serializable;
import org.apache.spark.api.java.JavaSparkContext;
import com.sum.context.SparkContext;

public abstract class Executor implements Serializable{
    protected static transient JavaSparkContext sc  = null;
    protected Executor(String name){
        sc = SparkContext.getContext("Filter01");
    }
    
    public void execute(){
        try{
            run();
        }finally{
            sc.close();
        }    
    }
    
    public abstract void run();
}
