import org.apache.log4j.Logger;
import com.sum.spark.Executor;
import com.sum.sparkImpl.Map01;


public class Main {

    private static transient Logger log = Logger.getLogger(Main.class);

    public static void main(String[] args) {

        Executor ex = new Map01();
        ex.execute();
    }
}
