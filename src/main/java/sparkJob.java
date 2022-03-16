import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public interface sparkJob {

    public static void main(String args[])  {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        mainClass.run(args,spark);
    }


}
