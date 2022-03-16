import org.apache.spark.sql.SparkSession;

public interface SparkSessionTestWrapper {

    SparkSession spark =  SparkSession
            .builder()
            .master("local[2]")
            .appName("SampleSparkScalaTest")
            .getOrCreate();

}
