import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class testMainClass implements SparkSessionTestWrapper {

    String prefix = "TempDirectory";
    private Path outputDirectory = Files.createTempDirectory("TmpDirectory");
    public testMainClass() throws IOException {
    }
  @BeforeClass
  public static void setUpClass() {

  }

  @AfterClass
  public static void afterClass() {

      // tmp directory to be deleted
     //tempDir.toFile().deleteOnExit();
  }

    @Test
    public void testInvalidRecord()
    {
        String[] args = {
                "--schema", "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/test/resources/scenarios/success/aus-capitals.json"
                ,"--data","/Users/gargi/Documents/InterviewQuestions/challengeProject/src/test/resources/scenarios/success/aus-capitals.csv"
                ,"--tag","/Users/gargi/Documents/InterviewQuestions/challengeProject/src/test/resources/scenarios/success/aus-capitals.tag"
                ,"--output", outputDirectory.toString()};
                 //"--output", "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/test/resources/scenarios/ExpectedOutput/"};
        mainClass.run(args,spark);

      Dataset<Row> result = spark.read()
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(getClass().getResource("scenarios/ExpectedOutput/output.csv").toString());

      Dataset<Row> expected = spark.read()
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(getClass().getResource("scenarios/ExpectedOutput/output.csv").toString());

    }
}
