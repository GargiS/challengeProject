import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.io.File;

public class testFileValidatorClass implements SparkSessionTestWrapper {

    protected final String outputDirectory = System.getProperty("user.dir") + "/sparkOutputDirectory";
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @BeforeClass
    public static void setUpClass() {

    }

    @AfterClass
    public static void afterClass() {
    }


    @Test
    public void testInvalidFile()
    {
        ClassLoader classLoader = getClass().getClassLoader();
        File tagFile = new File(classLoader.getResource("file/test.xml").getFile());

    }

    public void testRecordCount() {
        ClassLoader classLoader = getClass().getClassLoader();
        File csvFile = new File(classLoader.getResource("scenarios/aus-capitals.csv").getFile());
        File tagFile = new File(classLoader.getResource("scenarios/aus-capitals-invalid-1.tag").getFile());
        File schemaFile = new File(classLoader.getResource("scenarios/aus-capitals.csv").getFile());

    }

    /* done **/
    @Test
    public void testInvalidRecord()
    {
        System.out.println("hello");
        exit.expectSystemExitWithStatus(1);

        String[] args = {
                "--schema", "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals.json"
                ,"--data","/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals.csv"
                ,"--tag","/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals-invalid-1.tag"
                ,"--output", "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/output.csv"};
            mainClass.run(args,spark);
        }


    @Test
    public void testValidatePrimaryKey()
    {
        System.out.println("Test case when duplicates in primary Key...");
        exit.expectSystemExitWithStatus(3);

        String[] args = {
                "--schema", "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/duplicatePrimaryKey/aus-capitals.json"
                ,"--data","/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/duplicatePrimaryKey/aus-capitals.csv"
                ,"--tag","/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/duplicatePrimaryKey/aus-capitals.tag"
                ,"--output", "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/output.csv"};
        mainClass.run(args,spark);

    }

    @Test
    public void testvalidateFileName()
    {
        //exit.expectSystemExit(4);

        System.out.println("hello1");
        exit.expectSystemExitWithStatus(2);

        String[] args = {
                "--schema", "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/success/aus-capitals.json"
                ,"--data","/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/success/aus-capitals.csv"
                ,"--tag","/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/IncorrectFileName/aus-capitals-invalid-2.tag"
                ,"--output", "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/output.csv"};
        mainClass.run(args,spark);

    }

    @Test
 public void testParser() throws Exception {
      // final String[] args = {"-b", "-foobar"};

            //      assertTrue(cl.hasOption("b"));
              //    assertTrue(cl.hasOption("f"));
               //   assertEquals("bar", cl.getOptionValue("foo"));
              }
}


/***
 *
 //    Feed inputFeed = new Feed("/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals-invalid-1.tag",
 //          "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals.json",
 //        "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals.csv");

 //final String[] args = new String("--schema=\"/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals.json\"" +
 //      ",--data=\"/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals.csv\"" +
 //    ",--tag=\"/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals-invalid-1.tag\"").split(",");

 //String[] testArgs =
 //      { "-r", "opt1", "-S", "opt2", "arg1", "arg2",
 //            "arg3", "arg4", "--test", "-A", "opt3", };
 */