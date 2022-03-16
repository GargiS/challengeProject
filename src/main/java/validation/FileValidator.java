package validation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import projException.InvalidFileException;

import java.nio.file.Path;
import java.nio.file.Paths;


public class FileValidator {

     public static void validateFileName(String csvFileNameInTag,String csvPathInFeed) //throws InvalidFileException {
     {
            Path csvPath = Paths.get(csvPathInFeed);
            String dataFileName = csvPath.getFileName().toString();
            if (!dataFileName.equals(csvFileNameInTag)) {
                System.exit(2);
            }

        }
    public static void validatePrimaryKey(Dataset<Row> inputDF, JSONArray primary_key) {

            String joinedString = String.join(", ", primary_key);
            Dataset<Row> duplicateDF  =  inputDF.select(joinedString).groupBy(joinedString).count().filter("count>1");
            if ( (duplicateDF.count() >0) ||!duplicateDF.rdd().isEmpty() )
             System.exit(3);
        }

   public static void validateRecordCount(String tagFileCount, JavaRDD<String> inputDF)  {
            Long len =  Long.parseLong(tagFileCount);
            Long len2 = inputDF.count();
            if (len != len2-1)
                 System.exit(1);
        }


     static void validateCol(JSONObject schema, String inputHeader)
        {
            JSONArray ab = (JSONArray) schema.get("columns");
            try {
                String[] colNames = inputHeader.split(",");
                if (colNames.length != ab.size())
                          throw new InvalidFileException("Column size mismatch");

                for (int i = 0; i < colNames.length; i++) {
                    JSONObject current = (JSONObject) ab.get(i);
                    String tmpCol = current.get("name").toString();
                    if (!tmpCol.equals(colNames[i]))
                          throw new InvalidFileException("Column name mismatch");
                }
            }catch(InvalidFileException e)
            {
                System.exit(4);
            }
        }

    }
