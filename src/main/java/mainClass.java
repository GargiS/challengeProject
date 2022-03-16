import customParser.Parsers;
import entity.Feed;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import validation.*;

import java.util.*;

public class mainClass implements sparkJob{

    public static void run(String args[],SparkSession spark)  {

        Feed inputFeed = Parsers.readCommandLineOption(args);
        Feed.printFeed(inputFeed);

        JSONObject fileSchema = Parsers.parseJSONSchema(inputFeed.getSchemaFileName());
        JavaRDD<String> inputDF = spark.read().textFile(inputFeed.getDataFileName()).javaRDD();
        String[] tagArray = Parsers.csvReader(inputFeed.getTagFileName());
        FileValidator.validateRecordCount(tagArray[1], inputDF);
        FileValidator.validateFileName(tagArray[0],inputFeed.getDataFileName()) ;

        String[] columnsList = inputDF.first().split(",");
        List<String> tableColumns = Arrays.asList(columnsList);
        JavaRDD<Row> rdd2 = inputDF.filter(x -> !x.contains(columnsList[0])).map(s -> s.split(","))
                    .map(s -> RowFactory.create((Object[]) s));
        Dataset<Row> df = spark.createDataFrame(rdd2, createSchema(tableColumns));
        FileValidator.validatePrimaryKey(df, (JSONArray) fileSchema.get("primary_keys"));

        JavaRDD<String> outputDF2 =  rdd2.map(records -> {
            int dirty_flag=0;
            for (int i = 0; i < records.length(); i++) {
                if (!ContentValidator.validateDataType(records.getString(i), i, fileSchema) || !ContentValidator.validateMandatoryChk(records.getString(i), i, fileSchema)) {
                     dirty_flag = 1;
                     break;
                }
            }
            return new String(records.mkString(",") + "," + String.valueOf(dirty_flag));
         });

        String newColList = String.join("," ,String.join(",", tableColumns),"Dirty_flag");
        Dataset<Row> resultDS = RDDtoDatasetConvertor(newColList,outputDF2,spark);

        resultDS.write().format("delta").option("mergeSchema","true").save(inputFeed.getOutputFileName());

 }

    public static StructType createSchema(List<String> tableColumns){

        List<StructField> fields  = new ArrayList<StructField>();
        for(String column : tableColumns){
            fields.add(DataTypes.createStructField(column, DataTypes.StringType, true));

        }
        return DataTypes.createStructType(fields);
    }

    public static Dataset<Row> RDDtoDatasetConvertor(String colList,JavaRDD<String> inputDF,SparkSession spark)
{
    List<String> newTableColumns = Arrays.asList(colList.split(","));
    JavaRDD<Row> intermediateDS = inputDF.map(s -> s.split(",")).map(s -> RowFactory.create((Object[]) s));
    return spark.createDataFrame(intermediateDS, createSchema(newTableColumns));
}


}
