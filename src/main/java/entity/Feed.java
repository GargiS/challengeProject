package entity;

import java.io.Serializable;
import java.util.Objects;

public class Feed implements Serializable {

    String tagFileName;
    String schemaFileName;
    String dataFileName;
    String outputFileName;

    public Feed() {}

    public Feed(String tagFileName, String schemaFileName, String dataFileName,String outputFileName) {
        this.tagFileName = tagFileName;
        this.schemaFileName = schemaFileName;
        this.dataFileName = dataFileName;
        this.outputFileName = outputFileName;
    }


    public String getTagFileName() {
        return tagFileName;
    }

    public void setTagFileName(String tagFileName) {
        this.tagFileName = tagFileName;
    }

    public String getSchemaFileName() {
        return schemaFileName;
    }

    public void setSchemaFileName(String schemaFileName) {
        this.schemaFileName = schemaFileName;
    }

    public String getDataFileName() {
        return dataFileName;
    }

    public void setDataFileName(String dataFileName) {
        this.dataFileName = dataFileName;
    }
    public String getOutputFileName() {
        return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    @Override
    public String toString() {
        return "Feed{" +
                "tagFileName='" + tagFileName + '\'' +
                ", schemaFileName='" + schemaFileName + '\'' +
                ", dataFileName='" + dataFileName + '\'' +
                ", outputFileName='" + outputFileName + '\'' +
                '}';
    }

    public static void printFeed(Feed inputFeed)
    {
        System.out.println("File details are tagFile = " + inputFeed.getTagFileName());
        System.out.println("DataFile Path = " + inputFeed.getDataFileName());
        System.out.println("schemaFile Path = " + inputFeed.getSchemaFileName());
        System.out.println("OutputPath = " + inputFeed.getOutputFileName());
    }
}
