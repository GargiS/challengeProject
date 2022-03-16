package customParser;

import entity.Feed;
import org.apache.commons.cli.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Parsers {

    public  static Feed readCommandLineOption(String args[]) {

        System.out.println("parser");
        CommandLine commandLine;
        Feed f = new Feed();

        Options options = new Options();
        options.addOption(Option.builder().required(true).longOpt("schema").build()); //.valueSeparator('=')//.desc("The r option")
        options.addOption(Option.builder().required(true).longOpt("tag").build());
        options.addOption(Option.builder().required(true).longOpt("data").build());
        options.addOption(Option.builder().required(true).longOpt("output").build());
        CommandLineParser parser =  new DefaultParser();
        try
        {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption("tag"))
            {
                f.setTagFileName(commandLine.getOptionValue("tag"));
            }

            if (commandLine.hasOption("schema"))
            {
                f.setSchemaFileName(commandLine.getOptionValue("schema"));
            }

            if (commandLine.hasOption("data"))
            {
                f.setDataFileName(commandLine.getOptionValue("data"));
            }
            if (commandLine.hasOption("output"))
            {
                f.setDataFileName(commandLine.getOptionValue("output"));
            }
            String[] remainder = commandLine.getArgs();
            f.setSchemaFileName(remainder[0]);
            f.setDataFileName(remainder[1]);
            f.setTagFileName(remainder[2]);
            f.setOutputFileName(remainder[3]);
        }
        catch (ParseException exception)
        {
            System.out.print("Parse error: ");
            System.out.println(exception.getMessage());
        }
        return f;
    }

    public static JSONObject parseJSONSchema(String schemaFileName) {
        JSONObject jo = null;
        try {
            jo = (JSONObject) new JSONParser().parse(new FileReader(schemaFileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException | org.json.simple.parser.ParseException ee) {
            ee.printStackTrace();
        }
        return jo;
    }

    public static String[] csvReader(String csvFile) {

        String[] tagDetails = new String[0];
        String line = "";
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))){
            while ((line = reader.readLine()) != null) {
                tagDetails = line.trim().split("\\|");
            }
        }
        catch (IOException ex){
            ex.printStackTrace();
            System.exit(-1);
        }
        return tagDetails;
    }

    public  static String readFileAsString(String fileName) throws Exception
    {
        String data = "";
        data = new String(Files.readAllBytes(Paths.get(fileName)));
        return data;
    }
}


/**
 *
 *

 // typecasting obj to JSONObject
 //    jo = (JSONObject) obj;

 //    System.out.println( jo.get("primary_keys").toString());

 //  JSONArray ab = (JSONArray) jo.get("columns");
 // JSONObject one = (JSONObject)ab.get(0);
 // System.out.println( one.get("name") + " - " + one.get("type") + " -" + one.get("mandatory"));
 //+ " - " + ab.get(1) + " - " + ab.get(2));
 */