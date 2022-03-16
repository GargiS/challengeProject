package validation;

import projException.InvalidRecordException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ContentValidator {

    public static boolean validateDataType(String inputValue, int index, JSONObject schema)  {

        JSONArray ab = (JSONArray) schema.get("columns");
        System.out.println(" inside index = validatedatatype" + index);
        JSONObject current = (JSONObject)ab.get(index);
        String inValue  = current.get("type").toString();

        try {
            switch (inValue) {
                case "INTEGER":
                    System.out.println("checking for INteger");
                    validateInteger(inputValue);
                    break;
                case "DOUBLE":
                    System.out.println("checking for double");
                    validateFloat(inputValue);
                    break;
                case "DATE":
                    System.out.println("checking for Dat");
                    validateDate(inputValue, current.get("format").toString());
                    break;
                default:
                    break;
            }
        }catch(InvalidRecordException e)
        {
            System.out.println(e.toString());
            return false;
        }
        return true;
    }

    public static boolean validateMandatoryChk(String inputValue, int index, JSONObject schema) {

        JSONArray ab = (JSONArray) schema.get("columns");
        JSONObject current = (JSONObject)ab.get(index);
        String inValue  = current.get("mandatory").toString();
        if(inValue.equals("true") && (inputValue.isBlank() || inputValue.isEmpty()))
            return false;
        return true;
    }

    public static boolean validateInteger(String colValue) throws InvalidRecordException
    {
        try {
            Integer.parseInt(colValue);
        } catch (NumberFormatException e) {
            throw new InvalidRecordException("Incorrect Data Type. Expected Data type is Integer as per the Schema File");
        }
        return true;
    }

    public static void validateDate(String colValue,String format) throws InvalidRecordException
    {
        try{
            java.time.LocalDate.parse(colValue, java.time.format.DateTimeFormatter.ofPattern(format));

        } catch(java.time.format.DateTimeParseException e) {
            throw new InvalidRecordException("Incorrect DateTime Format");
        }
    }

    public static boolean validateFloat(String colValue) throws InvalidRecordException
    {
        try {
            Double.parseDouble(colValue);
        } catch (NumberFormatException e) {
            throw new InvalidRecordException("Incorrect Data Type. Expected Data type is Double as per the Schema File");
        }

        return true;
    }
}
