package projException;

public class InvalidRecordException extends Exception{

    public InvalidRecordException(String errorMessage) {
        super(errorMessage);
    }
    public InvalidRecordException() {

    }
}
