package projException;

public class InvalidFileException extends Exception{

    public InvalidFileException(String errorMessage) {
        super(errorMessage);
    }
    public InvalidFileException() {
        super();
    }
}
