package cn.edu.thssdb.exception;

public class IllegalTypeException extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception: Illegal Type Found!";
    }
}
