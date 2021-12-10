package com.subhadipmitra.datory.preprocessing.common.exceptions;

public class ApplicationException extends Exception {

    private static final long serialVersionUID = 7652359958369278826L;

    public ApplicationException(String message) {
        super(message);
    }

    public ApplicationException(Throwable throwable) {
        super(throwable);
    }

    public ApplicationException(String message, Throwable throwable) {
        super(message, throwable);
    }
}