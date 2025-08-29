package com.litmus7.inventoryfeedmultithreading.exception;

public class ProductDataAccessException extends Exception{
	private final String errorCode;

	public ProductDataAccessException(String errorCode) {
        super(errorCode);
        this.errorCode = errorCode;
    }

    public ProductDataAccessException(String errorCode, Throwable cause) {
        super(errorCode, cause);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

}
