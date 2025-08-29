package com.litmus7.inventoryfeedmultithreading.exception;

public class ProductServiceException extends Exception{
	private final String errorCode;

	public ProductServiceException(String errorCode) {
        super(errorCode);
        this.errorCode = errorCode;
    }

    public ProductServiceException(String errorCode, Throwable cause) {
        super(errorCode, cause);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

}
