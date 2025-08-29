package com.litmus7.inventoryfeedmultithreading.constant;



public class LoggerConstants { 

    public static final String RECEIVED_PRODUCT_DATA_LOG_MESSAGE =
            "Received product data: sku={}, Product_name={}, quantity={}, price={}";

    public static final String SUCCESSFULLY_SAVED_LOG_MESSAGE =
            "Successfully saved product with sku {}";

    public static final String SAVE_FAILURE_LOG_MESSAGE =
            "Save failure for product with sku {}";

    public static final String BUSINESS_EXCEPTION_LOG_MESSAGE =
            "Business exception occurred!";

    public static final String UNEXPECTED_ERROR_LOG_MESSAGE =
            "Unexpected error occurred!";

    public static final String NOT_CSV_LOG_MESSAGE =
            "Not a csv file";

    public static final String EXIT_METHOD_LOG_MESSAGE =
            "Exiting from {} method";

    public static final String ENTER_METHOD_WITH_SKUS_LOG_MESSAGE =
            "Entered {} method with {} skus";

    public static final String ENTER_METHOD_LOG_MESSAGE =
            "Entered {} method";

    public static final String ENTER_METHOD_WITH_SKU_LOG_MESSAGE =
            "Entered {} method with sku {}";

    public static final String ENTER_METHOD_WITH_PRODUCTS_LOG_MESSAGE =
            "Entered {} method with {} products";

    public static final String VALID_AND_NON_EXISTENT_PRODUCT_LOG_MESSAGE =
            "Product with sku {} passed validation and does not exist in db";

    public static final String VALIDATION_FAILED_LOG_MESSAGE =
            "Validation failed for product with sku {}";

    public static final String ALREADY_EXISTS_IN_DB_LOG_MESSAGE =
            "Product with sku {} already exists in db";

    public static final String EXITING_AFTER_SUCCESS_SAVE_IN_DB =
            "Exiting after successfully saving product with sku {} into db";

    public static final String EXITING_AFTER_FAILURE_SAVE_IN_DB =
            "Exiting after failing to save product with sku {} into db";

    public static final String VALID_AND_NON_EXISTENT_PRODUCTS_LOG_MESSAGE =
            "Saved {} products that were valid and non-existing in db";

    public static final String FETCHED_DETAILS_OF_EXISTING_SKUS_FROM_DB_LOG_MESSAGE =
            "Fetched the details of {} existing products from db";

    public static final String EXITING_AFTER_SUCCESS_UPDATE_IN_DB =
            "Exiting after successfully updating product with sku {} in db";

    public static final String EXITING_AFTER_SUCCESS_UPDATES_IN_DB =
            "Exiting after successfully updating {} products in db";

    public static final String EXITING_AFTER_FAILURE_UPDATE_IN_DB =
            "Exiting after failing to update product with sku {} in db";

    public static final String NON_EXISTING_IN_DB_LOG_MESSAGE =
            "Product with sku {} doesn't exist in db";

    public static final String EXITING_AFTER_SUCCESS_SAVES_IN_DB =
            "Exiting after successfully saving {} product(s) into db";

    public static final String EXITING_AFTER_FAILURE_SAVES_IN_DB =
            "Exiting after failing to save any of the product(s) into db";

    public static final String DB_CONNECTION_SUCCESS =
            "Successfully connected to the database";

    public static final String EXITING_AFTER_SUCCESS_DELETE_IN_DB =
            "Exiting after successfully deleting product with sku {} from db";
    
    public static final String BATCH_INSERT_PRODUCTS =
            "batchInsertProducts";
    
    
}





