package com.litmus7.inventoryfeedmultithreading.service;



import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.litmus7.inventoryfeedmultithreading.constant.LoggerConstants;
import com.litmus7.inventoryfeedmultithreading.dao.InventoryFeedDao;
import com.litmus7.inventoryfeedmultithreading.dto.Product;
import com.litmus7.inventoryfeedmultithreading.exception.ProductDataAccessException;
import com.litmus7.inventoryfeedmultithreading.exception.ProductServiceException;
import com.litmus7.inventoryfeedmultithreading.util.CsvUtil;
import com.litmus7.inventoryfeedmultithreading.util.FileMoverUtil;
import com.litmus7.inventoryfeedmultithreading.util.ValidationUtil;

public class InventoryFeedService {

	private static final Logger logger = LogManager.getLogger(InventoryFeedService.class);
    InventoryFeedDao inventoryDao = new InventoryFeedDao();

    public boolean writeFromProductFileToDb(String file) throws ProductServiceException {

        logger.trace(LoggerConstants.ENTER_METHOD_LOG_MESSAGE, "writeFromProductFileToDb()");
        List<String[]> records = new ArrayList<>();
        List<Product> products = new ArrayList<>();

        Path inputFile = Paths.get(file);
        Path baseDir = Paths.get("src/inventory-feed");
        Path targetForSuccess = baseDir.resolve("processed").resolve(inputFile.getFileName());
        Path targetForErrors = baseDir.resolve("error").resolve(inputFile.getFileName());

        // === Step 1: Read CSV ===
        try {
            logger.info("Reading CSV file: {}", file);
            records = CsvUtil.readCSV(file);
            logger.debug("CSV file [{}] read successfully, total records: {}", file, records.size());

        } catch (ProductDataAccessException e) {
            try {
                FileMoverUtil.moveFile(inputFile, targetForErrors, "CSV read failure");
            } catch (IOException ioEx) {
                throw new ProductServiceException("PRD-SVC-500.fileMoveErrorDirectory", ioEx);
            }
            throw new ProductServiceException(e.getErrorCode(), e);
        }

        // === Step 2: Mapping + Validation ===
        try {
            logger.trace("Starting product mapping and validation for file [{}]", file);
            for (String[] values : records) {
                Product product = new Product();
                try {
                    product.setSku(Integer.parseInt(values[0].trim()));
                    product.setProductName(values[1].trim());
                    product.setQuantity(Double.parseDouble(values[2].trim()));
                    product.setPrice(Double.parseDouble(values[3].trim()));
                } catch (NumberFormatException e) {
                    try {
                        FileMoverUtil.moveFile(inputFile, targetForErrors, "Invalid product format");
                    } catch (IOException ioEx) {
                        throw new ProductServiceException("PRD-SVC-500.fileMoveErrorDirectory", ioEx);
                    }
                    throw new ProductServiceException("PRD-SVC-400.invalidProduct", e);
                }

                if (!ValidationUtil.validateProduct(product)) {
                    try {
                        FileMoverUtil.moveFile(inputFile, targetForErrors, "Validation failure");
                    } catch (IOException ioEx) {
                        throw new ProductServiceException("PRD-SVC-500.fileMoveErrorDirectory", ioEx);
                    }
                    return false;
                }

                logger.debug("Validated product: {}", product);
                products.add(product);
            }

            // === Step 3: DB Insert ===
            logger.info("Attempting batch insert of {} products", products.size());
            boolean success = inventoryDao.batchInsertProducts(products);

            if (success) {
                try {
                    FileMoverUtil.moveFile(inputFile, targetForSuccess, "Successful DB insert");
                } catch (IOException ioEx) {
                    throw new ProductServiceException("PRD-SVC-500.fileMoveErrorDirectory", ioEx);
                }
                logger.info("Batch insert successful for file [{}]", file);
            } else {
                try {
                    FileMoverUtil.moveFile(inputFile, targetForErrors, "DB insert failure");
                } catch (IOException ioEx) {
                    throw new ProductServiceException("PRD-SVC-500.fileMoveErrorDirectory", ioEx);
                }
                logger.error("Batch insert failed for file [{}]", file);
            }

            logger.trace(LoggerConstants.EXIT_METHOD_LOG_MESSAGE, "writeFromProductFileToDb()");
            return success;

        } catch (ProductDataAccessException e) {
            try {
                FileMoverUtil.moveFile(inputFile, targetForErrors, "Database error");
            } catch (IOException ioEx) {
                throw new ProductServiceException("PRD-SVC-500.fileMoveErrorDirectory", ioEx);
            }
            throw new ProductServiceException(e.getErrorCode(), e);

        } 
    }
}


