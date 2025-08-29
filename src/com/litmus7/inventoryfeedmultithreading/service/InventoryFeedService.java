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

        try {
            logger.info("Reading CSV file: {}", file);
            records = CsvUtil.readCSV(file);
            logger.debug("CSV file [{}] read successfully, total records: {}", file, records.size());

        } catch (ProductDataAccessException e) {
            logger.error("Failed to read CSV file: {}", file, e);
            try {
                Files.createDirectories(targetForErrors.getParent());
                Files.move(inputFile, targetForErrors, StandardCopyOption.REPLACE_EXISTING);
                logger.warn("Moved file [{}] to error directory due to read failure", file);
            } catch (IOException ioEx) {
                logger.fatal("Failed to move file [{}] to error directory", file, ioEx);
                throw new ProductServiceException("PRD-SVC-500.fileMoveErrorDirectory", ioEx);
            }
            throw new ProductServiceException(e.getErrorCode(), e);
        }

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
                    logger.error("Invalid number format in file [{}] for record: {}", file, Arrays.toString(values), e);
                    Files.createDirectories(targetForErrors.getParent());
                    Files.move(inputFile, targetForErrors, StandardCopyOption.REPLACE_EXISTING);
                    logger.warn("Moved file [{}] to error directory due to invalid product format", file);
                    throw new ProductServiceException("PRD-SVC-400.invalidProduct", e);
                }

                if (!ValidationUtil.validateProduct(product)) {
                    logger.warn("Validation failed for product: {}", product);
                    Files.createDirectories(targetForErrors.getParent());
                    Files.move(inputFile, targetForErrors, StandardCopyOption.REPLACE_EXISTING);
                    logger.warn("Moved file [{}] to error directory due to validation failure", file);
                    return false;
                }
                logger.debug("Validated product: {}", product);
                products.add(product);
            }

            logger.info("Attempting batch insert of {} products", products.size());
            boolean success = inventoryDao.batchInsertProducts(products);

            if (success) {
                logger.info("Batch insert successful for file [{}]", file);
                Files.createDirectories(targetForSuccess.getParent());
                Files.move(inputFile, targetForSuccess, StandardCopyOption.REPLACE_EXISTING);
                logger.debug("Moved file [{}] to processed directory", file);
            } else {
                logger.error("Batch insert failed for file [{}]", file);
                Files.createDirectories(targetForErrors.getParent());
                Files.move(inputFile, targetForErrors, StandardCopyOption.REPLACE_EXISTING);
                logger.warn("Moved file [{}] to error directory due to DB insert failure", file);
            }

            logger.trace(LoggerConstants.EXIT_METHOD_LOG_MESSAGE, "writeFromProductFileToDb()");
            return success;

        } catch (ProductDataAccessException e) {
            logger.error("Database access error while inserting products for file [{}]", file, e);
            try {
                Files.createDirectories(targetForErrors.getParent());
                Files.move(inputFile, targetForErrors, StandardCopyOption.REPLACE_EXISTING);
                logger.warn("Moved file [{}] to error directory due to DB error", file);
            } catch (IOException ioEx) {
                logger.fatal("Failed to move file [{}] after DB error", file, ioEx);
                throw new ProductServiceException("PRD-SVC-500.fileMoveErrorDirectory", ioEx);
            }
            throw new ProductServiceException(e.getErrorCode(), e);
        } catch (IOException e) {
            logger.fatal("I/O error while moving file [{}]", file, e);
            throw new ProductServiceException("PRD-SVC-500.fileMove", e);
        }
    }
}

