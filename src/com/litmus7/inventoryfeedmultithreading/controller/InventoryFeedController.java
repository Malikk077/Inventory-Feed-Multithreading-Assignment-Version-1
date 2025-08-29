package com.litmus7.inventoryfeedmultithreading.controller;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import com.litmus7.inventoryfeedmultithreading.constant.Constant;
import com.litmus7.inventoryfeedmultithreading.constant.LoggerConstants;
import com.litmus7.inventoryfeedmultithreading.dto.Response;
import com.litmus7.inventoryfeedmultithreading.exception.ProductServiceException;
import com.litmus7.inventoryfeedmultithreading.service.InventoryFeedService;
import com.litmus7.inventoryfeedmultithreading.util.ErrorMessageUtil;

public class InventoryFeedController {
	private static final Logger logger = LogManager.getLogger(InventoryFeedController.class);
	InventoryFeedService inventoryService =new InventoryFeedService();
	
public Response<Integer> csvtoDB(URI folder) {
        
        logger.trace(LoggerConstants.ENTER_METHOD_LOG_MESSAGE, "csvtoDB()");
        Path folderPath = Paths.get(folder);
	    
        int successCount = 0;
        int failureCount = 0;

        try (Stream<Path> paths = Files.list(folderPath)) {
            logger.info("Scanning folder for files: {}", folderPath);

            for (Path file : (Iterable<Path>) paths.filter(Files::isRegularFile)::iterator) {
                
                String fileName = file.getFileName().toString().toLowerCase();
                logger.debug("Found file: {}", fileName);
                
                if (!fileName.endsWith(".csv")) {
                    logger.warn("Skipping non-CSV file: {}", fileName);
                    return new Response<>(Constant.FILE_NOT_CSV, ErrorMessageUtil.getErrorMessage("EMP-CTRL-400.notCSV"), successCount);
                }

                try {
                    logger.info("Processing file: {}", fileName);
                    boolean success = inventoryService.writeFromProductFileToDb(file.toString());
                    if (success) {
                        successCount++;
                        logger.debug("File processed successfully: {}", fileName);
                    } else {
                        failureCount++;
                        logger.warn("File processing failed: {}", fileName);
                    }

                } catch (ProductServiceException e) {
                    logger.error("Service exception while processing file: {}", fileName, e);
                    return new Response<>(Constant.FAILURE, ErrorMessageUtil.getErrorMessage(e.getErrorCode()), successCount);
                }
            }

            if (successCount > 0 && failureCount == 0) {
                logger.info("All files processed successfully. Total = {}", successCount);
                return new Response<>(Constant.SUCCESS, "All Files Processed files successfully", successCount);
            } else if (successCount > 0) {
                logger.warn("Partial success. Success = {}, Failures = {}", successCount, failureCount);
                return new Response<>(Constant.PARTIAL_SUCCESS, " Processed " + successCount + " Files successfully", successCount);
            } else {
                logger.error("No files processed successfully.");
                return new Response<>(Constant.FAILURE, " NO FILES Processed ", successCount);
            }

        } catch (IOException e) {
            logger.fatal("I/O error occurred while reading folder: {}", folderPath, e);
            return new Response<>(Constant.FAILURE, "I/O error occurred while reading folder", successCount);
        } finally {
            logger.trace(LoggerConstants.EXIT_METHOD_LOG_MESSAGE, "csvtoDB()");
        }
    }
}


