package com.litmus7.inventoryfeedmultithreading.util;

import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.*;

public class FileMoverUtil {

    private static final Logger logger = LogManager.getLogger(FileMoverUtil.class);

    /**
     * Moves a file safely to the target directory, creating parent directories if needed.
     *
     * @param source      Source file path
     * @param destination Destination file path
     * @param reason      Reason for the move (for logging)
     * @throws IOException if move fails
     */
    public static void moveFile(Path source, Path destination, String reason) throws IOException {
        try {
            Files.createDirectories(destination.getParent());
            Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
            logger.info("Moved file [{}] to [{}] due to {}", source.getFileName(), destination, reason);
        } catch (IOException e) {
            logger.error("Failed to move file [{}] to [{}] due to {}", source.getFileName(), destination, reason, e);
            throw e;
        }
    }
}
