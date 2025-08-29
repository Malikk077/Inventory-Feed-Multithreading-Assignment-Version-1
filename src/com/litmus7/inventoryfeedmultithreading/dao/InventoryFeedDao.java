package com.litmus7.inventoryfeedmultithreading.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.litmus7.inventoryfeedmultithreading.constant.LoggerConstants;

import com.litmus7.inventoryfeedmultithreading.constant.SQLConstant;
import com.litmus7.inventoryfeedmultithreading.dto.Product;
import com.litmus7.inventoryfeedmultithreading.exception.ProductDataAccessException;
import com.litmus7.inventoryfeedmultithreading.util.DBUtil;

public class InventoryFeedDao {
	
	private static final Logger logger = LogManager.getLogger(InventoryFeedDao.class);
	
	public boolean batchInsertProducts(List<Product> products ) throws ProductDataAccessException{
		
		logger.trace(LoggerConstants.ENTER_METHOD_LOG_MESSAGE,"batchInsertProducts()");

		Connection connection = null;
		boolean success=false;
		try {
			connection = DBUtil.getConnection();
		    PreparedStatement stmt = connection.prepareStatement(SQLConstant.INSERT_INTO_PRODUCT );
			connection.setAutoCommit(false);
			logger.info("DB Connection Established Autocommit turned off");
			for(Product product: products ) {
				stmt.setInt(1, product.getSku());
			    stmt.setString(2, product.getProductName());
			    stmt.setDouble(3, product.getQuantity());
			    stmt.setDouble(4, product.getPrice());
			    stmt.addBatch();	
			    logger.debug("Prepared batch  for sku ID {} ",product.getSku() );
			 
			}
			
			int[] result = stmt.executeBatch();
			logger.info("Batch insert completed. Total rows affected: {}", Arrays.stream(result).sum());
	        logger.trace(LoggerConstants.EXITING_AFTER_SUCCESS_UPDATES_IN_DB,Arrays.stream(result).sum());
			boolean hasZero = Arrays.stream(result).anyMatch(n -> n == 0);
			if (!hasZero) {
				connection.commit();
				success=true;
				logger.info("All products transferred successfully to db ");
				
			}else {
				logger.warn("One or more Product failed to insert , Performing rollback.");
				connection.rollback();
			}
			
			stmt.close();
		}catch(SQLException e) {
			logger.error("Error during product batch transfer to db Exiting...");
			throw new ProductDataAccessException("PRD-DAO-500.batchtransactionfailure",e);
		}finally {
		        if (connection != null) {
		            try {
		                if (!connection.getAutoCommit()) {
		                    connection.setAutoCommit(true);
		                }
		                connection.close();
		                logger.debug("Database connection closed and auto-commit reset");
		            } catch (SQLException e) {
		            	logger.error("Error while resetting auto-commit or closing connection: {}", e.getMessage(), e);
		                throw new ProductDataAccessException("SQL-ERR-99999.unknown", e);
		            }
		        }
		    }
		    logger.trace("Exiting batchInsertProducts with result: {}", success);
		    return success;
		}
}