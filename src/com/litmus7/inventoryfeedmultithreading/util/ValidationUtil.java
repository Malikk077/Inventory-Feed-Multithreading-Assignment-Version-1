package com.litmus7.inventoryfeedmultithreading.util;
import com.litmus7.inventoryfeedmultithreading.dto.Product;

public class ValidationUtil {
	
	public static boolean isNonEmpty(String str) {
        return str != null && !str.trim().isEmpty();
    }
	
	public static boolean isValidPrice(Double salary) {
        return salary != null && salary >= 0;
    }
	
	public static boolean isValidQuantity(Double salary) {
        return salary != null && salary >= 0;
    }
	
	public static boolean validateProduct(Product product) {
        return product.getSku() > 0
            && isNonEmpty(product.getProductName())
            && isValidQuantity(product.getQuantity())
            && isValidPrice(product.getPrice());       
    }


}
