package com.litmus7.inventoryfeedmultithreading.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import com.litmus7.inventoryfeedmultithreading.exception.ProductDataAccessException;
import com.litmus7.inventoryfeedmultithreading.exception.ProductServiceException;

public class CsvUtil {
	
	
	public static List<String[]> readCSV(String file) throws ProductDataAccessException{
    
    List<String[]> data = new ArrayList<>();
   
    try {
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);

        String line;
        br.readLine(); // Skip header line
        

        while ((line = br.readLine()) != null) {
            String[] values = line.split(",");
            data.add(values);
        }

        br.close();

    } catch (IOException e) {
    	throw new ProductDataAccessException("PRD-SVC-500.fileRead",e);
    }

    return data;
 }


}
