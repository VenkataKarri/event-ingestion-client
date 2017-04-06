package com.bmc.event;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Venkata.Karri
 */

public class EventIngestion {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(EventIngestion.class);
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	public static void main(String args[]) throws Exception {
		String url = args[0];
		String email = args[1];
		String apiKey = args[2];
		String filePath = args[3];
		LOGGER.info("Started: [{}]", sdf.format(new Date(System.currentTimeMillis())));
		ExecutorService executor = Executors.newFixedThreadPool(100);
        File file = null;
        FileInputStream fis =  null;
        XSSFWorkbook xssfWorkbook = null;
        XSSFSheet xssfSheet = null;
        try {
        	file = new File(filePath); 
        	fis = new FileInputStream(file); 
        	xssfWorkbook = new XSSFWorkbook (fis); 
        	xssfSheet = xssfWorkbook.getSheetAt(0); 
        	for (int i=1; i<=100; i++) {
        		SendEvent worker = new SendEvent(url, email, apiKey, xssfSheet);
        		executor.execute(worker);
        	}
        	executor.shutdown();
        	while(!executor.isTerminated()) {
        		
        	}
        	LOGGER.info("Finished at [{}] processed [{}] rows", sdf.format(new Date(System.currentTimeMillis())), SendEvent.ATOMIC_INTEGER.get()-1);
        } catch (Exception e) {
        	LOGGER.error("Error reading excel", e);
        } finally {
        	xssfWorkbook.close();
        	fis.close();
        }
	}
}


