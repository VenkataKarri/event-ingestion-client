package com.bmc.event;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.monitorjbl.xlsx.StreamingReader;

/**
 * @author Venkata.Karri
 */

public class EventIngestion {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(EventIngestion.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public static final int THREAD_COUNT = 50;
    public static final int ROW_CACHE_SIZE = 100;
    public static final int BUFFER_SIZE = 4096;
    	
    public static void main(String args[]) throws Exception {
        String url = args[0];
        String email = args[1];
        String apiKey = args[2];
        String filePath = args[3];
        LOGGER.info("Started: [{}]", sdf.format(new Date(System.currentTimeMillis())));
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        try (InputStream is = new FileInputStream(new File(filePath));
                Workbook workbook = StreamingReader.builder().rowCacheSize(ROW_CACHE_SIZE).bufferSize(BUFFER_SIZE).open(is);) {
            Sheet sheet = workbook.getSheetAt(0);
            Row headerRow = null;
            int count = 0;
            for (Row currentRow : sheet) {
                if (currentRow.getRowNum() == 0) {
                    headerRow = currentRow;
                    continue;
                }
                SendEvent worker = new SendEvent(url, email, apiKey, headerRow, currentRow);
                executor.execute(worker);
                count++;
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            LOGGER.info("Finished at [{}] processed [{}] rows", sdf.format(new Date(System.currentTimeMillis())), count);
        } catch (Exception e) {
            LOGGER.error("Error reading excel", e);
        }
    }
}


