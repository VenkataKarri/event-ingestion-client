package com.bmc.event;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.jboss.resteasy.client.jaxrs.BasicAuthentication;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.engines.ApacheHttpClient4Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * @author Venkata.Karri
 */

public class SendEvent implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(SendEvent.class);
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	public static AtomicInteger ATOMIC_INTEGER = new AtomicInteger(1);
	private static Client CLIENT = getClient();
	
	private static final int MAX_EVENT_SIZE = 32768;
	private static final String UTF_8 = "UTF-8";
	
	private static final String TITLE = "title";
	private static final String SOURCE = "source";
	private static final String SENDER = "sender";
	private static final String REF = "ref";
	private static final String TYPE = "type";
	private static final String NAME = "name";
	
	private static final String SEVERITY = "severity";
	private static final String STATUS = "status";
	private static final String TAGS = "tags";
	private static final String MESSAGE = "message";
	private static final String CREATED_AT = "createdAt";
	private static final String EVENT_CLASS = "eventClass";
	private static final String FINGER_PRINT_FIELDS = "fingerprintFields";
	private static final String PROPERTIES = "properties";
	
	private static final ImmutableSet<String> TAGGED_FINGERPRINTFIELDS = ImmutableSet.of(TITLE, MESSAGE, STATUS, SEVERITY);
	
	private XSSFSheet xssfSheet;
	private String url;
	private String email;
	private String apiKey;
	
	public SendEvent(String url, String email, String apiKey, XSSFSheet xssfSheet) {
		this.url = url;
		this.email = email;
		this.apiKey = apiKey;
		this.xssfSheet = xssfSheet;
	}
	
	public static Client getClient() {
		Client client = null;
        try {
            HttpClient httpClient = HttpClients.custom().setMaxConnTotal(100).setMaxConnPerRoute(100).build();
            ApacheHttpClient4Engine engine = new ApacheHttpClient4Engine(httpClient);
            ExecutorService executor = Executors.newFixedThreadPool(100);
            client = new ResteasyClientBuilder().httpEngine(engine).asyncExecutor(executor).build();
        } catch (Exception e) {
        	LOGGER.error("Error creating all trusting client: ", e);
            throw new RuntimeException(e);
        } 
        return client;
	}
	
	private void sendEvent(String payload, int rownum) {
		Response response = null;
		WebTarget target = null;
		long createdAt = System.currentTimeMillis();
		try {
			target = CLIENT.target(url).register(new BasicAuthentication(email, apiKey));
            response = target.request().post(Entity.entity(payload, MediaType.APPLICATION_JSON));
	        int status = response.getStatus();
	        if (status == Response.Status.ACCEPTED.getStatusCode()) {
	        	LOGGER.debug("Successfully sent event at time: [{}]", sdf.format(new Date(createdAt)));
            } else {
            	String errorMsg = response.readEntity(String.class);
            	LOGGER.error("Failed for Row [{}]: HTTP error code : [{}] at time: [{}] with error msg: [{}]", rownum+1 , response.getStatus(), sdf.format(new Date(createdAt)), errorMsg);
            }
		} catch (Exception e) {
			LOGGER.error("Error creating event for Row [{}] at time [{}]: ", rownum+1, sdf.format(new Date(createdAt)), e);
		} finally {
			if (response != null) {
                response.close();
            }
		}
	}

	@Override
	public void run() {
		int count = 0;
		XSSFRow headerRow = xssfSheet.getRow(0);
		while (true) {
			int rownum = ATOMIC_INTEGER.get();
			XSSFRow currentRow = xssfSheet.getRow(rownum);
			if (currentRow == null) {
				LOGGER.debug("Thread count: [{}]", count);
				break;
			}
			rownum = ATOMIC_INTEGER.getAndIncrement();
			currentRow = xssfSheet.getRow(rownum);
		    String payload = getPayload(headerRow, currentRow);
		    if (payload != null) {
		    	sendEvent(payload, rownum);
		    }
			count++;
		}
	}
	
	private String getPayload(XSSFRow headerRow, XSSFRow currentRow) {
		String payload = null;
		try {
			StringBuilder payloadBuilder = new StringBuilder("{");
			Iterator<Cell> cellIterator = currentRow.cellIterator();
			int cellIndex = 0;
			int mandatoryFields = 0;
			StringBuilder propertiesBuilder = new StringBuilder();
			while(cellIterator.hasNext()) {
				Cell cell = cellIterator.next();
				if (cell != null) {
					cellIndex = cell.getColumnIndex();
					String cellValue;
					if(cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
						cellValue = NumberToTextConverter.toText(cell.getNumericCellValue()).trim();
					} else {
						cellValue = escape(cell.getStringCellValue().trim());
					}
					if (cellValue.isEmpty()) {
						continue;
					}
					String headerCell = headerRow.getCell(cellIndex).getStringCellValue();
					if (TITLE.equalsIgnoreCase(headerCell)) {
						payloadBuilder.append("\"").append(TITLE).append("\": \"").append(cellValue).append("\",");
						mandatoryFields++;
					} else if (SOURCE.equalsIgnoreCase(headerCell)) {
						String[] sourceArray = cellValue.split(",");
						String sourceRef = sourceArray[0].trim();
						String sourceType = sourceArray[1].trim();
						payloadBuilder.append("\"").append(SOURCE).append("\": {")
			                .append("\"").append(REF).append("\": \"").append(sourceRef).append("\",")
			                .append("\"").append(TYPE).append("\": \"").append(sourceType).append("\"");
					    if (sourceArray.length == 3) {
					    	String sourceName = sourceArray[2].trim();
					    	payloadBuilder.append(",\"").append(NAME).append("\": \"").append(sourceName).append("\"");
					    }
					    payloadBuilder.append("},");
					    mandatoryFields++;
					} else if (SENDER.equalsIgnoreCase(headerCell)) {
						String[] senderArray = cellValue.split(",");
						payloadBuilder.append("\"").append(SENDER).append("\": {");
						if (senderArray.length >= 1) {
							String senderRef = senderArray[0].trim();
							payloadBuilder.append("\"").append(REF).append("\": \"").append(senderRef).append("\",");
						}
						if (senderArray.length >= 2) {
							String senderType = senderArray[1].trim();
							payloadBuilder.append("\"").append(TYPE).append("\": \"").append(senderType).append("\",");
						}
					    if (senderArray.length == 3) {
					    	String senderName = senderArray[2].trim();
					    	payloadBuilder.append("\"").append(NAME).append("\": \"").append(senderName).append("\",");
					    }
					    payloadBuilder.deleteCharAt(payloadBuilder.length() - 1).append("},");
					} else if (FINGER_PRINT_FIELDS.equalsIgnoreCase(headerCell)) {
						String[] fingerprintFieldsArray = cellValue.split(",");
						Set<String> fingerprintFields = Sets.newHashSet(fingerprintFieldsArray);
						payloadBuilder.append("\"").append(FINGER_PRINT_FIELDS).append("\": [");
				        for (String fingerprintField : fingerprintFields) {
				        	String fingerprintFieldTrimmed = fingerprintField.trim();
				        	String fingerprintFieldLowerCase = fingerprintField.toLowerCase();
				        	if (TAGGED_FINGERPRINTFIELDS.contains(fingerprintFieldLowerCase)) {
				        		fingerprintFieldTrimmed = String.format("@%s", fingerprintFieldTrimmed);
				        	}
				        	payloadBuilder.append("\"").append(fingerprintFieldTrimmed).append("\",");
				        }
				        payloadBuilder.deleteCharAt(payloadBuilder.length() - 1);
				        payloadBuilder.append("],");
				        mandatoryFields++;
					} else if (SEVERITY.equalsIgnoreCase(headerCell)) {
						payloadBuilder.append("\"").append(SEVERITY).append("\": \"").append(cellValue).append("\",");
					} else if (STATUS.equalsIgnoreCase(headerCell)) {
						payloadBuilder.append("\"").append(STATUS).append("\": \"").append(cellValue).append("\",");
					} else if (MESSAGE.equalsIgnoreCase(headerCell)) {
						payloadBuilder.append("\"").append(MESSAGE).append("\": \"").append(cellValue).append("\",");
					} else if (CREATED_AT.equalsIgnoreCase(headerCell)) {
						payloadBuilder.append("\"").append(CREATED_AT).append("\": \"").append(cellValue).append("\",");
					} else if (EVENT_CLASS.equalsIgnoreCase(headerCell)) {
						payloadBuilder.append("\"").append(EVENT_CLASS).append("\": \"").append(cellValue).append("\",");
					} else if (TAGS.equalsIgnoreCase(headerCell)) {
						String[] tagsArray = cellValue.split(",");
						Set<String> tags = Sets.newHashSet(tagsArray);
						payloadBuilder.append("\"").append(TAGS).append("\": [");
						for (String tag :tags) {
							String tagTrimmed = tag.trim();
							payloadBuilder.append("\"").append(tagTrimmed).append("\",");
						}
						payloadBuilder.deleteCharAt(payloadBuilder.length() - 1);
						payloadBuilder.append("],");
					} else {
						if (propertiesBuilder.length() == 0) {
							propertiesBuilder.append("\"").append(PROPERTIES).append("\": {");
						}
						propertiesBuilder.append("\"").append(headerCell).append("\":")
						    .append("\"").append(cellValue).append("\",");
					}
				}
			}
			if (propertiesBuilder.length() != 0) {
				propertiesBuilder.deleteCharAt(propertiesBuilder.length() - 1).append("}");
				payloadBuilder.append(propertiesBuilder.toString());
			}
			payloadBuilder.append("}");
			payload = payloadBuilder.toString();
			int payloadBytes = payload.getBytes(UTF_8).length;
			if (payloadBytes > MAX_EVENT_SIZE) {
				LOGGER.error("Request size [{}] bytes too large, must be under 32768 bytes for row [{}] ", payloadBytes, currentRow.getRowNum()+1);
				return null;
			}
			if (mandatoryFields != 3) {
				LOGGER.error("Mandatory Fields i.e title, source and/or fingerprintfields are missing in the row [{}]: ", currentRow.getRowNum()+1);
				return null;
			}
			LOGGER.debug("payload: [{}]", payload);
		} catch (Exception e) {
			LOGGER.error("Error parsing the row [{}]: ", currentRow.getRowNum()+1, e);
		}
		return payload;
	}
	
	private static String escape(String value) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < value.length(); i++){
			char ch = value.charAt(i);
			switch(ch){
			case '"':
				sb.append("\\\"");
				break;
			case '\\':
				sb.append("\\\\");
				break;
			case '\b':
				sb.append("\\b");
				break;
			case '\f':
				sb.append("\\f");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\t':
				sb.append("\\t");
				break;
			case '/':
				sb.append("\\/");
				break;
			default:
				sb.append(ch);
			}
		}
		return sb.toString();
	}
}


