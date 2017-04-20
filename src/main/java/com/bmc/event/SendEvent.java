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
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.jboss.resteasy.client.jaxrs.BasicAuthentication;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.engines.ApacheHttpClient4Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * @author Venkata.Karri
 */

public class SendEvent implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendEvent.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static AtomicInteger ROW_NUMBER = new AtomicInteger(1);
    private static Client CLIENT = getClient();
    	
    private static final int MAX_EVENT_SIZE = 32768;
    private static final int MAX_EVENT_FIELDS = 128;
    private static final String UTF_8 = "UTF-8";
    	
    private static final String TITLE = "title";
    private static final String SOURCE = "source";
    private static final String SENDER = "sender";
    private static final String REF = "ref";
    private static final String TYPE = "type";
    private static final String NAME = "name";
    private static final String SOURCE_NAME = "source.name";
    	
    private static final String SEVERITY = "severity";
    private static final String STATUS = "status";
    private static final String TAGS = "tags";
    private static final String MESSAGE = "message";
    private static final String CREATED_AT = "createdAt";
    private static final String EVENT_CLASS = "eventClass";
    private static final String FINGER_PRINT_FIELDS = "fingerprintFields";
    private static final String PROPERTIES = "properties";
    	
    private static final ImmutableSet<String> TAGGED_FINGERPRINTFIELDS = ImmutableSet.of(TITLE, MESSAGE, STATUS, SEVERITY, SOURCE_NAME);
    	
    private Row headerRow;
    private Row currentRow;
    private String url;
    private String email;
    private String apiKey;
	
    public SendEvent(String url, String email, String apiKey, Row headerRow, Row currentRow) {
        this.url = url;
        this.email = email;
        this.apiKey = apiKey;
        this.headerRow = headerRow;
        this.currentRow = currentRow;
    }
    	
    public static Client getClient() {
        Client client = null;
        try {
            HttpClient httpClient = HttpClients.custom().setMaxConnTotal(EventIngestion.THREAD_COUNT).setMaxConnPerRoute(EventIngestion.THREAD_COUNT).build();
            ApacheHttpClient4Engine engine = new ApacheHttpClient4Engine(httpClient);
            ExecutorService executor = Executors.newFixedThreadPool(EventIngestion.THREAD_COUNT);
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
        String payload = getPayload(headerRow, currentRow);
        if (payload != null) {
        	sendEvent(payload, currentRow.getRowNum());
        }
    }
    	
    private String getPayload(Row headerRow, Row currentRow) {
        String payload = null;
        try {
            ObjectNode payloadNode = MAPPER.createObjectNode();
            ObjectNode propertiesNode = MAPPER.createObjectNode();
            ObjectNode sourceNode = MAPPER.createObjectNode();
            Iterator<Cell> cellIterator = currentRow.cellIterator();
            int cellIndex = 0;
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
                        payloadNode.put(TITLE, cellValue);
                    } else if (SOURCE.equalsIgnoreCase(headerCell)) {
                        String[] sourceArray = cellValue.split(",");
                        if (sourceArray.length >= 1) {
                            String sourceRef = sourceArray[0].trim();
                            sourceNode.put(REF, sourceRef);
                        }
                        if (sourceArray.length >= 2) {
                            String sourceType = sourceArray[1].trim();
                            sourceNode.put(TYPE, sourceType);
                        }
                        if (sourceArray.length == 3) {
                            String sourceName = sourceArray[2].trim();
                            sourceNode.put(NAME, sourceName);
                        }
                        payloadNode.set(SOURCE, sourceNode);
                    } else if (SENDER.equalsIgnoreCase(headerCell)) {
                        String[] senderArray = cellValue.split(",");
                        ObjectNode senderNode = MAPPER.createObjectNode();
                        if (senderArray.length >= 1) {
                            String senderRef = senderArray[0].trim();
                            senderNode.put(REF, senderRef);
                        }
                        if (senderArray.length >= 2) {
                            String senderType = senderArray[1].trim();
                            senderNode.put(TYPE, senderType);
                        }
                        if (senderArray.length == 3) {
                            String senderName = senderArray[2].trim();
                            senderNode.put(NAME, senderName);
                        }
                        payloadNode.set(SENDER, senderNode);
                    } else if (FINGER_PRINT_FIELDS.equalsIgnoreCase(headerCell)) {
                        String[] fingerprintFieldsArray = cellValue.split(",");
                        ArrayNode fingerprintFieldsNode = MAPPER.createArrayNode();
                        Set<String> fingerprintFields = Sets.newHashSet(fingerprintFieldsArray);
                        for (String fingerprintField : fingerprintFields) {
                            String fingerprintFieldTrimmed = fingerprintField.trim();
                            String fingerprintFieldLowerCase = fingerprintField.toLowerCase();
                            if (TAGGED_FINGERPRINTFIELDS.contains(fingerprintFieldLowerCase)) {
                                fingerprintFieldTrimmed = String.format("@%s", fingerprintFieldTrimmed);
                            }
                            fingerprintFieldsNode.add(fingerprintFieldTrimmed);
                        }
                        payloadNode.set(FINGER_PRINT_FIELDS, fingerprintFieldsNode);
                    } else if (SEVERITY.equalsIgnoreCase(headerCell)) {
                        payloadNode.put(SEVERITY, cellValue);
                    } else if (STATUS.equalsIgnoreCase(headerCell)) {
                        payloadNode.put(STATUS, cellValue);
                    } else if (MESSAGE.equalsIgnoreCase(headerCell)) {
                        payloadNode.put(MESSAGE, cellValue);
                    } else if (CREATED_AT.equalsIgnoreCase(headerCell)) {
                        payloadNode.put(CREATED_AT, cellValue);
                    } else if (EVENT_CLASS.equalsIgnoreCase(headerCell)) {
                        payloadNode.put(EVENT_CLASS, cellValue);
                    } else if (TAGS.equalsIgnoreCase(headerCell)) {
                        String[] tagsArray = cellValue.split(",");
                        Set<String> tags = Sets.newHashSet(tagsArray);
                        ArrayNode tagsNode = MAPPER.createArrayNode();
                        for (String tag :tags) {
                            String tagTrimmed = tag.trim();
                            tagsNode.add(tagTrimmed);
                        }
                        payloadNode.put(TAGS, cellValue);
                    } else {
                        propertiesNode.put(headerCell, cellValue);
                    }
                }
            }
            if (!(sourceNode.size() >= 2 && payloadNode.has(FINGER_PRINT_FIELDS) && payloadNode.has(TITLE))) {
                LOGGER.error("Mandatory Fields i.e title, source.ref, source.type and/or fingerprintfields missing in the row [{}]: ", currentRow.getRowNum()+1);
                return null;
            }
            if (propertiesNode.size() > MAX_EVENT_FIELDS) {
                LOGGER.error("Event properties field count of [{}] exceeds maximum of [{}] for row [{}]", propertiesNode.size(), MAX_EVENT_FIELDS, currentRow.getRowNum()+1);
                return null;
            }
            if (propertiesNode.size() > 0) {
                payloadNode.set(PROPERTIES, propertiesNode);
            }
            payload = payloadNode.toString();
            int payloadBytes = payload.getBytes(UTF_8).length;
            if (payloadBytes > MAX_EVENT_SIZE) {
                LOGGER.error("Request size [{}] bytes too large, must be under [{}] bytes for row [{}] ", payloadBytes, MAX_EVENT_SIZE, currentRow.getRowNum()+1);
                return null;
            }
            LOGGER.debug("row [{}] payload: [{}]", currentRow.getRowNum(), payload);
        } catch (Exception e) {
            LOGGER.error("Error parsing the row [{}]: ", currentRow.getRowNum()+1, e);
        }
        return payload;
    }
    	
    private static String escape(String value) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch(ch) {
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


