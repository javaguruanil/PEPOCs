package com.nisum;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.nisum.constants.ConstantsForCsvAndJson;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class CsvToJson {

    private final static Log logger = LogFactory.getLog(CsvToJson.class);

    public void processCsvToJson() throws Exception {
        logger.info("Start:: CsvToJson.processCsvToJson()");
        Optional<List<String[]>> allData = null;
        Optional<List<String>> headersList = null;
        try {
            allData = readDataFromCSVFile();
            headersList = readHeadersData();
            constructJsonFromCSV(headersList, allData);
        } catch (Exception exception) {
            logger.error("Error during CsvToJson.processCsvToJson() , error msg is :", exception);
            throw exception;
        }
        logger.info("End:: CsvToJson.processCsvToJson()");
    }

    private Optional<List<String[]>> readDataFromCSVFile() throws IOException {
        FileReader filereader = null;
        CSVReader csvReader = null;
        Optional<List<String[]>> entireCsvData = null;

        try {
            filereader = new FileReader(ConstantsForCsvAndJson.CSV_FILE_LOCATION);
            csvReader = new CSVReaderBuilder(filereader).withSkipLines(ConstantsForCsvAndJson.TWO).build();
            entireCsvData = Optional.ofNullable(csvReader.readAll());
        } catch (IOException ioException) {
            logger.error("Error during CsvToJson.readDataFromCSVFile() , error msg is :  ", ioException);
            throw ioException;
        }
        return entireCsvData;
    }


    private Optional<List<String>> readHeadersData() throws IOException {
        FileReader filereader = null;
        CSVReader csvReader = null;
        Optional<List<String>> entireHeadersData = null;
        BufferedReader br = null;
        Optional<String> rawHeaders = null;

        try {
            filereader = new FileReader(ConstantsForCsvAndJson.HEADERS_FILE_LOCATION);
            br = new BufferedReader(filereader);
            rawHeaders = Optional.ofNullable(br.readLine());
        } catch (IOException ioException) {
            logger.error("Error during CsvToJson.readHeadersData() , error msg is :  ", ioException);
            throw ioException;
        }
        return rawHeaders.flatMap(headersFromFile -> Optional.ofNullable(headersFromFile.split(ConstantsForCsvAndJson.COMA_DELIMITER))).flatMap(stringArray -> Optional.ofNullable(Arrays.asList(stringArray)));
    }


    private void constructJsonFromCSV(Optional<List<String>> optionalHeadersList, Optional<List<String[]>> allData) throws IOException {
        JsonFactory factory = null;
        FileWriter jsonObjectWriter = null;

        try {
            factory = new JsonFactory();
            jsonObjectWriter = new FileWriter(new File(ConstantsForCsvAndJson.JSON_FILE_LOCATION));
            JsonGenerator generator = factory.createGenerator(jsonObjectWriter);
            List<String> headersList = optionalHeadersList.orElseThrow(() -> new IOException("No data in Headers file to construct JSON headers"));
            generator.useDefaultPrettyPrinter();
            generator.writeStartObject();
            generator.writeFieldName(ConstantsForCsvAndJson.JSON_ROOT_ELEMENT);
            generator.writeStartArray();
            allData.orElseThrow(() -> new IOException("No data from CSV file to construct JSON file"));
            allData.ifPresent(
                    listString -> {
                        listString.forEach(
                                strArray -> {
                                    try {
                                        generator.writeStartObject();
                                        for (int i = ConstantsForCsvAndJson.ZERO; i < strArray.length; i++) {
                                            generator.writeFieldName(headersList.get(i));
                                            generator.writeString(strArray[i]);
                                        }
                                        generator.writeEndObject();
                                    } catch (IOException ioException) {
                                        logger.error("Error during CsvToJson.constructJsonFromCSV() for key and value attributes construction, error msg is :", ioException);
                                    }
                                }

                        );
                    }
            );
            generator.writeEndArray();
            generator.writeEndObject();
            generator.close();
        } catch (IOException ioException) {
            logger.error("Error during CsvToJson.constructJsonFromCSV() , error msg is :", ioException);
            throw ioException;
        }
    }
}