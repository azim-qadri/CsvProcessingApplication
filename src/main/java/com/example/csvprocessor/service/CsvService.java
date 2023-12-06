package com.example.csvprocessor.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class CsvService {

    public void combineAndWriteCsvFiles(List<String> filePaths, String outputFilePath) {
        List<CompletableFuture<List<Map<String, String>>>> futureList = filePaths.stream()
                .map(this::readCsvFile)
                .collect(Collectors.toList());

        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]));

        allDoneFuture.thenRun(() -> {
            List<List<Map<String, String>>> results = futureList.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());

            List<Map<String, String>> combined = combineRows(results);
            try {
                writeCombinedCsv(outputFilePath, combined);
                System.out.println("Combined CSV file created successfully.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).join();
    }

    private List<Map<String, String>> combineRows(List<List<Map<String, String>>> csvLists) {
        int maxSize = csvLists.stream().mapToInt(List::size).max().orElse(0);
        List<Map<String, String>> combined = new ArrayList<>();

        for (int i = 0; i < maxSize; i++) {
            Map<String, String> combinedRow = new LinkedHashMap<>();
            for (List<Map<String, String>> csvList : csvLists) {
                if (i < csvList.size()) {
                    combinedRow.putAll(csvList.get(i));
                }
            }
            combined.add(combinedRow);
        }
        return combined;
    }

    private CompletableFuture<List<Map<String, String>>> readCsvFile(String filePath) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, String>> records = new ArrayList<>();
            try (CSVParser csvParser = new CSVParser(new FileReader(filePath), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
                for (CSVRecord record : csvParser) {
                    records.add(record.toMap());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return records;
        });
    }

    private void writeCombinedCsv(String outputFilePath, List<Map<String, String>> combinedData) throws IOException {
        Set<String> headers = combinedData.stream()
                .flatMap(map -> map.keySet().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        try (Writer writer = Files.newBufferedWriter(Paths.get(outputFilePath));
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])))) {
            for (Map<String, String> record : combinedData) {
                csvPrinter.printRecord(headers.stream().map(header -> record.getOrDefault(header, "")).toArray());
            }
        }
    }

    public List<Map<String, String>> readCsvData(String filePath) throws IOException {
        try (CSVParser csvParser = new CSVParser(new FileReader(filePath), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            return csvParser.getRecords().stream()
                    .map(CSVRecord::toMap)
                    .collect(Collectors.toList());
        }
    }

    public Set<String> readCsvHeaders(String filePath) throws IOException {
        try (CSVParser csvParser = new CSVParser(new FileReader(filePath), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            return csvParser.getHeaderMap().keySet();
        }
    }

}
