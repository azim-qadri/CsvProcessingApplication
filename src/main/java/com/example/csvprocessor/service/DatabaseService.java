package com.example.csvprocessor.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

@Service
public class DatabaseService {
    @Autowired
    private CsvService csvService;

    private final JdbcTemplate jdbcTemplate;

    public DatabaseService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void writeTableToCsv(String tableName, String csvFilePath) throws IOException {
        try (Writer writer = new PrintWriter(csvFilePath)) {
            jdbcTemplate.query("SELECT * FROM " + tableName.toUpperCase(), (ResultSetExtractor<Void>) rs -> {
                try {
                    CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(rs));
                    while (rs.next()) {
                        int columnCount = rs.getMetaData().getColumnCount();
                        String[] rowData = new String[columnCount];
                        for (int i = 0; i < columnCount; i++) {
                            rowData[i] = rs.getString(i + 1);
                        }
                        csvPrinter.printRecord((Object[]) rowData);
                    }
                    csvPrinter.flush();
                    return null;
                } catch (IOException e) {
                    throw new SQLException("Error writing to CSV", e);
                }
            });
        }
    }
    public boolean doesTableExist(String tableName) {
        try {
            jdbcTemplate.queryForObject("SELECT 1 FROM " + tableName.toUpperCase() + " LIMIT 1", Integer.class);
            return true;
        } catch (DataAccessException e) {
            return false;
        }
    }
    public void createTableAndInsertData(String tableName, Set<String> columns, List<Map<String, String>> data) {
        createTable(tableName, columns, data);

    }

    private void createTable(String tableName, Set<String> columns, List<Map<String, String>> data) {
        StringJoiner columnDefinitions = new StringJoiner(", ");
        if (doesTableExist(tableName)) {
            try {
                writeTableToCsv(tableName.toUpperCase(), "uploads/existing"+tableName.toUpperCase()+".csv");
            } catch (IOException e) {
                e.printStackTrace();
            }
            String sql = "DROP TABLE IF EXISTS " + tableName.toUpperCase();
            jdbcTemplate.execute(sql);
            List<String> filePaths = new ArrayList<>();
            filePaths.add("uploads/existing"+tableName.toUpperCase()+".csv");
            filePaths.add("uploads/combined_output"+tableName.toUpperCase()+".csv");
            csvService.combineAndWriteCsvFiles(filePaths,"uploads/combined_output"+tableName.toUpperCase()+".csv");
            Path path = Paths.get("uploads/existing"+tableName.toUpperCase()+".csv");

            try {
                Files.delete(path);
                System.out.println("Existing deleted successfully");
            } catch (IOException e) {
                System.out.println("Failed to delete existing: " + e.getMessage());
            }
            try{
                columns = csvService.readCsvHeaders("uploads/combined_output"+tableName.toUpperCase()+".csv");
                data = csvService.readCsvData("uploads/combined_output"+tableName.toUpperCase()+".csv");
            }catch (Exception e){
                System.out.print("Error reading headers combined "+ e.getMessage());
            }



        }
        columns.forEach(column -> columnDefinitions.add("\"" + column + "\" VARCHAR(255)"));
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName.toUpperCase() + " (" + columnDefinitions + ")";
        jdbcTemplate.execute(sql);
        insertData(tableName, columns, data);
    }

    private void insertData(String tableName, Set<String> columns, List<Map<String, String>> data) {
        for (Map<String, String> rowData : data) {
            StringJoiner columnNames = new StringJoiner(", ");
            List<Object> values = new ArrayList<>();

            for (String column : columns) {
                columnNames.add("\"" + column + "\"");
                values.add(rowData.getOrDefault(column, ""));
            }

            String sql = "INSERT INTO \"" + tableName.toUpperCase() + "\" (" + columnNames.toString() + ") VALUES ("
                    + String.join(", ", Collections.nCopies(values.size(), "?")) + ")";

            jdbcTemplate.update(sql, values.toArray());
        }
    }

    public List<String> getExistingTableNames() {
        String sql = "SHOW TABLES;";
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("TABLE_NAME"));
    }

}
