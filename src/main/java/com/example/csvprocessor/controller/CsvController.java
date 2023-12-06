package com.example.csvprocessor.controller;

import com.example.csvprocessor.service.CsvService;
import com.example.csvprocessor.service.DatabaseService;
import com.example.csvprocessor.service.ProcessingStatusService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.ui.Model;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller
@RequestMapping("/api/csv")
public class CsvController {

    @Autowired
    private CsvService csvService;

    @Autowired
    private ProcessingStatusService statusService;

    @Autowired
    private DatabaseService databaseService;

    @GetMapping("/upload")
    public String showUploadForm(Model model) {
        List<String> tableNames = databaseService.getExistingTableNames();
        model.addAttribute("tables", tableNames);
        return "upload";
    }

    @PostMapping("/process")
    public String handleFileUpload(@RequestParam("files") MultipartFile[] files,
                                   @RequestParam("tableName") String tableName, Model model) {
        String uploadsDir = "uploads";
        createUploadDirectory(uploadsDir);

        List<String> filePaths = new ArrayList<>();
        String outputFilePath = "uploads/combined_output"+tableName.toUpperCase()+".csv";

        try {
            for (MultipartFile file : files) {
                String fileName = file.getOriginalFilename();
                Path path = Paths.get(uploadsDir + File.separator + fileName);
                Files.copy(file.getInputStream(), path, StandardCopyOption.REPLACE_EXISTING);
                filePaths.add(path.toString());
            }

            csvService.combineAndWriteCsvFiles(filePaths, outputFilePath);
            Set<String> headers = csvService.readCsvHeaders(outputFilePath);
            List<Map<String, String>> data = csvService.readCsvData(outputFilePath);
            databaseService.createTableAndInsertData(tableName, headers, data);
            return "success";
        } catch (IOException e) {
            model.addAttribute("error", "Error processing CSV data: " + e.getMessage());
            return "error";
        }
    }

    private void createUploadDirectory(String directoryPath) {
        File uploadDir = new File(directoryPath);
        if (!uploadDir.exists() && !uploadDir.mkdirs()) {
            throw new RuntimeException("Failed to create directory: " + directoryPath);
        }
    }

    @GetMapping("/status")
    @ResponseBody
    public String getProcessingStatus(Model model) {
        boolean isProcessing = statusService.isProcessing();
        return isProcessing ? "Processing is ongoing" : "Processing is complete";
    }
}
