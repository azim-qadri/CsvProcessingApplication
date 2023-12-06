package com.example.csvprocessor.service;

import org.springframework.stereotype.Service;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class ProcessingStatusService {

    private final AtomicBoolean processing = new AtomicBoolean(false);

    public void startProcessing() {
        processing.set(true);
    }

    public void stopProcessing() {
        processing.set(false);
    }

    public boolean isProcessing() {
        return processing.get();
    }
}
