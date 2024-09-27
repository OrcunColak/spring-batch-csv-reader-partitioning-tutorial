package com.colak.springtutorial.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CSVPartitioner implements Partitioner {

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        // Define the range of the data
        int min = 1;

        // Hardcoded max value. Ideally, this should be calculated dynamically based on the actual CSV file
        int max = 100; // Total records in the CSV file

        // Calculate the size of each partition
        int targetSize = (max - min + 1) / gridSize; // +1 ensures we don't miss any records

        // Map to store the partitions
        Map<String, ExecutionContext> result = new HashMap<>();

        // Initialize the partition index and starting point
        int partitionIndex = 0;
        int currentStart = min;
        int currentEnd = currentStart + targetSize - 1;

        // Iterate and create partitions
        while (currentStart <= max) {
            // Create a new ExecutionContext for the current partition
            ExecutionContext executionContext = new ExecutionContext();
            result.put("partition" + partitionIndex, executionContext);

            // Ensure the last partition ends at 'max'
            if (currentEnd > max) {
                currentEnd = max;
            }

            // Store the partition details in the ExecutionContext
            executionContext.putInt("minValue", currentStart);
            executionContext.putInt("maxValue", currentEnd);
            log.info("Partition {}: start={}, end={}", partitionIndex, currentStart, currentEnd);

            // Update the start point for the next partition
            currentStart = currentEnd + 1;
            currentEnd = currentStart + targetSize - 1;
            partitionIndex++;
        }

        return result;
    }
}
