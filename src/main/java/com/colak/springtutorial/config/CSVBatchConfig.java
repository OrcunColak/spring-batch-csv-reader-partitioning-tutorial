package com.colak.springtutorial.config;

import com.colak.springtutorial.dto.ProductDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.atomic.AtomicInteger;

// Spring Batch 4 to 5 migration breaking changes
// See https://levelup.gitconnected.com/spring-batch-4-to-5-migration-breaking-changes-9bac1c063dc5
@Configuration
@Slf4j
public class CSVBatchConfig {

    @Bean
    public Job job(JobRepository jobRepository, Step masterStep) {
        // We are now required to pass in JobRepository upon using JobBuilder
        return new JobBuilder("job", jobRepository)
                .start(masterStep)
                .listener(listener())
                .build();
    }

    @Bean
    public PartitionHandler partitionHandler(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setGridSize(4);
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(slaveStep(jobRepository, transactionManager));
        return taskExecutorPartitionHandler;
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setQueueCapacity(4);
        return taskExecutor;
    }

    @Bean
    JobExecutionListener listener() {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                JobExecutionListener.super.beforeJob(jobExecution);
                log.info("Before Job");
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                JobExecutionListener.super.afterJob(jobExecution);
                log.info("After Job");
            }
        };
    }

    @Bean
    public Step masterStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        Step slaveStep = slaveStep(jobRepository, transactionManager);
        PartitionHandler partitionHandler = partitionHandler(jobRepository, transactionManager);

        return new StepBuilder("masterSTep", jobRepository).
                partitioner(slaveStep.getName(), csvPartitioner())
                .partitionHandler(partitionHandler)
                .build();
    }

    @Bean
    public CSVPartitioner csvPartitioner() {
        return new CSVPartitioner();
    }

    @Bean
    public Step slaveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        AtomicInteger counter = new AtomicInteger();

        return new StepBuilder("step_first", jobRepository)
                .<ProductDTO, ProductDTO>chunk(4, transactionManager)
                .reader(csvReader())
                .writer(chunk -> chunk.forEach(item -> log.info("Counter : {} ProductDTO : {}", counter.incrementAndGet(), item)))
                .build();
    }

    @Bean
    public ItemReader<ProductDTO> csvReader() {
        ClassPathResource classPathResource = new ClassPathResource("product_catalog_full.csv");
        FlatFileItemReader<ProductDTO> flatFileItemReader = new FlatFileItemReaderBuilder<ProductDTO>()
                .name("csvReader")          // Reader name
                .resource(classPathResource)// Resource (CSV file)
                .linesToSkip(1)             // Skip the header line
                .delimited()                // Enable delimited tokenizer
                .strict(false)


                // Set names of FieldSet that we can use in MarketDataFieldSetMapper
                .names("productId", "productName", "productBrand", "price", "description")// Column names
                .fieldSetMapper(new ProductFieldSetMapper()) // Custom FieldSetMapper

                .build();

        SynchronizedItemStreamReader<ProductDTO> synchronizedItemStreamReader = new SynchronizedItemStreamReader<>();
        synchronizedItemStreamReader.setDelegate(flatFileItemReader);
        return synchronizedItemStreamReader;
    }

}