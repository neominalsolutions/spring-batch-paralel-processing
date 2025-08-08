package com.mertalptekin.springbatchparalelprocessing.controller;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/batch")
public class BatchController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("simpleSplitJob")
    private Job simpleSplitJob;


    @PostMapping("/runSimpleSplitJob")
    public ResponseEntity<String> runSimpleSplitJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {

        var params = new JobParametersBuilder().addString("UUID", UUID.randomUUID().toString()).toJobParameters();

        jobLauncher.run(simpleSplitJob,params);

        // Logic to run the simple split job
        return ResponseEntity.ok("Simple Split Job has been started.");
    }

    @Autowired
    @Qualifier("simpleCustomerJob")
    private Job simpleCustomerJob;

    @PostMapping("/simpleCustomerJob")
    public ResponseEntity<String> runSimpleCustomerJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {

        var params = new JobParametersBuilder().addString("UUID", UUID.randomUUID().toString()).toJobParameters();

        jobLauncher.run(simpleCustomerJob,params);

        // Logic to run the simple split job
        return ResponseEntity.ok("Simple Split Job has been started.");
    }


    @Autowired
    @Qualifier("simplePartitionJob")
    private Job simplePartitionJob;

    @PostMapping("/runSimplePartitionJob")
    public ResponseEntity<String> runSimplePartitionJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {

        var params = new JobParametersBuilder().addString("UUID", UUID.randomUUID().toString()).toJobParameters();

        jobLauncher.run(simplePartitionJob,params);

        // Logic to run the simple split job
        return ResponseEntity.ok("Simple Split Job has been started.");
    }

    @Autowired
    @Qualifier("jdbcPagingItemReaderJob")
    private Job jdbcPagingItemReaderJob;

    @PostMapping("/runJdbcPagingItemReaderJob")
    public ResponseEntity<String> runJdbcItemReaderJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {

        var params = new JobParametersBuilder().addString("UUID", UUID.randomUUID().toString()).toJobParameters();

        jobLauncher.run(jdbcPagingItemReaderJob,params);

        // Logic to run the simple split job
        return ResponseEntity.ok("JdbcPagingItemReaderJob has been started.");
    }


}
