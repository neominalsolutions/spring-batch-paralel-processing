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


}
