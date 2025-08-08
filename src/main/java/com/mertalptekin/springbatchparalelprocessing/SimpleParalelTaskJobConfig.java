package com.mertalptekin.springbatchparalelprocessing;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@EnableBatchProcessing
@Configuration
public class SimpleParalelTaskJobConfig {

    private static final Logger logger = LoggerFactory.getLogger(SimpleParalelTaskJobConfig.class);

    @Bean
    public Tasklet tasklet1() {
        return (contribution, chunkContext) -> {
            System.out.println("Tasklet 1 is running");
            logger.info("Tasklet 1 Thread Name :" + Thread.currentThread().getName());
            Thread.sleep(2000); // Simulate some work
            return null;
        };
    }
    @Bean
    public Tasklet tasklet2() {
        return (contribution, chunkContext) -> {
            System.out.println("Tasklet 2 is running");
            logger.info("Tasklet 2 Thread Name :" + Thread.currentThread().getName());
            Thread.sleep(2000); // Simulate some work
            return null;
        };
    }
    @Bean
    public Tasklet tasklet3() {
        return (contribution, chunkContext) -> {
            System.out.println("Tasklet 3 is running");
            logger.info("Tasklet 3 Thread Name :" + Thread.currentThread().getName());
            Thread.sleep(2000); // Simulate some work
            return null;
        };
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step1",jobRepository)
                .tasklet(tasklet1(), transactionManager)
                .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step2",jobRepository)
                .tasklet(tasklet2(), transactionManager)
                .build();
    }

    @Bean
    public Step step3(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step3",jobRepository)
                .tasklet(tasklet3(), transactionManager)
                .build();
    }

    @Bean
    public Flow flow1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new FlowBuilder<SimpleFlow>("flow1").start(step1(jobRepository,transactionManager)).build();
    }

    // Flow2 Flow1den ayırarak paralel bir akış olarak çalıştırmak istiyoruz
    // Split işlemi için farklı akışlar içerisine süreçlerimizi almamız lazım
    @Bean
    public Flow flow2(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new FlowBuilder<SimpleFlow>("flow2").start(step2(jobRepository,transactionManager)).next(step3(jobRepository,transactionManager)).build();
    }

    @Bean(name = "simpleSplitJob")
    public Job splitFlowJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("splitFlowJob", jobRepository)
                .start(flow1(jobRepository, transactionManager))
                .split(new SimpleAsyncTaskExecutor()).add(flow2(jobRepository, transactionManager))
                //.split(new SimpleAsyncTaskExecutor()).add()
                .build().build(); // JobBuilder build
    }




}
