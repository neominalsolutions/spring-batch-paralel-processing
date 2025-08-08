package com.mertalptekin.springbatchparalelprocessing;


import com.mertalptekin.springbatchparalelprocessing.model.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@EnableBatchProcessing
@Configuration
public class SimplePartitionJobConfig {


    @Bean
    @StepScope
    public JsonItemReader<Customer> jsonItemReader() {
        return new JsonItemReaderBuilder<Customer>().resource(new ClassPathResource("customers.json")).jsonObjectReader(new JacksonJsonObjectReader<>(Customer.class)).name("customerJsonItemReader").build();
    }
    @Bean
    public ItemProcessor<Customer, Customer> itemProcessor() {
        return item ->  {
            item.setLastName(item.getLastName().toUpperCase());
            return  item;
        };
    }
    @Bean
    public ItemWriter<Customer> itemWriter(DataSource dataSource) {
        JdbcBatchItemWriter<Customer> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("insert into customer (id,firstName, lastName, birthYear) values (?,?,?,?)");
        writer.setItemPreparedStatementSetter(new ItemPreparedStatementSetter<Customer>() {
            @Override
            public void setValues(Customer item, PreparedStatement ps) throws SQLException {
                ps.setInt(1, item.getId());
                ps.setString(2, item.getFirstName());
                ps.setString(3, item.getLastName());
                ps.setInt(4, item.getBirthYear());
            }
        });
        return writer;
    }

    // taskExecutor, paralel çalıştırmak için kullanılır. İşlemleri partition'lara ayırmak için paralel işleme yeteneği sağlar.
    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        taskExecutor.setThreadNamePrefix("taskExecutor-partition-");
        return taskExecutor;
    }

    // Partitionlara ayırmak için kullanılır. Bu, paralel işleme yeteneği sağlar.
    // paralel işlem yapmak için TaskExecutor sınıfını kullanılır.
    // setGridSize ile kaç adet partion oluşturulacağını belirleriz.
    // setStep hangi step partion ayrılacağını belirtir.
    @Bean
    public PartitionHandler partitionerHandler(Step mainStep) {
        TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();
        partitionHandler.setTaskExecutor(taskExecutor()); // Paralel çalıştırmak için taskExecutor kullanılır
        partitionHandler.setGridSize(4); // Partisyon sayısı, 25li olarak veriyi paralelde farklı threadlerde işleyeceğiz.
        partitionHandler.setStep(mainStep);

        return partitionHandler;
    }

    @Bean
    public Step mainStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,DataSource dataSource) {
        return new StepBuilder("mainStep",jobRepository).<Customer,Customer>chunk(10,transactionManager)
                .reader(jsonItemReader())
                .processor(itemProcessor())
                .writer(itemWriter(dataSource))
                .build();
    }

    // Partition yapılarında job normal olarak mainStep chunk bazlı çalışan step yerine partitiondan başlar. Partition ise mainStep'i partionlara ayırır.
    // Bölümleme işlemi yapan özel steplere partitionedStep denir. Bu yüzden burada chunk tanımlaması yapmıyoruz.
    @Bean
    public Step partitionedStep(JobRepository jobRepository,Step mainStep) {

        SimplePartitioner partioner = new SimplePartitioner();
        // partition hanfler kullanarak mainStep ismindeki step'i partionlara ayır.
        return  new StepBuilder("partitionStep",jobRepository)
                .partitioner("mainStep",partioner) // veritabanında hangi step ismi ile partion yapılyor.
                //.step(mainStep) // bu setepi partionlara böl
                .partitionHandler(partitionerHandler(mainStep))
                .build();
    }

    // Bean Step bazlı Job çalıştırmak için kullanılır
    @Bean(name = "simplePartitionJob")
    public Job simplePartitionJob(JobRepository jobRepository,Step partitionedStep) {
        return new JobBuilder("simplePartitionJob", jobRepository)
                .start(partitionedStep)
                .build();
    }


    // Bean Step bazlı Job çalıştırmak için kullanılır
    @Bean(name = "simpleCustomerJob")
    public Job simpleCustomerJob(JobRepository jobRepository,Step mainStep) {
        return new JobBuilder("simpleCustomerJob", jobRepository)
                .start(mainStep)
                .build();
    }



    // Other beans and configurations can be added here as needed








}
