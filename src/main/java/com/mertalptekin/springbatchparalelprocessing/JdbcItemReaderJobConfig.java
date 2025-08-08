package com.mertalptekin.springbatchparalelprocessing;

import com.mertalptekin.springbatchparalelprocessing.model.EmployeeEntitlement;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@EnableBatchProcessing
@Configuration
public class JdbcItemReaderJobConfig {

    // Not: JdbcCursorItemReader kullanarak veritabanından veri okuma işlemi yapılacak.
    // Bu yapıda daha küçük veri kümleri tercih edilir.

    // Not:JdbcPagingItemReader kullanarak veritabanından veri okuma işlemi yapılacak.
    // Bu yapıda daha büyük veri kümleri tercih edilir.

    // Normal SELECT Sorgusu ile bir örnek bide view ile bir örnek yapabiliriz.




    @Bean
    public PagingQueryProvider pagingQueryProvider(DataSource dataSource) throws Exception {
        SqlPagingQueryProviderFactoryBean factoryBean = new SqlPagingQueryProviderFactoryBean();
        factoryBean.setDataSource(dataSource);
        factoryBean.setSelectClause("select employee_id, _month,_year,total_bonus, total_deduction, total_prepaid_amount, total_income, calculation_date");
        factoryBean.setFromClause("from employee_entitlement_vm");
//        factoryBean.setSelectClause("SELECT " +
//                "es.employee_id, " +
//                "CAST(EXTRACT(MONTH FROM es.date) AS INT) AS _month, " +
//                "CAST(EXTRACT(YEAR FROM es.date) AS INT) AS _YEAR, " +
//                "SUM(CASE WHEN es.type = 'Prim' THEN es.total ELSE 0 END) AS total_bonus, " +
//                "SUM(CASE WHEN es.type = 'Kesinti' THEN es.total ELSE 0 END) AS total_deduction, " +
//                "SUM(CASE WHEN es.type = 'Avans' THEN es.total ELSE 0 END) AS total_prepaid_amount, " +
//                "SUM(CASE WHEN es.type = 'Prim' THEN es.total ELSE 0 END) - " +
//                "SUM(CASE WHEN es.type = 'Kesinti' THEN es.total ELSE 0 END) + " +
//                "SUM(CASE WHEN es.type = 'Avans' THEN es.total ELSE 0 END) AS total_income, " +
//                "CURRENT_DATE AS calculation_date");
//        factoryBean.setFromClause("FROM employee_salary es");
//        factoryBean.setWhereClause("EXTRACT(MONTH FROM es.date) = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM es.date) = EXTRACT(YEAR FROM CURRENT_DATE) ");
//        factoryBean.setGroupClause("GROUP BY es.employee_id, EXTRACT(MONTH FROM es.date), EXTRACT(YEAR FROM es.date)");
        factoryBean.setSortKey("employee_id");

        return factoryBean.getObject();

    }

    // Chunk:10 PageSize:100 TotalData:1000 -> 10 sayfa 100 chunk ile bu süreç ilerletilecek.
    // Senkron bir şekilde veritabanından verileri okur ve her sayfada 100 kayıt alır.
    //  Eğer paralel okuma yapıp partition yapısı kullanıcaksak @StepScope anotasyonu ile birlikte kullanmalıyız.
    @Bean
    public JdbcPagingItemReader<EmployeeEntitlement> jdbcPagingItemReader(DataSource dataSource) throws Exception {
        JdbcPagingItemReader<EmployeeEntitlement> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setQueryProvider(pagingQueryProvider(dataSource));
        reader.setPageSize(5); // Her sayfada kaç kayıt okunacağını belirler
        reader.setRowMapper(new EmployeeEntitlementRowMapper()); // RowMapper sınıfını kullanarak verileri nesneye dönüştürür

        return reader;
    }


    // @StepScope anotasyonu ile birlikte kullanmalıyız. Eğer paralel olarak okuma yapıcaksak unutmayalım.
    @Bean
    public JdbcCursorItemReader<EmployeeEntitlement> jdbcCursorItemReader(DataSource dataSource) throws Exception {
        JdbcCursorItemReader<EmployeeEntitlement> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("SELECT employee_id, _month, _year, total_bonus, total_deduction, total_prepaid_amount, total_income, calculation_date FROM employee_entitlement_vm");
        reader.setRowMapper(new EmployeeEntitlementRowMapper()); // RowMapper sınıfını kullanarak verileri nesneye dönüştürür

        return reader;
    }

    @Bean
    public ItemProcessor<EmployeeEntitlement, EmployeeEntitlement> jdbcItemProcessor() {
        return  item -> {
            System.out.println(item.getEmployeeId());
            return item;
        };
    }

    @Bean
    public ItemWriter<EmployeeEntitlement> jdbcItemWriter() {
        return items -> {
            for (EmployeeEntitlement item : items) {
                System.out.println("Writing item: " + item);
            }
        };
    }

    @Bean
    public Step jdbcItemReaderStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,DataSource dataSource) throws Exception {
        return new StepBuilder("jdbcItemReaderStep", jobRepository).<EmployeeEntitlement, EmployeeEntitlement>chunk(2, transactionManager)
                .reader(jdbcCursorItemReader(dataSource))
                .processor(jdbcItemProcessor())
                .writer(jdbcItemWriter())
                .faultTolerant()
                .skipLimit(5)
                .build();

    }

    @Bean(name = "jdbcPagingItemReaderJob")
    public Job jdbcItemReaderJob(JobRepository jobRepository, PlatformTransactionManager transactionManager,DataSource dataSource) throws Exception {
        return  new JobBuilder("jdbcPagingItemReaderJob", jobRepository)
                .start(jdbcItemReaderStep(jobRepository, transactionManager,dataSource))
                .build();
    }

}
