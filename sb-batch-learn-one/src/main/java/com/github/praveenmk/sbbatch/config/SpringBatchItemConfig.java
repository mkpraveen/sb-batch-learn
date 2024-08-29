package com.github.praveenmk.sbbatch.config;

import java.net.MalformedURLException;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.transaction.PlatformTransactionManager;

import com.github.praveenmk.sbbatch.model.Transaction;
import com.github.praveenmk.sbbatch.service.CustomItemProcessor;
import com.github.praveenmk.sbbatch.service.RecordFieldSetMapper;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@Configuration
@RequiredArgsConstructor
public class SpringBatchItemConfig {


    @Value("input/record.csv")
    private Resource inputCsv;

    @Value("file:xml/output.xml")
    private WritableResource outputXml;

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    
    @Bean
    public ItemReader<Transaction> itemReader()
      throws UnexpectedInputException, ParseException {
        FlatFileItemReader<Transaction> reader = new FlatFileItemReader<Transaction>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        String[] tokens = { "recordid", "username", "userid", "transactiondate", "amount" };
        tokenizer.setNames(tokens);
        reader.setResource(inputCsv);
        DefaultLineMapper<Transaction> lineMapper = 
          new DefaultLineMapper<Transaction>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new RecordFieldSetMapper());
        reader.setLineMapper(lineMapper);
        reader.setLinesToSkip(1);
        return reader;
    }

    @Bean
    public ItemProcessor<Transaction, Transaction> itemProcessor() {
        return new CustomItemProcessor();
    }

//    @Bean
//    public ItemWriter<Transaction> itemWriter(Marshaller marshaller)
//      throws MalformedURLException {
//        StaxEventItemWriter<Transaction> itemWriter = 
//          new StaxEventItemWriter<Transaction>();
//        itemWriter.setMarshaller(marshaller);
//        itemWriter.setRootTagName("transactionRecord");
//        itemWriter.setResource(outputXml);
//        return itemWriter;
//    }

    @Bean
    @SneakyThrows
    public KafkaItemWriter<String,Transaction> salesInfoKafkaItemWriter(){
        var kafkaItemWriter = new KafkaItemWriter<String,Transaction>();
        kafkaItemWriter.setKafkaTemplate(kafkaTemplate);
        kafkaItemWriter.setItemKeyMapper(transaction -> String.valueOf(transaction.getRecordId()));
        kafkaItemWriter.setDelete(Boolean.FALSE);
        kafkaItemWriter.afterPropertiesSet();
        return kafkaItemWriter;
    }
    
    @Bean
    public Marshaller marshaller() {
        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(new Class[] { Transaction.class });
        return marshaller;
    }

    @Bean
    public TaskExecutor taskExecutor() {
      return new SimpleAsyncTaskExecutor("spring_batch");
    }
    
    @Bean
    protected Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager,
            @Qualifier("itemProcessor") ItemProcessor<Transaction, Transaction> processor,
            ItemWriter<Transaction> writer) {
        return new StepBuilder("step1", jobRepository)
                .<Transaction, Transaction>chunk(10, transactionManager)
                .reader(itemReader())
                .processor(processor)
                .writer(writer)
                // .taskExecutor(taskExecutor())
                .build();
    }



    @Bean(name = "firstBatchJob")
    protected Job job(JobRepository jobRepository, @Qualifier("step1") Step step1) {
        return new JobBuilder("firstBatchJob", jobRepository)
                .preventRestart()
                .start(step1)
                .build();
    }
}
