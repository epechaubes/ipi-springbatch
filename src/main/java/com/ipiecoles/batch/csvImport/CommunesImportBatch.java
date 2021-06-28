package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.dto.CommuneDto;
import com.ipiecoles.batch.exception.CommuneCSVException;
import com.ipiecoles.batch.entity.Commune;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class CommunesImportBatch {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public EntityManagerFactory entityManagerFactory;

    @Value("${communeImportBatch.Chunk}")
    private Integer chunk;

    @Value("${communeImportBatch.File}")
    private String file;

    @Bean
    public Tasklet helloWorldTasklet(){
        return new HelloWorldTasklet();
    }

    @Bean
    public CommuneProcessor communeProcessor(){
        return new CommuneProcessor();
    }

    @Bean
    public CommuneAddGpsProcessor communeAddGpsProcessor() { return new CommuneAddGpsProcessor(); }

    @Bean
    public StepExecutionListener communeCSVImportStepListener () { return new CommuneCSVImportStepListener(); }

    @Bean
    public ChunkListener communeCSVImportChunkListener(){
        return new CommuneCSVImportChunkListener();
    }

    @Bean
    public ItemReadListener<CommuneDto> communeCSVItemReadListener(){
        return new CommuneCSVItemListener();
    }

    @Bean
    public ItemWriteListener<Commune> communeCSVItemWriteListener(){
        return new CommuneCSVItemListener();
    }

    @Bean
    public CommunesMissingCoordinatesSkipListener communesMissingCoordinatesSkipListener(){
        return new CommunesMissingCoordinatesSkipListener();
    }

    @Bean
    public FlatFileItemReader<CommuneDto> communesCSVReader() {
        return new FlatFileItemReaderBuilder<CommuneDto>()
                .name("communesCSVReader").linesToSkip(1)
                .resource(new ClassPathResource(file))
                .delimited().delimiter(";")
                .names("insee", "nom", "cp", "ligne5", "libelle", "gps")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(CommuneDto.class);
                }})
                .build();
    }

//    @Bean
//    public JpaPagingItemReader<Commune> communesMissingCoordinatesJpaItemReader() {
//        return new JpaPagingItemReaderBuilder<Commune>()
//                .name("communesMissingCoordinatesJpaItemReader")
//                .entityManagerFactory(entityManagerFactory)
//                .pageSize(10)
//                .queryString("from Commune c where c.latitude is null or c.longitude is null")
//                .build();
//    }

    @Bean
    public JpaItemWriter<Commune> writerJPA(){
        return new JpaItemWriterBuilder<Commune>().entityManagerFactory(entityManagerFactory).build();
    }

    @Bean
    public JdbcBatchItemWriter<Commune> writerJDBC(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Commune>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO COMMUNE(code_insee, nom, code_postal, latitude, longitude) VALUES " +
                        "(:codeInsee, :nom, :codePostal, :latitude, :longitude) " +
                        "ON DUPLICATE KEY UPDATE nom=c.nom, code_postal=c.code_postal, latitude=c.latitude, longitude=c.longitude")
                .dataSource(dataSource).build();
    }

    @Bean
    public Step stepHelloWorld(){
        return stepBuilderFactory.get("stepHelloWorld").tasklet(helloWorldTasklet())
                .listener(helloWorldTasklet())
                .build();
    }

    @Bean
    public Step stepImportCSV(JpaItemWriter<Commune> writerJpa){
        return stepBuilderFactory.get("importCSV")
                .<CommuneDto, Commune> chunk(chunk)
                .reader(communesCSVReader())
                .processor(communeProcessor())
                .writer(writerJpa)
                .faultTolerant()
                .skipPolicy(new AlwaysSkipItemSkipPolicy())
                .skip(CommuneCSVException.class)
                .skip(FlatFileParseException.class)
                .skip(CommuneCSVException.class)
                .listener(communesMissingCoordinatesSkipListener())
                .listener(communeCSVImportStepListener())
                .listener(communeCSVImportChunkListener())
                .listener(communeCSVItemReadListener())
                .listener(communeCSVItemWriteListener())
                .listener(communeProcessor())
                .build();
    }

//    @Bean
//    public Step stepImportCSVNoGps(JpaItemWriter<Commune> writerJpa){
//        FixedBackOffPolicy policy = new FixedBackOffPolicy();
//        policy.setBackOffPeriod(2000);
//        return stepBuilderFactory.get("importCSVNoGps")
//                .<Commune, Commune> chunk(chunk)
//                .reader(communesMissingCoordinatesJpaItemReader())
//                .processor(communeAddGpsProcessor())
//                .writer(writerJpa)
//                .faultTolerant()
//                .retryLimit(5)
//                .retry(NetworkException.class)
//                .backOffPolicy(policy)
//                .build();
//    }
//
//    @Bean
//    public Job importCsvJob(Step stepImportCSV, Step stepImportCSVNoGps) {
//        return jobBuilderFactory.get("importCsvJob")
//                .incrementer(new RunIdIncrementer())
//                .flow(stepHelloWorld())
//                .next(stepImportCSV)
//                .on("COMPLETED_WITH_MISSING_COORDINATES").to(stepImportCSVNoGps)
//                .end()
//                .build();
//    }

}
