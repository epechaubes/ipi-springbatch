package com.ipiecoles.batch.dbexport;


import com.ipiecoles.batch.entity.Commune;
import com.ipiecoles.batch.repository.CommuneRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Sort;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class CommunesDBExportBatch {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public CommuneRepository communeRepository;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Value("${communeImportBatch.Chunk}")
    private Integer chunk;

    @Value("${communesDBExportBatch.FileSystemResource}")
    private FileSystemResource outputResource;

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Bean
    public Tasklet countFileTasklet(){
        return new CountFileTasklet();
    }

    @Bean
    public Step stepCountFileTasklet(){
        return stepBuilderFactory.get("stepCountFileTasklet").tasklet(countFileTasklet())
                .build();
    }

    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[] {"countCp","countCommune"});
        return listener;
    }

    @Bean
    public Step stepReaderWriter (RepositoryItemReader<Commune> repositoryCommuneReader , ItemWriter<Commune> writer){
        return stepBuilderFactory.get("repositoryCommuneReader")
                .<Commune, Commune>chunk(chunk)
                .reader(repositoryCommuneReader)
                .writer(writer)
                .listener(promotionListener())
                .build();
    }


    @Bean
    @Qualifier("exportCommunes")
    public Job exportCommunes(Step stepCountFileTasklet, Step stepReaderWriter){
        return jobBuilderFactory.get("exportCommunes")
                .incrementer(new RunIdIncrementer())
                .flow(stepCountFileTasklet)
                .next(stepReaderWriter)
                .end()
                .build();
    }


    @Bean
    @Qualifier("repositoryCommuneReader")
    public RepositoryItemReader<Commune> repositoryCommuneReader() {
        Map<String, Sort.Direction> map = new HashMap<>();
        map.put("codePostal", Sort.Direction.DESC);
        RepositoryItemReader<Commune> repositoryItemReader = new RepositoryItemReader<>();
        repositoryItemReader.setRepository(communeRepository);
        repositoryItemReader.setMethodName("findAll");
        repositoryItemReader.setSort(map);
        return repositoryItemReader;
    }

    @Bean
    @Qualifier("writer")
    public FlatFileItemWriter<Commune> writer() {
        return new FlatFileItemWriterBuilder<Commune>()
                .name("itemWriter")
                .resource(outputResource)
                .lineAggregator(newLineAgg())
                .headerCallback(new Header())
                .footerCallback(new Footer())
                .build();
    }


    public LineAggregator<Commune> newLineAgg (){
            return new DelimitedLineAggregator<>() {
                {
                    setDelimiter(" - ");
                    setFieldExtractor(new BeanWrapperFieldExtractor<>() {
                        {
                            setNames(new String[]{"codeInsee", "codePostal", "nom", "latitude", "longitude" });
                        }

                    });
                }
            };
    }
}
