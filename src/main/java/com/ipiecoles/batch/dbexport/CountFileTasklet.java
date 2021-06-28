package com.ipiecoles.batch.dbexport;

import com.ipiecoles.batch.repository.CommuneRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;


public class CountFileTasklet implements Tasklet , StepExecutionListener{

    @Autowired
    CommuneRepository communeRepository;

    private StepExecution stepExecution;
    Logger logger = LoggerFactory.getLogger(this.getClass());
    public Long countCp = null;
    public Long countCommune = null;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        try {
            countCp = communeRepository.countDistinctCodePostal();
            Reference.countCp = countCp;
            countCommune = communeRepository.countCommune();
            Reference.countCommune = countCommune;

            logger.info("il y a {} code(s) postal/postaux", countCp);
            logger.info("il y a {} commune(s)", countCp);

            ExecutionContext stepContext = this.stepExecution.getExecutionContext();
            stepContext.put("countCp", countCp);
            stepContext.put("countCommune", countCommune);
            return RepeatStatus.FINISHED;

        } catch (Exception e) {
            logger.error("soucis détecter lors du compte de codes postaux et/ou de communes");
        }
        //status finished here to not stop the process
        return RepeatStatus.FINISHED;
    }

    @Override
    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    @AfterStep
    public ExitStatus afterStep(StepExecution sExec) {
        sExec.getJobExecution().getExecutionContext().put("CountCp", countCp);
        sExec.getJobExecution().getExecutionContext().put("CountCommune", countCommune);
        return ExitStatus.COMPLETED;
    }
}
