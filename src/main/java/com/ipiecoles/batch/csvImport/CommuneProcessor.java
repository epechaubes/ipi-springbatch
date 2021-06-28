package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.dto.CommuneDto;
import com.ipiecoles.batch.exception.CommuneCSVException;
import com.ipiecoles.batch.entity.Commune;
import org.apache.commons.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterProcess;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeProcess;
import org.springframework.batch.core.annotation.OnProcessError;
import org.springframework.batch.item.ItemProcessor;

public class CommuneProcessor implements ItemProcessor<CommuneDto, Commune> {

    private Integer nbCommunesWithoutCoordinates = 0;

    @Override
    public Commune process(CommuneDto item) throws Exception {
        if(item.getNom()==null){
            return null;
        }
        Commune newItem = new Commune();
        validateCommuneCSV(item);
        newItem.setCodeInsee(item.getInsee());
        String nom = WordUtils.capitalizeFully(item.getNom());
        nom.replace("^L ", "L'");
        nom.replace(" L ", "L'");
        nom.replace("^D ", "D'");
        nom.replace(" D ", "D'");
        nom.replace("^Ste ", "Sainte ");
        nom.replace(" Ste ", "Sainte ");
        nom.replace("^St ", "Saint ");
        nom.replace(" St ", "Saint ");
        newItem.setNom(nom);
        newItem.setCodePostal(item.getCp());
        String[] coord = item.getGps().split(",");
        if(coord.length == 2){
            newItem.setLatitude(Double.valueOf(coord[0]));
            newItem.setLongitude(Double.valueOf(coord[1]));
        }
        return newItem;
    }

    private void validateCommuneCSV(CommuneDto item) throws CommuneCSVException {
        //Contrôler Code INSEE 5 chiffres
        if(item.getInsee() != null && !item.getInsee().matches("^[0-9AB]{5}$")){
            throw new CommuneCSVException("Le code Insee ne contient pas 5 chiffres");
        }
        //Contrôler Code postal 5 chiffres
        if(item.getCp() != null && !item.getCp().matches("^[0-9]{5}$")){
            throw new CommuneCSVException("Le code Postal ne contient pas 5 chiffres");
        }
        //Contrôler nom de la communes lettres en majuscules, espaces, tirets, et apostrophes
        if(item.getNom() != null && !item.getNom().matches("^[A-Z-' ]+$")){
            throw new CommuneCSVException("Le nom de la commune n'est pas composé uniquement de lettres, espaces et tirets");
        }
        //Contrôler les coordonnées GPS
        if(item.getGps() != null && !item.getGps().matches("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?),\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$")){
            nbCommunesWithoutCoordinates++;
            throw new CommuneCSVException("Le nom de la commune n'est pas composé uniquement de lettres, espaces et tirets");
        }
    }

    Logger logger = LoggerFactory.getLogger(this.getClass());
    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("Exec du AfterStepListener");
//        logger.info(stepExecution.getExecutionContext().getString("MSG"));
        logger.info(stepExecution.getSummary());
        if(nbCommunesWithoutCoordinates > 0){
            return new ExitStatus("COMPLETED_WITH_MISSING_COORDINATES");
        }
        return ExitStatus.COMPLETED;
    }
    @BeforeProcess
    public void beforeProcess(CommuneDto input){
        logger.info("Before Process => " + input.toString());
    }
    @AfterProcess
    public void afterProcess(CommuneDto input, Commune output){
        logger.info("After Process => " + input.toString() + " => " + output.toString());
    }
    @OnProcessError
    public void onProcessError(CommuneDto input, Exception ex){
        logger.error("Error Process => " + input.toString() + " => " + ex.getMessage());
    }

}
