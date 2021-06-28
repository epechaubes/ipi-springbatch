package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.entity.Commune;
import com.ipiecoles.batch.utils.OpenStreetMapUtils;
import org.springframework.batch.item.ItemProcessor;

import java.util.Map;

public class CommuneAddGpsProcessor implements ItemProcessor<Commune, Commune> {
    @Override
    public Commune process(Commune item) throws Exception {
        String adress = item.getNom() + " " + item.getCodePostal();
        Map<String,Double> map = OpenStreetMapUtils.getInstance().getCoordinates(adress);
        if(map != null && map.size() == 2){
            item.setLatitude(map.get("lat"));
            item.setLongitude(map.get("lon"));
            return item;
        }
        return null;
    }
}
