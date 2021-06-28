package com.ipiecoles.batch.repository;

import com.ipiecoles.batch.entity.Commune;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface CommuneRepository extends JpaRepository<Commune, String>, PagingAndSortingRepository<Commune, String> {
    @Query("select count(distinct c.codePostal) from Commune c")
    long countDistinctCodePostal();

    @Query("select count(c.codePostal) from Commune c")
    long countCommune();

    @Override
    Page<Commune> findAll(Pageable pageable);
}
