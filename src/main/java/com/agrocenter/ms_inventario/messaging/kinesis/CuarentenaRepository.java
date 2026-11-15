package com.agrocenter.ms_inventario.messaging.kinesis;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import jakarta.persistence.LockModeType;
import java.util.Optional;

public interface CuarentenaRepository extends JpaRepository<EventoCuarentena, String> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select e from EventoCuarentena e where e.id = :id")
    Optional<EventoCuarentena> buscarParaReprocesar(@Param("id") String id);
}
