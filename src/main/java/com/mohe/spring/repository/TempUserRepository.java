package com.mohe.spring.repository;

import com.mohe.spring.entity.TempUser;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Optional;

@Repository
public interface TempUserRepository extends JpaRepository<TempUser, String> {
    
    Optional<TempUser> findByEmail(String email);
    
    @Query("SELECT tu FROM TempUser tu WHERE tu.id = :id AND tu.expiresAt > :now")
    Optional<TempUser> findValidTempUser(@Param("id") String id, @Param("now") OffsetDateTime now);
    
    @Modifying
    @Query("DELETE FROM TempUser tu WHERE tu.expiresAt < :now")
    void deleteExpiredTempUsers(@Param("now") OffsetDateTime now);
    
    boolean existsByEmail(String email);
}