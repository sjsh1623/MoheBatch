package com.mohe.spring.repository;

import com.mohe.spring.entity.UserPreferenceVector;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface UserPreferenceVectorRepository extends JpaRepository<UserPreferenceVector, Long> {
    
    Optional<UserPreferenceVector> findByUserId(Long userId);
    
    void deleteByUserId(Long userId);
    
    List<UserPreferenceVector> findByUserIdIn(List<Long> userIds);
}