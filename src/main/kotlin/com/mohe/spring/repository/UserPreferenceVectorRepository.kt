package com.mohe.spring.repository

import com.mohe.spring.entity.UserPreferenceVector
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface UserPreferenceVectorRepository : JpaRepository<UserPreferenceVector, Long> {
    fun findByUserId(userId: Long): UserPreferenceVector?
    fun deleteByUserId(userId: Long)
    fun findByUserIdIn(userIds: List<Long>): List<UserPreferenceVector>
}