package com.mohe.spring.security

import io.jsonwebtoken.Claims
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.SignatureAlgorithm
import io.jsonwebtoken.security.Keys
import org.springframework.beans.factory.annotation.Value
import org.springframework.security.core.Authentication
import org.springframework.stereotype.Component
import java.security.Key
import java.util.*

@Component
class JwtTokenProvider {
    
    @Value("\${jwt.secret:mySecretKey123456789012345678901234567890123456789012345678901234567890}")
    private lateinit var jwtSecret: String
    
    @Value("\${jwt.access-token-expiration:3600000}") // 1 hour
    private val accessTokenExpiration: Long = 3600000
    
    @Value("\${jwt.refresh-token-expiration:2592000000}") // 30 days
    private val refreshTokenExpiration: Long = 2592000000
    
    private val key: Key by lazy { Keys.hmacShaKeyFor(jwtSecret.toByteArray()) }
    
    fun generateAccessToken(authentication: Authentication): String {
        val userPrincipal = authentication.principal as UserPrincipal
        val expiryDate = Date(Date().time + accessTokenExpiration)
        
        return Jwts.builder()
            .setSubject(userPrincipal.id.toString())
            .setIssuedAt(Date())
            .setExpiration(expiryDate)
            .claim("nickname", userPrincipal.nickname)
            .claim("roles", userPrincipal.authorities.map { it.authority })
            .signWith(key, SignatureAlgorithm.HS512)
            .compact()
    }
    
    fun generateRefreshToken(userId: Long): String {
        val expiryDate = Date(Date().time + refreshTokenExpiration)
        
        return Jwts.builder()
            .setSubject(userId.toString())
            .setIssuedAt(Date())
            .setExpiration(expiryDate)
            .claim("type", "refresh")
            .signWith(key, SignatureAlgorithm.HS512)
            .compact()
    }
    
    fun getUserIdFromToken(token: String): Long {
        val claims = Jwts.parserBuilder()
            .setSigningKey(key)
            .build()
            .parseClaimsJws(token)
            .body
        
        return claims.subject.toLong()
    }
    
    fun validateToken(token: String): Boolean {
        try {
            Jwts.parserBuilder()
                .setSigningKey(key)
                .build()
                .parseClaimsJws(token)
            return true
        } catch (ex: Exception) {
            return false
        }
    }
    
    fun getClaimsFromToken(token: String): Claims {
        return Jwts.parserBuilder()
            .setSigningKey(key)
            .build()
            .parseClaimsJws(token)
            .body
    }
    
    fun getAccessTokenExpiration(): Long = accessTokenExpiration
    fun getRefreshTokenExpiration(): Long = refreshTokenExpiration
}