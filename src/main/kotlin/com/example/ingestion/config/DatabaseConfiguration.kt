package com.example.ingestion.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import javax.sql.DataSource

@Configuration
class DatabaseConfiguration(
    @Value("\${spring.datasource.url}") private val jdbcUrl: String,
    @Value("\${spring.datasource.username}") private val username: String,
    @Value("\${spring.datasource.password}") private val password: String,
    @Value("\${spring.datasource.hikari.maximum-pool-size:20}") private val maximumPoolSize: Int,
    @Value("\${spring.datasource.hikari.minimum-idle:5}") private val minimumIdle: Int,
    @Value("\${spring.datasource.hikari.connection-timeout:20000}") private val connectionTimeout: Long,
    @Value("\${spring.datasource.hikari.idle-timeout:300000}") private val idleTimeout: Long,
    @Value("\${spring.datasource.hikari.max-lifetime:1200000}") private val maxLifetime: Long
) {

    private val logger = LoggerFactory.getLogger(DatabaseConfiguration::class.java)

    @Bean
    @Primary
    fun dataSource(): DataSource {
        logger.info("Configuring HikariCP DataSource")
        logger.info("JDBC URL: {}", jdbcUrl)
        logger.info("Username: {}", username)
        logger.info("Max Pool Size: {}", maximumPoolSize)
        logger.info("Min Idle: {}", minimumIdle)
        
        val config = HikariConfig().apply {
            this.jdbcUrl = this@DatabaseConfiguration.jdbcUrl
            this.username = this@DatabaseConfiguration.username
            this.password = this@DatabaseConfiguration.password
            this.maximumPoolSize = this@DatabaseConfiguration.maximumPoolSize
            this.minimumIdle = this@DatabaseConfiguration.minimumIdle
            this.connectionTimeout = this@DatabaseConfiguration.connectionTimeout
            this.idleTimeout = this@DatabaseConfiguration.idleTimeout
            this.maxLifetime = this@DatabaseConfiguration.maxLifetime
            
            // Additional HikariCP optimizations
            this.leakDetectionThreshold = 60000 // 60 seconds
            this.connectionTestQuery = "SELECT 1"
            this.poolName = "DataIngestionPool"
            
            // Performance optimizations
            this.addDataSourceProperty("cachePrepStmts", "true")
            this.addDataSourceProperty("prepStmtCacheSize", "250")
            this.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
            this.addDataSourceProperty("useServerPrepStmts", "true")
            this.addDataSourceProperty("useLocalSessionState", "true")
            this.addDataSourceProperty("rewriteBatchedStatements", "true")
            this.addDataSourceProperty("cacheResultSetMetadata", "true")
            this.addDataSourceProperty("cacheServerConfiguration", "true")
            this.addDataSourceProperty("elideSetAutoCommits", "true")
            this.addDataSourceProperty("maintainTimeStats", "false")
        }

        return HikariDataSource(config)
    }
}