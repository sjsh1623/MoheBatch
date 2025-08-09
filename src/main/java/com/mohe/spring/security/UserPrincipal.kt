package com.mohe.spring.security

import com.mohe.spring.entity.User
import org.springframework.security.core.GrantedAuthority
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.userdetails.UserDetails

class UserPrincipal(
    val id: Long,
    val email: String,
    private val password: String,
    val nickname: String?,
    private val authorities: Collection<GrantedAuthority>
) : UserDetails {
    
    companion object {
        fun create(user: User): UserPrincipal {
            val authorities = listOf(SimpleGrantedAuthority("ROLE_USER"))
            
            return UserPrincipal(
                id = user.id,
                email = user.email,
                password = user.passwordHash,
                nickname = user.nickname,
                authorities = authorities
            )
        }
    }
    
    override fun getAuthorities(): Collection<GrantedAuthority> = authorities
    
    override fun getPassword(): String = password
    
    override fun getUsername(): String = email
    
    override fun isAccountNonExpired(): Boolean = true
    
    override fun isAccountNonLocked(): Boolean = true
    
    override fun isCredentialsNonExpired(): Boolean = true
    
    override fun isEnabled(): Boolean = true
    
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is UserPrincipal) return false
        return id == other.id
    }
    
    override fun hashCode(): Int = id.hashCode()
}