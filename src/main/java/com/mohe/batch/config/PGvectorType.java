package com.mohe.batch.config;

import com.pgvector.PGvector;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Custom Hibernate UserType for PGvector
 * Allows Hibernate to properly map PGvector objects to PostgreSQL vector columns
 */
public class PGvectorType implements UserType<PGvector> {

    @Override
    public int getSqlType() {
        return Types.OTHER;
    }

    @Override
    public Class<PGvector> returnedClass() {
        return PGvector.class;
    }

    @Override
    public boolean equals(PGvector x, PGvector y) {
        if (x == y) return true;
        if (x == null || y == null) return false;
        return x.toString().equals(y.toString());
    }

    @Override
    public int hashCode(PGvector x) {
        return x == null ? 0 : x.hashCode();
    }

    @Override
    public PGvector nullSafeGet(ResultSet rs, int position, SharedSessionContractImplementor session, Object owner)
            throws SQLException {
        Object value = rs.getObject(position);
        if (value == null) {
            return null;
        }

        if (value instanceof org.postgresql.util.PGobject) {
            org.postgresql.util.PGobject pgObject = (org.postgresql.util.PGobject) value;
            try {
                return new PGvector(pgObject.getValue());
            } catch (Exception e) {
                throw new SQLException("Failed to parse PGvector from: " + pgObject.getValue(), e);
            }
        }

        if (value instanceof PGvector) {
            return (PGvector) value;
        }

        throw new SQLException("Unexpected type for PGvector: " + value.getClass().getName());
    }

    @Override
    public void nullSafeSet(PreparedStatement st, PGvector value, int index, SharedSessionContractImplementor session)
            throws SQLException {
        if (value == null) {
            st.setNull(index, Types.OTHER);
        } else {
            org.postgresql.util.PGobject pgObject = new org.postgresql.util.PGobject();
            pgObject.setType("vector");
            pgObject.setValue(value.toString());
            st.setObject(index, pgObject);
        }
    }

    @Override
    public PGvector deepCopy(PGvector value) {
        if (value == null) return null;
        try {
            return new PGvector(value.toString());
        } catch (Exception e) {
            throw new HibernateException("Failed to deep copy PGvector", e);
        }
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Serializable disassemble(PGvector value) {
        return value == null ? null : value.toString();
    }

    @Override
    public PGvector assemble(Serializable cached, Object owner) {
        if (cached == null) return null;
        try {
            return new PGvector((String) cached);
        } catch (Exception e) {
            throw new HibernateException("Failed to assemble PGvector from: " + cached, e);
        }
    }

    @Override
    public PGvector replace(PGvector detached, PGvector managed, Object owner) {
        return deepCopy(detached);
    }
}
