package com.pulsestream.infrastructure.persistence.repository;

import com.pulsestream.infrastructure.persistence.entity.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

/**
 * Repository for {@link UserEntity}.
 *
 * <p>Users are created lazily by the service layer when an unknown userId is seen.
 * This repository provides the lookup and upsert hooks for that behaviour.
 */
@Repository
public interface UserRepository extends JpaRepository<UserEntity, UUID> {

    Optional<UserEntity> findByExternalId(String externalId);

    boolean existsByExternalId(String externalId);
}
