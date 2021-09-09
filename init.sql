CREATE DATABASE analytics_mainnet;

REVOKE CREATE ON SCHEMA PUBLIC FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA PUBLIC FROM PUBLIC;

CREATE ROLE readonly;
GRANT USAGE ON SCHEMA public TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;

CREATE USER mainnet with password 'password';
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mainnet;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO mainnet;

CREATE TABLE daily_statistics
(
    computed_for_timestamp     numeric(20, 0) NOT NULL,
    transactions_count         numeric(20, 0) NOT NULL,
    teragas_used               numeric(20, 0) NOT NULL,
    deposit_amount             numeric(45, 0) NOT NULL,
    new_accounts_count         numeric(20, 0) NOT NULL,
    deleted_accounts_count     numeric(20, 0) NOT NULL,
    active_accounts_count      numeric(20, 0) NOT NULL,
    new_contracts_count        numeric(20, 0) NOT NULL,
    new_unique_contracts_count numeric(20, 0) NOT NULL,
    active_contracts_count     numeric(20, 0) NOT NULL
);

ALTER TABLE ONLY daily_statistics
    ADD CONSTRAINT daily_statistics_pkey PRIMARY KEY (computed_for_timestamp);
