CREATE DATABASE analytics_mainnet;

REVOKE CREATE ON SCHEMA PUBLIC FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA PUBLIC FROM PUBLIC;

CREATE ROLE readonly;
GRANT USAGE ON SCHEMA public TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;

CREATE TABLE daily_transactions_count
(
    daily_timestamp    numeric(20, 0) PRIMARY KEY,
    transactions_count numeric(20, 0) NOT NULL
);

CREATE TABLE daily_teragas_used
(
    daily_timestamp numeric(20, 0) PRIMARY KEY,
    teragas_used    numeric(20, 0) NOT NULL
);

CREATE TABLE daily_deposit_amount
(
    daily_timestamp numeric(20, 0) PRIMARY KEY,
    deposit_amount  numeric(45, 0) NOT NULL
);

CREATE TABLE daily_new_accounts_count
(
    daily_timestamp    numeric(20, 0) PRIMARY KEY,
    new_accounts_count numeric(20, 0) NOT NULL
);

CREATE TABLE daily_deleted_accounts_count
(
    daily_timestamp        numeric(20, 0) PRIMARY KEY,
    deleted_accounts_count numeric(20, 0) NOT NULL
);

CREATE TABLE daily_active_accounts_count
(
    daily_timestamp       numeric(20, 0) PRIMARY KEY,
    active_accounts_count numeric(20, 0) NOT NULL
);

CREATE TABLE daily_new_contracts_count
(
    daily_timestamp     numeric(20, 0) PRIMARY KEY,
    new_contracts_count numeric(20, 0) NOT NULL
);

CREATE TABLE daily_new_unique_contracts_count
(
    daily_timestamp            numeric(20, 0) PRIMARY KEY,
    new_unique_contracts_count numeric(20, 0) NOT NULL
);

CREATE TABLE daily_active_contracts_count
(
    daily_timestamp        numeric(20, 0) PRIMARY KEY,
    active_contracts_count numeric(20, 0) NOT NULL
);
