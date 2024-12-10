"""
Option Trades Table DDL
"""

OPTION_TRADES_TABLE_DDL = """
    CREATE TABLE option_trades (
        key VARBINARY(2147483647),
        id VARCHAR(36) NOT NULL COMMENT 'Unique identifier for the trade.',
        timestamp_ms TIMESTAMP(3) NOT NULL COMMENT 'Timestamp in milliseconds since UNIX epoch of trade or book update.',
        symbol VARCHAR(9) NOT NULL COMMENT 'Option Symbol as a string formatted to the OPRA-PILLAR standard.',
        underlying VARCHAR(9) NOT NULL COMMENT 'Underlying Symbol.',
        spot DECIMAL(21,9) NOT NULL COMMENT 'Current price of the underlying asset.',
        strike DECIMAL(21,9) NOT NULL COMMENT 'Strike price of the contract.',
        exp_date DATE NOT NULL COMMENT 'The maturity/last trade date of the contract in milliseconds from the unix epoch.',
        dte INT NOT NULL COMMENT 'Days to Expiry.',
        put_call VARCHAR(2147483647) NOT NULL COMMENT 'Option Type: Put or Call.',
        qty INT N OT NULL COMMENT 'Trade quantity.',
        price DECIMAL(21,9) NOT NULL COMMENT 'Trade price.',
        premium DECIMAL(38, 10) NOT NULL COMMENT 'Total value of the trade.',
        side VARCHAR(2147483647) NOT NULL COMMENT 'Book side of the trade, A=Ask, B=Bid, N=None.',
        bias VARCHAR(2147483647) NOT NULL COMMENT 'Sentiment of the trade, B=Bullish, R=Bearish, N=None.',
        score DECIMAL(8,6) NOT NULL COMMENT 'Sentiment score. -1.000000 very bearish, +1.000000 very bullish, 0.000000 neutral.',
        venue VARCHAR(2147483647) NOT NULL COMMENT 'The venue that published the trade.',
        condition VARCHAR(2147483647) NOT NULL COMMENT 'Trade condition code.',
        bid DECIMAL(21,9) NOT NULL COMMENT 'Best bid price (NBBO Bid).',
        ask DECIMAL(21,9) NOT NULL COMMENT 'Best ask price (NBBO Ask).',
        theo DECIMAL(21,9) NOT NULL COMMENT 'Theoretical price of the option.',
        iv DECIMAL(18, 16) NOT NULL COMMENT 'Implied Volatility of the option expressed as a fraction (e.g. 0.1234567890 for 12.34567890%).',
        delta DECIMAL(18, 16) NOT NULL COMMENT 'Delta of the option, ranges from -1.0000000000 to 1.0000000000.',
        gamma DECIMAL(18, 16) NOT NULL COMMENT 'Gamma of the option, typically a very small number requiring high precision.',
        vega DECIMAL(18, 16) NOT NULL COMMENT 'Vega of the option, measures sensitivity to volatility changes.',
        theta DECIMAL(18, 16) NOT NULL COMMENT 'Theta of the option, represents time decay and is typically negative.',
        rho DECIMAL(18, 16) NOT NULL COMMENT 'Rho of the option, measures sensitivity to interest rate changes.',
        v_ask INT NOT NULL COMMENT 'The number of contracts nearer to the NBBO ask.',
        v_bid INT NOT NULL COMMENT 'The number of contracts nearer to the NBBO bid.',
        v_unk INT NOT NULL COMMENT 'The number of contracts traded without a clear aggressor.',
        v_mid INT NOT NULL COMMENT 'The number of contracts traded in the middle of the spread.',
        v_leg INT NOT NULL COMMENT 'The number of contracts traded as part of a complex trade with multiple legs.',
        v_stk INT NOT NULL COMMENT 'The number of shares traded as a result of the contract traded as a leg in complex, multiple leg trades.',
        v BIGINT NOT NULL COMMENT 'The number of contracts traded at the time of trade execution for the trading session.',
        oi BIGINT NOT NULL COMMENT 'The contract''s open interest is the number of un-closed contracts remain after the previous session. Calculated nightly.',
        tags ARRAY<VARCHAR(2147483647) NOT NULL> NOT NULL COMMENT 'List of flags associated with the trade.',
        PRIMARY KEY (id) NOT ENFORCED,
        WATERMARK FOR timestamp_ms AS timestamp_ms - INTERVAL '1' SECOND

    )
    DISTRIBUTED BY HASH(key) INTO 16 BUCKETS
    WITH (
        'changelog.mode' = 'append',
        'connector' = 'confluent',
        'kafka.cleanup-policy' = 'delete',
        'kafka.max-message-size' = '2097164 bytes',
        'kafka.retention.size' = '0 bytes',
        'kafka.retention.time' = '30 d',
        'key.format' = 'raw',
        'scan.bounded.mode' = 'unbounded',
        'scan.startup.mode' = 'earliest-offset',
        'value.format' = 'avro-registry'
    )
"""

__all__ = ["OPTION_TRADES_TABLE_DDL"]
