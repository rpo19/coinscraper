CREATE TABLE IF NOT EXISTS tweets (
 timestamp      TIMESTAMPTZ         PRIMARY KEY,
 text           TEXT                NOT NULL
);

CREATE TABLE IF NOT EXISTS prices (
 timestamp      TIMESTAMPTZ         PRIMARY KEY,
 askprice       DOUBLE PRECISION    NOT NULL,
 askqty         DOUBLE PRECISION    NOT NULL,
 bidprice       DOUBLE PRECISION    NOT NULL,
 bidqty         DOUBLE PRECISION    NOT NULL,
 symbol         CHAR(15)            NOT NULL,
 lastmasktrend  BOOLEAN,
 lastmbidtrend  BOOLEAN
);

CREATE TABLE IF NOT EXISTS trendperminute (
 timestamp      TIMESTAMPTZ         PRIMARY KEY,
 asktrend           BOOLEAN         NOT NULL
);

SELECT create_hypertable('tweets', 'timestamp');
SELECT create_hypertable('prices', 'timestamp');
SELECT create_hypertable('trendperminute', 'timestamp');