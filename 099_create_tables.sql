CREATE TABLE IF NOT EXISTS tweets (
 timestamp      TIMESTAMPTZ         NOT NULL,
 text           TEXT                NOT NULL
);

CREATE TABLE IF NOT EXISTS prices (
 timestamp      TIMESTAMPTZ         NOT NULL,
 askprice       DOUBLE PRECISION    NOT NULL,
 askqty         DOUBLE PRECISION    NOT NULL,
 bidprice       DOUBLE PRECISION    NOT NULL,
 bidqty         DOUBLE PRECISION    NOT NULL,
 symbol         CHAR(15)            NOT NULL,
 lastmasktrend  DOUBLE PRECISION,
 lastmbidtrend  DOUBLE PRECISION
);

SELECT create_hypertable('tweets', 'timestamp');
SELECT create_hypertable('prices', 'timestamp');