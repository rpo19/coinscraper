CREATE TABLE IF NOT EXISTS tweets (
 timestamp      TIMESTAMPTZ         NOT NULL,
 text           TEXT                NOT NULL
);

CREATE TABLE IF NOT EXISTS prices (
 timestamp      TIMESTAMPTZ         NOT NULL,
 askPrice       DOUBLE PRECISION    NOT NULL,
 askQty         DOUBLE PRECISION    NOT NULL,
 bidPrice       DOUBLE PRECISION    NOT NULL,
 bidQty         DOUBLE PRECISION    NOT NULL,
 symbol         CHAR(15)            NOT NULL
);

SELECT create_hypertable('tweets', 'timestamp');
SELECT create_hypertable('prices', 'timestamp');