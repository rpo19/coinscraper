CREATE TABLE IF NOT EXISTS tweets (
 id             BIGSERIAL              NOT NULL,
 timestamp      TIMESTAMPTZ         NOT NULL,
 text           TEXT                NOT NULL,
 prediction     BOOLEAN,
 PRIMARY KEY(timestamp, id)
);

CREATE TABLE IF NOT EXISTS prices (
 id             BIGSERIAL              NOT NULL,
 timestamp      TIMESTAMPTZ         NOT NULL,
 askprice       DOUBLE PRECISION    NOT NULL,
 askqty         DOUBLE PRECISION    NOT NULL,
 bidprice       DOUBLE PRECISION    NOT NULL,
 bidqty         DOUBLE PRECISION    NOT NULL,
 symbol         CHAR(15)            NOT NULL,
 PRIMARY KEY(timestamp, id)
);

CREATE TABLE IF NOT EXISTS trendperminute (
 id             BIGSERIAL           NOT NULL,
 timestamp      TIMESTAMPTZ         UNIQUE NOT NULL,
 asktrend       BOOLEAN             NOT NULL,
 PRIMARY KEY(timestamp, id)
);

SELECT create_hypertable('tweets', 'timestamp');
SELECT create_hypertable('prices', 'timestamp');
SELECT create_hypertable('trendperminute', 'timestamp');