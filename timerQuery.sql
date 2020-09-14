WITH avgaskperminute AS (
	SELECT AVG(askprice) as avgask, time_bucket('1 minutes', timestamp) AS one_min
		FROM prices
		WHERE timestamp < date_trunc('minute', NOW())
		AND (timestamp >= (SELECT MAX(timestamp) FROM trendperminute)
		+ interval '1 minute'
		OR (SELECT MAX(timestamp) FROM trendperminute) IS NULL)
		GROUP BY one_min
	)
	
	INSERT INTO trendperminute (avgask, timestamp, asktrend)
		SELECT avm1.avgask AS avgask,
			avm1.one_min AS timestamp,
			(avm2.avgask >= avm1.avgask) AS asktrend
			FROM avgaskperminute AS avm1 JOIN avgaskperminute AS avm2
			ON avm1.one_min + interval '1 minute' = avm2.one_min;