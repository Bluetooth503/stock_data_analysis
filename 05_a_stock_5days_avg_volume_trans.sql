-- 创建普通物化视图
CREATE MATERIALIZED VIEW a_stock_5days_avg_volume_trans AS
SELECT
    MAKE_TIME(
        EXTRACT(HOUR FROM time_bucket('5 minutes', f.trade_time))::INTEGER,
        EXTRACT(MINUTE FROM time_bucket('5 minutes', f.trade_time))::INTEGER,
        0
    ) AS trade_time,
    f.ts_code,
    AVG(f.volume) AS avg_volume,
    MIN(f.volume) AS min_volume,
    MAX(f.volume) AS max_volume,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.volume) AS median_volume,
    AVG(f.transaction_num) AS avg_trans,
    MIN(f.transaction_num) AS min_trans,
    MAX(f.transaction_num) AS max_trans,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.transaction_num) AS median_trans
FROM a_stock_5min_orderbook_factors f
JOIN a_stock_trade_dates d ON DATE(f.trade_time) = d.trade_date
GROUP BY f.trade_time, f.ts_code;

-- 创建索引以提高查询性能
CREATE UNIQUE INDEX idx_stock_5days_avg_volume_trans_ts_time ON a_stock_5days_avg_volume_trans(ts_code, trade_time);
CREATE INDEX idx_stock_5days_avg_volume_trans_ts      ON a_stock_5days_avg_volume_trans (ts_code);
CREATE INDEX idx_stock_5days_avg_volume_trans_time    ON a_stock_5days_avg_volume_trans (trade_time DESC);

-- 创建刷新视图的函数
CREATE OR REPLACE FUNCTION refresh_stock_5days_avg_volume_trans()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY a_stock_5days_avg_volume_trans;
    RAISE NOTICE 'a_stock_5days_avg_volume_trans refreshed at %', now();
END;
$$ LANGUAGE plpgsql;
