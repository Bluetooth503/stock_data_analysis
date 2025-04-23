CREATE MATERIALIZED VIEW a_stock_5min_orderbook_factors
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', "timestamp") AS trade_time,
    ts_code,

    -- OHLCV 基础指标 
    FIRST(open_price, "timestamp") AS open,
    MAX(high_price) AS high,
    MIN(low_price) AS low,
    LAST(last_price, "timestamp") AS close,
    LAST(volume, "timestamp") - FIRST(volume, "timestamp") AS volume,
    LAST(amount, "timestamp") - FIRST(amount, "timestamp") AS amount,

    -- 盘口不平衡比例（买卖盘总量比）
    AVG((bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5) / 
        NULLIF(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5, 0)) AS total_bid_ask_ratio,

    -- 综合VWAP（买卖盘整体）
    AVG(CASE WHEN (bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5 + 
                  ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5) > 0 
        THEN (bid_volume1*bid_price1 + bid_volume2*bid_price2 + bid_volume3*bid_price3 + bid_volume4*bid_price4 + bid_volume5*bid_price5 +
              ask_volume1*ask_price1 + ask_volume2*ask_price2 + ask_volume3*ask_price3 + ask_volume4*ask_price4 + ask_volume5*ask_price5) / 
             (bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5 + 
              ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5)
        ELSE NULL END) AS total_vwap,
        
    -- 买卖盘口指标 
    AVG(ask_price1 - bid_price1) AS avg_spread,

    -- 流动性集中度指标
    AVG(bid_volume1 / NULLIF(bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5, 0)) AS bid_concentration,
    AVG(ask_volume1 / NULLIF(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5, 0)) AS ask_concentration,
    
    -- 订单簿倾斜度
    AVG(CASE WHEN (bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5) > 0 AND (bid_price1 - bid_price5) > 0
        THEN (bid_price1 - bid_price5)/NULLIF(bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5, 0)
        ELSE NULL END) AS bid_slope,
    
    AVG(CASE WHEN (ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5) > 0 AND (ask_price5 - ask_price1) > 0
        THEN (ask_price5 - ask_price1)/NULLIF(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5, 0)
        ELSE NULL END) AS ask_slope,
    
    -- 流动性分布不对称性
    AVG(CASE WHEN (bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5) > 0 
             AND  (ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5) > 0
        THEN (bid_volume1/NULLIF(bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5, 0)) / 
             NULLIF(ask_volume1/NULLIF(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5, 0), 0)
        ELSE NULL END) AS liquidity_asymmetry,
    
    -- 订单簿凸度(近似计算)
    AVG(CASE WHEN bid_price1 > 0 AND bid_price3 > 0 AND bid_price5 > 0
        THEN (bid_price1 - 2*bid_price3 + bid_price5)
        ELSE NULL END) AS bid_convexity,
    
    AVG(CASE WHEN ask_price1 > 0 AND ask_price3 > 0 AND ask_price5 > 0
        THEN (ask_price1 - 2*ask_price3 + ask_price5)
        ELSE NULL END) AS ask_convexity,
    
    -- 订单簿压力与均价偏差
    AVG((last_price - ((bid_price1 + ask_price1) / 2)) / NULLIF((ask_price1 - bid_price1), 0)) AS price_pressure_ratio,
        
    -- 买卖盘能量指标（类似物理学中的势能）
    AVG(bid_volume1 * (bid_price1 / last_price - 1)^2 + 
        bid_volume2 * (bid_price2 / last_price - 1)^2 + 
        bid_volume3 * (bid_price3 / last_price - 1)^2 + 
        bid_volume4 * (bid_price4 / last_price - 1)^2 + 
        bid_volume5 * (bid_price5 / last_price - 1)^2) AS bid_energy,
    
    AVG(ask_volume1 * (ask_price1 / last_price - 1)^2 + 
        ask_volume2 * (ask_price2 / last_price - 1)^2 + 
        ask_volume3 * (ask_price3 / last_price - 1)^2 + 
        ask_volume4 * (ask_price4 / last_price - 1)^2 + 
        ask_volume5 * (ask_price5 / last_price - 1)^2) AS ask_energy
FROM public.a_stock_level1_data
GROUP BY trade_time, ts_code;

-- 步骤1: 暂停持续聚合策略(如果有)
SELECT remove_continuous_aggregate_policy('a_stock_5min_orderbook_factors');
=
-- 步骤2: 删除持续聚合视图
DROP MATERIALIZED VIEW a_stock_5min_orderbook_factors;

-- 步骤3: 重新添加持续聚合策略
SELECT add_continuous_aggregate_policy('a_stock_5min_orderbook_factors',
    start_offset => INTERVAL '10 minutes' + INTERVAL '10 seconds',
    end_offset   => INTERVAL '10 seconds',
    schedule_interval => INTERVAL '1 minutes');

-- 手动刷新持续聚合，立即生成结果
CALL refresh_continuous_aggregate('a_stock_5min_orderbook_factors', NULL, NULL);