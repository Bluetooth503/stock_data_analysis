CREATE TABLE public.ods_a_stock_level1_data (
  trade_time timestamp NOT NULL,
	ts_code TEXT NOT NULL,
	last_price float8 NULL,
	"open" float8 NULL,
	high float8 NULL,
	low float8 NULL,
	pre_close float8 NULL,
	volume int8 NULL,
	amount float8 NULL,
	transaction_num int8 NULL,
	bid_price1 float8 NULL,
	bid_volume1 int8 NULL,
	bid_price2 float8 NULL,
	bid_volume2 int8 NULL,
	bid_price3 float8 NULL,
	bid_volume3 int8 NULL,
	bid_price4 float8 NULL,
	bid_volume4 int8 NULL,
	bid_price5 float8 NULL,
	bid_volume5 int8 NULL,
	ask_price1 float8 NULL,
	ask_volume1 int8 NULL,
	ask_price2 float8 NULL,
	ask_volume2 int8 NULL,
	ask_price3 float8 NULL,
	ask_volume3 int8 NULL,
	ask_price4 float8 NULL,
	ask_volume4 int8 NULL,
	ask_price5 float8 NULL,
	ask_volume5 int8 NULL,
	CONSTRAINT ods_a_stock_level1_data_unique UNIQUE (ts_code, trade_time)
);
SELECT create_hypertable('ods_a_stock_level1_data', 'trade_time', migrate_data => true);
ALTER TABLE ods_a_stock_level1_data SET (timescaledb.compress,timescaledb.compress_segmentby = 'ts_code');
SELECT add_compression_policy('ods_a_stock_level1_data', INTERVAL '15 days');


CREATE TABLE public.a_stock_5m_kline_wfq_baostock (
	trade_time timestamp NOT NULL,
	ts_code text NOT NULL,
	"open" numeric(18, 4) NULL,
	high numeric(18, 4) NULL,
	low numeric(18, 4) NULL,
	"close" numeric(18, 4) NULL,
	volume numeric(18, 4) NULL,
	amount numeric(18, 4) NULL,
	adjust_flag text NULL,
	CONSTRAINT a_stock_5m_kline_wfq_baostock_pkey PRIMARY KEY (trade_time, ts_code)
);
CREATE INDEX a_stock_5m_kline_wfq_baostock_trade_time_idx ON public.a_stock_5m_kline_wfq_baostock USING btree (trade_time);
CREATE INDEX idx_5m_kline_wfq_baostock_ts_code ON public.a_stock_5m_kline_wfq_baostock USING btree (ts_code);
SELECT create_hypertable('a_stock_5m_kline_wfq_baostock', 'trade_time');
ALTER TABLE a_stock_30m_kline_wfq_baostock SET (timescaledb.compress, timescaledb.compress_segmentby = 'ts_code');
-- SELECT add_compression_policy('a_stock_30m_kline_wfq_baostock', INTERVAL '2 days');

CREATE TABLE public.a_stock_30m_kline_wfq_baostock (
	trade_time timestamp NOT NULL,
	ts_code text NOT NULL,
	"open" float8 NULL,
	high float8 NULL,
	low float8 NULL,
	"close" float8 NULL,
	volume float8 NULL,
	amount float8 NULL,
	adjust_flag text NULL,
	CONSTRAINT a_stock_30m_kline_wfq_baostock_pkey PRIMARY KEY (trade_time, ts_code)
);
CREATE INDEX a_stock_30m_kline_wfq_baostock_trade_time_idx ON public.a_stock_30m_kline_wfq_baostock USING btree (trade_time);
CREATE INDEX idx_30m_kline_wfq_baostock_ts_code ON public.a_stock_30m_kline_wfq_baostock USING btree (ts_code);
SELECT create_hypertable('a_stock_30m_kline_wfq_baostock', 'trade_time', migrate_data => true);
ALTER TABLE a_stock_30m_kline_wfq_baostock SET (timescaledb.compress, timescaledb.compress_segmentby = 'ts_code');
SELECT add_compression_policy('a_stock_30m_kline_wfq_baostock', INTERVAL '2 days');


CREATE TABLE public.a_stock_1d_kline_wfq_baostock (
	trade_date date NOT NULL,
	ts_code text NOT NULL,
	"open" numeric NULL,
	high numeric NULL,
	low numeric NULL,
	"close" numeric NULL,
	volume int8 NULL,
	amount numeric NULL,
	turn numeric NULL,
	adjust_flag text NULL,
	pre_close numeric NULL,
	pct_chg numeric NULL,
	pe_ttm numeric NULL,
	pb_mrq numeric NULL,
	ps_ttm numeric NULL,
	pcf_ncf_ttm numeric NULL,
	trade_status text NULL,
	is_st text NULL,
	CONSTRAINT a_stock_1d_kline_wfq_baostock_pkey PRIMARY KEY (trade_date, ts_code)
);
CREATE INDEX a_stock_1d_kline_wfq_baostock_trade_date_idx ON public.a_stock_1d_kline_wfq_baostock USING btree (trade_date);
CREATE INDEX a_stock_1d_kline_wfq_baostock_ts_code_idx ON public.a_stock_1d_kline_wfq_baostock USING btree (ts_code);
SELECT create_hypertable('a_stock_1d_kline_wfq_baostock', 'trade_date');
ALTER TABLE a_stock_1d_kline_wfq_baostock SET (timescaledb.compress, timescaledb.compress_segmentby = 'ts_code');


CREATE TABLE public.a_stock_daily_basic (
	trade_date date NOT NULL,
	ts_code text NOT NULL,
	"close" numeric(18, 2) NULL,
	turnover_rate numeric(10, 4) NULL,
	turnover_rate_f numeric(10, 4) NULL,
	volume_ratio numeric(10, 4) NULL,
	pe numeric(18, 2) NULL,
	pe_ttm numeric(18, 2) NULL,
	pb numeric(18, 2) NULL,
	ps numeric(18, 2) NULL,
	ps_ttm numeric(18, 2) NULL,
	dv_ratio numeric(10, 4) NULL,
	dv_ttm numeric(10, 4) NULL,
	total_share numeric(18, 2) NULL,
	float_share numeric(18, 2) NULL,
	free_share numeric(18, 2) NULL,
	total_mv numeric(18, 2) NULL,
	circ_mv numeric(18, 2) NULL,
	circ_mv_range text NULL,
	CONSTRAINT a_stock_daily_basic_unique UNIQUE (trade_date, ts_code)
);
CREATE INDEX a_stock_daily_basic_trade_date_idx ON public.a_stock_daily_basic USING btree (trade_date);
CREATE INDEX a_stock_daily_basic_ts_code_idx ON public.a_stock_daily_basic USING btree (ts_code);
COMMENT ON COLUMN "public"."a_stock_daily_basic"."ts_code" IS 'TS股票代码';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."trade_date" IS '交易日期';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."close" IS '当日收盘价';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."turnover_rate" IS '换手率(%)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."turnover_rate_f" IS '换手率(自由流通股)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."volume_ratio" IS '量比';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."pe" IS '市盈率(总市值/净利润，亏损的PE为空)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."pe_ttm" IS '市盈率(TTM，亏损的PE为空)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."pb" IS '市净率(总市值/净资产)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."ps" IS '市销率';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."ps_ttm" IS '市销率(TTM)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."dv_ratio" IS '股息率(%)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."dv_ttm" IS '股息率(TTM)(%)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."total_share" IS '总股本(万股)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."float_share" IS '流通股本(万股)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."free_share" IS '自由流通股本(万股)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."total_mv" IS '总市值(万元)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."circ_mv" IS '流通市值(万元)';
COMMENT ON COLUMN "public"."a_stock_daily_basic"."circ_mv_range" IS '流通市值区间';
COMMENT ON TABLE "public"."a_stock_daily_basic" IS 'A股每日基础数据表';
SELECT create_hypertable('a_stock_daily_basic', 'trade_date', migrate_data => true);
ALTER TABLE a_stock_daily_basic SET (timescaledb.compress, timescaledb.compress_segmentby = 'ts_code');
SELECT add_compression_policy('a_stock_daily_basic', INTERVAL '2 days');



CREATE TABLE public.a_stock_moneyflow (
	trade_date date NOT NULL,
	ts_code text NOT NULL,
	buy_sm_vol int4 NULL,
	buy_sm_amount numeric(18, 2) NULL,
	sell_sm_vol int4 NULL,
	sell_sm_amount numeric(18, 2) NULL,
	buy_md_vol int4 NULL,
	buy_md_amount numeric(18, 2) NULL,
	sell_md_vol int4 NULL,
	sell_md_amount numeric(18, 2) NULL,
	buy_lg_vol int4 NULL,
	buy_lg_amount numeric(18, 2) NULL,
	sell_lg_vol int4 NULL,
	sell_lg_amount numeric(18, 2) NULL,
	buy_elg_vol int4 NULL,
	buy_elg_amount numeric(18, 2) NULL,
	sell_elg_vol int4 NULL,
	sell_elg_amount numeric(18, 2) NULL,
	net_mf_vol int4 NULL,
	net_mf_amount numeric(18, 2) NULL,
	CONSTRAINT a_stock_moneyflow_unique UNIQUE (trade_date, ts_code)
);
CREATE INDEX a_stock_moneyflow_trade_date_idx ON public.a_stock_moneyflow USING btree (trade_date);
CREATE INDEX a_stock_moneyflow_ts_code_idx ON public.a_stock_moneyflow USING btree (ts_code);
COMMENT ON COLUMN "public"."a_stock_moneyflow"."ts_code" IS 'TS代码';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."trade_date" IS '交易日期';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."buy_sm_vol" IS '小单买入量(手)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."buy_sm_amount" IS '小单买入金额(万元)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."sell_sm_vol" IS '小单卖出量(手)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."sell_sm_amount" IS '小单卖出金额(万元)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."buy_md_vol" IS '中单买入量(手)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."buy_md_amount" IS '中单买入金额(万元)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."sell_md_vol" IS '中单卖出量(手)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."sell_md_amount" IS '中单卖出金额(万元)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."buy_lg_vol" IS '大单买入量(手)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."buy_lg_amount" IS '大单买入金额(万元)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."sell_lg_vol" IS '大单卖出量(手)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."sell_lg_amount" IS '大单卖出金额(万元)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."buy_elg_vol" IS '特大单买入量(手)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."buy_elg_amount" IS '特大单买入金额(万元)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."sell_elg_vol" IS '特大单卖出量(手)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."sell_elg_amount" IS '特大单卖出金额(万元)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."net_mf_vol" IS '净流入量(手)';
COMMENT ON COLUMN "public"."a_stock_moneyflow"."net_mf_amount" IS '净流入额(万元)';
COMMENT ON TABLE "public"."a_stock_moneyflow" IS 'A股资金流向表。小单：5万以下 中单：5万～20万 大单：20万～100万 特大单：成交额>=100万 ，数据基于主动买卖单统计';
SELECT create_hypertable('a_stock_moneyflow', 'trade_date', migrate_data => true);
ALTER TABLE a_stock_moneyflow SET (timescaledb.compress, timescaledb.compress_segmentby = 'ts_code');
SELECT add_compression_policy('a_stock_moneyflow', INTERVAL '2 days');


CREATE TABLE public.a_stock_basic (
	ts_code text NULL,
	symbol text NULL,
	"name" text NULL,
	area text NULL,
	industry text NULL,
	cnspell text NULL,
	market text NULL,
	list_date text NULL,
	act_name text NULL,
	act_ent_type text NULL,
	CONSTRAINT a_stock_basic_unique UNIQUE (ts_code)
);
CREATE INDEX a_stock_basic_name_idx ON public.a_stock_basic USING btree (name);
CREATE INDEX a_stock_basic_ts_code_idx ON public.a_stock_basic USING btree (ts_code);




CREATE TABLE "public"."a_stock_daily_k" (
  "ts_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL,
  "trade_date" varchar(10) COLLATE "pg_catalog"."default" NOT NULL,
  "open" numeric(18,2),
  "high" numeric(18,2),
  "low" numeric(18,2),
  "close" numeric(18,2),
  "pre_close" numeric(18,2),
  "change" numeric(18,2),
  "pct_chg" numeric(10,4),
  "vol" numeric(18,2),
  "amount" numeric(18,2),
  CONSTRAINT "a_stock_daily_k_pkey" PRIMARY KEY ("ts_code", "trade_date")
);
ALTER TABLE "public"."a_stock_daily_k" OWNER TO "postgres";
CREATE INDEX "idx_trade_date_d6f69" ON "public"."a_stock_daily_k" USING btree ("trade_date" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
CREATE INDEX "idx_ts_code_d6f69" ON "public"."a_stock_daily_k" USING btree ("ts_code" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
COMMENT ON COLUMN "public"."a_stock_daily_k"."ts_code" IS '股票代码';
COMMENT ON COLUMN "public"."a_stock_daily_k"."trade_date" IS '交易日期';
COMMENT ON COLUMN "public"."a_stock_daily_k"."open" IS '开盘价';
COMMENT ON COLUMN "public"."a_stock_daily_k"."high" IS '最高价';
COMMENT ON COLUMN "public"."a_stock_daily_k"."low" IS '最低价';
COMMENT ON COLUMN "public"."a_stock_daily_k"."close" IS '收盘价';
COMMENT ON COLUMN "public"."a_stock_daily_k"."pre_close" IS '昨收价(除权价前复权)';
COMMENT ON COLUMN "public"."a_stock_daily_k"."change" IS '涨跌额';
COMMENT ON COLUMN "public"."a_stock_daily_k"."pct_chg" IS '涨跌幅(基于除权后的昨收计算的涨跌幅)';
COMMENT ON COLUMN "public"."a_stock_daily_k"."vol" IS '成交量(手)';
COMMENT ON COLUMN "public"."a_stock_daily_k"."amount" IS '成交额(千元)';
COMMENT ON TABLE "public"."a_stock_daily_k" IS 'A股每日K线数据表';


CREATE TABLE "public"."a_stock_index_dailybasic" (
  "index_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL,
  "trade_date" varchar(8) COLLATE "pg_catalog"."default" NOT NULL,
  "total_mv" numeric(18,2),
  "float_mv" numeric(18,2),
  "total_share" numeric(18,2),
  "float_share" numeric(18,2),
  "free_share" numeric(18,2),
  "turnover_rate" numeric(10,4),
  "turnover_rate_f" numeric(10,4),
  "pe" numeric(10,4),
  "pe_ttm" numeric(10,4),
  "pb" numeric(10,4),
  "index_name" varchar COLLATE "pg_catalog"."default",
  CONSTRAINT "index_dailybasic_pkey" PRIMARY KEY ("index_code", "trade_date")
);
ALTER TABLE "public"."a_stock_index_dailybasic" OWNER TO "postgres";
CREATE INDEX "idx_trade_date_gkt5c" ON "public"."a_stock_index_dailybasic" USING btree ("trade_date" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
CREATE INDEX "idx_ts_code_gkt5c" ON "public"."a_stock_index_dailybasic" USING btree ("index_code" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."index_code" IS '指数代码';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."trade_date" IS '交易日期';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."total_mv" IS '当日总市值(元)';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."float_mv" IS '当日流通市值(元)';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."total_share" IS '当日总股本(股)';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."float_share" IS '当日流通股本(股)';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."free_share" IS '当日自由流通股本(股)';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."turnover_rate" IS '换手率';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."turnover_rate_f" IS '换手率(基于自由流通股本)';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."pe" IS '市盈率';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."pe_ttm" IS '市盈率TTM';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."pb" IS '市净率';
COMMENT ON COLUMN "public"."a_stock_index_dailybasic"."index_name" IS '指数名称';
COMMENT ON TABLE "public"."a_stock_index_dailybasic" IS '指数每日基础数据表';


CREATE TABLE "public"."a_stock_index_marketvalue_score" (
  "index_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL,
  "index_name" varchar(100) COLLATE "pg_catalog"."default",
  "trade_date" varchar(8) COLLATE "pg_catalog"."default" NOT NULL,
  "流通市值(亿元)" numeric(18,2),
  "流通市值分位数" numeric(5,2),
  "换手率(%)" numeric(10,2),
  "换手率分位数" numeric(5,2),
  "平均价格" numeric(18,4),
  CONSTRAINT "index_marketvalue_score_pkey" PRIMARY KEY ("index_code", "trade_date")
);
ALTER TABLE "public"."a_stock_index_marketvalue_score" OWNER TO "postgres";
CREATE INDEX "idx_index_code_u92xb" ON "public"."a_stock_index_marketvalue_score" USING btree ("index_code" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
CREATE INDEX "idx_trade_date_u92xb" ON "public"."a_stock_index_marketvalue_score" USING btree ("trade_date" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
COMMENT ON COLUMN "public"."a_stock_index_marketvalue_score"."index_code" IS '指数代码';
COMMENT ON COLUMN "public"."a_stock_index_marketvalue_score"."index_name" IS '指数名称';
COMMENT ON COLUMN "public"."a_stock_index_marketvalue_score"."trade_date" IS '交易日期(格式：yyyyMMdd)';
COMMENT ON COLUMN "public"."a_stock_index_marketvalue_score"."流通市值(亿元)" IS '流通市值(单位：亿)';
COMMENT ON COLUMN "public"."a_stock_index_marketvalue_score"."流通市值分位数" IS '流通市值分位数';
COMMENT ON COLUMN "public"."a_stock_index_marketvalue_score"."换手率(%)" IS '换手率(%)';
COMMENT ON COLUMN "public"."a_stock_index_marketvalue_score"."换手率分位数" IS '换手率分位数';
COMMENT ON COLUMN "public"."a_stock_index_marketvalue_score"."平均价格" IS '平均价格';
COMMENT ON TABLE "public"."a_stock_index_marketvalue_score" IS '指数流动性分析表';


CREATE TABLE "public"."a_stock_market_moneyflow_score" (
  "trade_date" varchar(8) COLLATE "pg_catalog"."default" NOT NULL,
  "净值(亿元)" numeric(18,2),
  "净值分位数" numeric(5,2),
  CONSTRAINT "a_stock_market_moneyflow_score_pkey" PRIMARY KEY ("trade_date")
);
ALTER TABLE "public"."a_stock_market_moneyflow_score" OWNER TO "postgres";
CREATE INDEX "idx_trade_date_hbb57" ON "public"."a_stock_market_moneyflow_score" USING btree ("trade_date" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
COMMENT ON COLUMN "public"."a_stock_market_moneyflow_score"."trade_date" IS '交易日期(格式：yyyyMMdd)';
COMMENT ON COLUMN "public"."a_stock_market_moneyflow_score"."净值(亿元)" IS '净值(单位：亿元)';
COMMENT ON COLUMN "public"."a_stock_market_moneyflow_score"."净值分位数" IS '净值分位数';
COMMENT ON TABLE "public"."a_stock_market_moneyflow_score" IS 'A股市场资金流动得分表';




CREATE TABLE "public"."a_stock_moneyflow_industry_score" (
  "trade_date" varchar(10) COLLATE "pg_catalog"."default" NOT NULL,
  "industry_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL,
  "industry" varchar(50) COLLATE "pg_catalog"."default" NOT NULL,
  "排名" int4 NOT NULL,
  "净额(亿元)" numeric(18,2) NOT NULL,
  "净额分位数" numeric(10,4) NOT NULL,
  "过去5日分位数" numeric(10,4) NOT NULL,
  "过去4日分位数" numeric(10,4) NOT NULL,
  "过去3日分位数" numeric(10,4) NOT NULL,
  "过去2日分位数" numeric(10,4) NOT NULL,
  "过去1日分位数" numeric(10,4) NOT NULL,
  CONSTRAINT "a_stock_moneyflow_industry_score_pkey" PRIMARY KEY ("trade_date", "industry_code")
);
ALTER TABLE "public"."a_stock_moneyflow_industry_score" OWNER TO "postgres";
CREATE INDEX "idx_industry_code_fg2hs" ON "public"."a_stock_moneyflow_industry_score" USING btree ("industry_code" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
CREATE INDEX "idx_trade_date_fg2hs" ON "public"."a_stock_moneyflow_industry_score" USING btree ("trade_date" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
CREATE INDEX "idx_排名_fg2hs" ON "public"."a_stock_moneyflow_industry_score" USING btree ("排名" "pg_catalog"."int4_ops" ASC NULLS LAST);
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."trade_date" IS '交易日期';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."industry_code" IS '行业代码';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."industry" IS '行业名称';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."排名" IS '排名';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."净额(亿元)" IS '净额（亿元）';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."净额分位数" IS '净额分位数';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."过去5日分位数" IS '过去5日分位数';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."过去4日分位数" IS '过去4日分位数';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."过去3日分位数" IS '过去3日分位数';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."过去2日分位数" IS '过去2日分位数';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_score"."过去1日分位数" IS '过去1日分位数';
COMMENT ON TABLE "public"."a_stock_moneyflow_industry_score" IS 'A股行业资金流向分位数表';



CREATE TABLE "public"."a_stock_moneyflow_industry_ths" (
  "trade_date" varchar(10) COLLATE "pg_catalog"."default" NOT NULL,
  "industry_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL,
  "industry" varchar(50) COLLATE "pg_catalog"."default" NOT NULL,
  "lead_stock" varchar(50) COLLATE "pg_catalog"."default" NOT NULL,
  "close" numeric(18,2) NOT NULL,
  "pct_change" numeric(10,4) NOT NULL,
  "company_num" int4 NOT NULL,
  "pct_change_stock" numeric(10,4) NOT NULL,
  "close_price" numeric(18,2) NOT NULL,
  "net_buy_amount" numeric(18,2) NOT NULL,
  "net_sell_amount" numeric(18,2) NOT NULL,
  "net_amount" numeric(18,2) NOT NULL,
  CONSTRAINT "a_stock_moneyflow_industry_ths_pkey" PRIMARY KEY ("trade_date", "industry_code")
);
ALTER TABLE "public"."a_stock_moneyflow_industry_ths" OWNER TO "postgres";
CREATE INDEX "idx_industry_7feuv" ON "public"."a_stock_moneyflow_industry_ths" USING btree ("industry" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
CREATE INDEX "idx_trade_date_7feuv" ON "public"."a_stock_moneyflow_industry_ths" USING btree ("trade_date" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
CREATE INDEX "idx_ts_code_7feuv" ON "public"."a_stock_moneyflow_industry_ths" USING btree ("industry_code" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."trade_date" IS '交易日期';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."industry_code" IS '板块代码';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."industry" IS '板块名称';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."lead_stock" IS '领涨股票名称';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."close" IS '收盘指数';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."pct_change" IS '指数涨跌幅';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."company_num" IS '公司数量';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."pct_change_stock" IS '领涨股涨跌幅';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."close_price" IS '领涨股最新价';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."net_buy_amount" IS '流入资金(亿元)';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."net_sell_amount" IS '流出资金(亿元)';
COMMENT ON COLUMN "public"."a_stock_moneyflow_industry_ths"."net_amount" IS '净额(亿元)';
COMMENT ON TABLE "public"."a_stock_moneyflow_industry_ths" IS 'A股行业资金流向表(同花顺)';


CREATE TABLE "public"."a_stock_moneyflow_score" (
  "trade_date" text COLLATE "pg_catalog"."default",
  "ts_code" text COLLATE "pg_catalog"."default",
  "统计天数" int8,
  "排名" int8,
  "市值区间" text COLLATE "pg_catalog"."default",
  "特大单/市值" float8,
  "特大单/市值_Z" float8,
  "大单/市值" float8,
  "大单/市值_Z" float8,
  "中单/市值" float8,
  "中单/市值_Z" float8,
  "小单/市值" float8,
  "小单/市值_Z" float8,
  "换手率均值" float8,
  "换手率均值_Z" float8,
  "量比均值" float8,
  "量比均值_Z" float8,
  "成交额分位数" float8,
  "净流入分位数" float8,
  "综合得分" float8,
  CONSTRAINT "a_stock_moneyflow_score_unique" UNIQUE ("trade_date", "ts_code")
);
ALTER TABLE "public"."a_stock_moneyflow_score" OWNER TO "postgres";
CREATE INDEX "idx_trade_date_q9wmj" ON "public"."a_stock_moneyflow_score" USING btree ("trade_date" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
CREATE INDEX "idx_ts_code_q9wmj" ON "public"."a_stock_moneyflow_score" USING btree ("ts_code" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);




CREATE TABLE "public"."ths_index_members" (
  "index_code" varchar(50) COLLATE "pg_catalog"."default",
  "index_name" varchar(50) COLLATE "pg_catalog"."default",
  "index_type" varchar(50) COLLATE "pg_catalog"."default",
  "ts_count" varchar(50) COLLATE "pg_catalog"."default",
  "list_date" varchar(50) COLLATE "pg_catalog"."default",
  "ts_code" varchar(20) COLLATE "pg_catalog"."default",
  "ts_name" varchar(50) COLLATE "pg_catalog"."default",
  CONSTRAINT "ths_index_members_unique" UNIQUE ("index_code", "ts_code")
);
ALTER TABLE "public"."ths_index_members" OWNER TO "postgres";
CREATE INDEX "ths_index_members_index_code_idx" ON "public"."ths_index_members" USING btree ("index_code" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
CREATE INDEX "ths_index_members_ts_code_idx" ON "public"."ths_index_members" USING btree ("ts_code" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);
CREATE INDEX "ths_index_members_ts_name_idx" ON "public"."ths_index_members" USING btree ("ts_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST);






-- IMMUTABLE函数,timestamptz转日期
CREATE OR REPLACE FUNCTION immutable_date(timestamptz) 
RETURNS date AS $$
  SELECT ($1)::date;
$$ LANGUAGE SQL IMMUTABLE;

-- 创建a_stock_level1_data超表
CREATE TABLE a_stock_level1_data (
    ts_code VARCHAR(16) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    last_price DOUBLE PRECISION,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    pre_close DOUBLE PRECISION,
    volume BIGINT,
    amount DOUBLE PRECISION,
    pvolume BIGINT,              -- 原始成交量    
    transaction_num BIGINT,      -- 成交笔数
    stock_status INTEGER,        -- 股票状态
    bid_price1 DOUBLE PRECISION,
    bid_volume1 BIGINT,
    bid_price2 DOUBLE PRECISION,
    bid_volume2 BIGINT,
    bid_price3 DOUBLE PRECISION,
    bid_volume3 BIGINT,
    bid_price4 DOUBLE PRECISION,
    bid_volume4 BIGINT,
    bid_price5 DOUBLE PRECISION,
    bid_volume5 BIGINT,
    ask_price1 DOUBLE PRECISION,
    ask_volume1 BIGINT,
    ask_price2 DOUBLE PRECISION,
    ask_volume2 BIGINT,
    ask_price3 DOUBLE PRECISION,
    ask_volume3 BIGINT,
    ask_price4 DOUBLE PRECISION,
    ask_volume4 BIGINT,
    ask_price5 DOUBLE PRECISION,
    ask_volume5 BIGINT,
    etl_time TIMESTAMPTZ NOT NULL
);

SELECT create_hypertable('a_stock_level1_data', 'timestamp');
CREATE INDEX idx_a_stock_level1_data_ts_code      ON a_stock_level1_data (ts_code);
CREATE INDEX idx_a_stock_level1_data_timestamp    ON a_stock_level1_data (timestamp DESC);
CREATE INDEX idx_a_stock_level1_data_ts_code_time ON a_stock_level1_data (ts_code, timestamp DESC);
CREATE INDEX idx_a_stock_level1_data_date ON a_stock_level1_data(immutable_date("timestamp") DESC);
ALTER TABLE a_stock_level1_data SET (timescaledb.compress,timescaledb.compress_segmentby = 'ts_code');
SELECT add_compression_policy('a_stock_level1_data', INTERVAL '7 days');







CREATE TABLE ths_limit_list (
    "trade_date" date NOT NULL,       -- 交易日期
    ts_code TEXT NOT NULL,            -- 股票代码
    name TEXT NOT NULL,               -- 股票名称
    price DOUBLE PRECISION NOT NULL,  -- 收盘价(元)
    pct_chg DOUBLE PRECISION NOT NULL,-- 涨跌幅%
    open_num INTEGER NOT NULL,        -- 打开次数
    lu_desc TEXT NOT NULL,            -- 涨停原因
    limit_type TEXT NOT NULL,         -- 板单类别
    tag TEXT NOT NULL,                -- 涨停标签
    status TEXT NOT NULL,             -- 涨停状态(N连板、一字板)
    first_lu_time TEXT,               -- 首次涨停时间
    last_lu_time TEXT,                -- 最后涨停时间
    first_ld_time TEXT,               -- 首次跌停时间
    last_ld_time TEXT,                -- 最后涨停时间
    limit_order DOUBLE PRECISION NOT NULL, -- 封单量(元)
    limit_amount DOUBLE PRECISION NOT NULL,-- 封单额(元)
    turnover_rate DOUBLE PRECISION NOT NULL, -- 换手率%
    free_float DOUBLE PRECISION NOT NULL,   -- 实际流通(元)
    lu_limit_order DOUBLE PRECISION NOT NULL, -- 最大封单(元)
    limit_up_suc_rate DOUBLE PRECISION NOT NULL, -- 近一年涨停封板率
    turnover DOUBLE PRECISION NOT NULL,      -- 成交额
    rise_rate DOUBLE PRECISION,              -- 涨速
    sum_float DOUBLE PRECISION,              -- 总市值(亿元)
    market_type TEXT NOT NULL                -- 股票类型：HS沪深主板、GEM创业板、STAR科创板
);


CREATE TABLE public.a_stock_qmt_sector (
	index_name text NULL,
	ts_code text NULL
);

CREATE TABLE public.a_index_1day_kline_baostock (
	trade_date date NOT NULL,
	ts_code varchar(20) NOT NULL,
	"open" numeric(12, 4) NULL,
	high numeric(12, 4) NULL,
	low numeric(12, 4) NULL,
	"close" numeric(12, 4) NULL,
	volume numeric(20, 4) NULL,
	amount numeric(20, 4) NULL,
	CONSTRAINT a_index_1day_kline_baostock_tmp_pkey PRIMARY KEY (ts_code, trade_date)
);



