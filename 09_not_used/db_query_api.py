#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
import psycopg2
import psycopg2.extras
import pandas as pd
from flask import Flask, request, jsonify
import logging
import json
from datetime import datetime, date

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 创建Flask应用
app = Flask(__name__)

# 读取配置文件
def get_db_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return {
        'host': config['postgresql']['host'],
        'port': config['postgresql']['port'],
        'user': config['postgresql']['user'],
        'password': config['postgresql']['password'],
        'database': config['postgresql']['database']
    }

# 数据库连接函数
def get_db_connection():
    db_config = get_db_config()
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败: {str(e)}")
        raise

# 自定义JSON编码器，处理日期和时间类型
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

app.json_encoder = CustomJSONEncoder

# 执行SQL查询并返回结果
def execute_query(sql, params=None):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql, params)
        
        # 检查是否是SELECT查询
        if sql.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            return {'success': True, 'data': results, 'rowCount': len(results)}
        else:
            # 对于非SELECT查询，提交事务并返回影响的行数
            conn.commit()
            return {'success': True, 'rowCount': cursor.rowcount, 'message': f'操作成功，影响了 {cursor.rowcount} 行'}
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"查询执行失败: {str(e)}")
        return {'success': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()

# 查询接口 - JSON格式返回
@app.route('/api/query', methods=['POST'])
def query_api():
    try:
        data = request.get_json()
        if not data or 'sql' not in data:
            return jsonify({'success': False, 'error': '缺少SQL查询语句'}), 400
        
        sql = data['sql']
        params = data.get('params', None)
        
        # 执行查询
        result = execute_query(sql, params)
        return jsonify(result)
    except Exception as e:
        logger.error(f"API错误: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

# 查询接口 - Pandas DataFrame格式返回
@app.route('/api/query/dataframe', methods=['POST'])
def query_dataframe_api():
    try:
        data = request.get_json()
        if not data or 'sql' not in data:
            return jsonify({'success': False, 'error': '缺少SQL查询语句'}), 400
        
        sql = data['sql']
        params = data.get('params', None)
        
        # 使用pandas直接查询
        conn = get_db_connection()
        try:
            df = pd.read_sql_query(sql, conn, params=params)
            return jsonify({
                'success': True, 
                'data': df.to_dict(orient='records'), 
                'columns': df.columns.tolist(),
                'rowCount': len(df)
            })
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"DataFrame API错误: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

# 健康检查接口
@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=18000, debug=True) 