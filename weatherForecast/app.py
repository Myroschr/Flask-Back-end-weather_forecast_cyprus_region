from flask import Flask, request, jsonify
import mysql.connector
from datetime import datetime
from weather_fetcher import save_db_data
import threading
import time

app = Flask(__name__)

def get_db_connection():
    return mysql.connector.connect(user='root', password='your_password',
                                   host='127.0.0.1', database='weather_db')

@app.route('/locations', methods=['GET'])
def list_locations():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT location FROM weather_data")
    locations = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify(locations)

@app.route('/latest_forecast', methods=['GET'])
def latest_forecast():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT location, date, temperature 
    FROM weather_data 
    WHERE (location, date) IN (
        SELECT location, MAX(date) 
        FROM weather_data 
        GROUP BY location
    )
    """)
    forecasts = [{'location': row[0], 'date': row[1], 'temperature': row[2]} for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify(forecasts)

@app.route('/average_temperature', methods=['GET'])
def average_temperature():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT location, AVG(temperature) as avg_temp
    FROM (
        SELECT location, date, temperature 
        FROM weather_data 
        ORDER BY date DESC
        LIMIT 3
    ) as subquery
    GROUP BY location
    """)
    averages = [{'location': row[0], 'avg_temp': row[1]} for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify(averages)

@app.route('/top_locations', methods=['GET'])
def top_locations():
    metric = request.args.get('metric', 'temperature')
    n = int(request.args.get('n', 3))
    conn = get_db_connection()
    cursor = conn.cursor()
    query = f"""
    SELECT location, AVG({metric}) as avg_metric
    FROM weather_data
    GROUP BY location
    ORDER BY avg_metric DESC
    LIMIT %s
    """
    cursor.execute(query, (n,))
    top_locations = [{'location': row[0], 'avg_metric': row[1]} for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify(top_locations)

if __name__ == "__main__":
    app.run(debug=True)
