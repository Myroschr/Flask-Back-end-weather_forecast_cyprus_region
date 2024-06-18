import requests
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector

weather_map = {
    101: 'Clear sky',
    102: 'Light clouds',
    103: 'Partly cloudy',
    104: 'Cloudy',
    105: 'Rain',
    106: 'Rain and snow / sleet',
    107: 'Snow',
    108: 'Rain shower',
    109: 'Snow shower',
    110: 'Sleet shower',
    111: 'Light Fog',
    112: 'Dense fog',
    113: 'Freezing rain',
    114: 'Thunderstorms',
    115: 'Drizzle',
    116: 'Sandstorm'
}


username='your_username'
password='your_password'

def dates():
    def date_str(date):
        return date.strftime("%Y-%m-%d")
    start_day = datetime.now()
    end_day = start_day + timedelta(7)
    return date_str(start_day),date_str(end_day)

def get_url_data(location):
    try:
        start_day,end_day =dates()
        url = f'https://api.meteomatics.com/{start_day}T00:00:00Z--{end_day}T00:00:00Z:PT24H/t_2m:C,weather_symbol_3h:idx/{location}/json'
        response  = requests.get(url,auth=(username,password))
        if response.status_code == 200:
            
            return response.json()
        else:
            return f"Failed to retrieve data: {response.status_code}"
    except requests.exceptions.RequestException as error:
        print(f"Failed to retrieve data: {error}")
        return None


    
def prepare_data(coord,location):
    data = get_url_data(coord)
    if data is None:
        return pd.DataFrame([],columns=['date','temperature','weather'])
    
    temp = pd.DataFrame(data['data'][0]['coordinates'][0]['dates'])
    temp.columns = ['date','temperature']
    weather = pd.DataFrame(data['data'][1]['coordinates'][0]['dates'])
    weather.columns = ['date','weather']
    data = pd.merge(temp, weather,on='date',how='left')
    data.insert(0,'location',location)
    data['date'] = pd.to_datetime(data['date'])
    data['date'] = data['date'].dt.strftime('%Y-%m-%d')
    data['weather'] = data['weather'].map(weather_map)
    return data

def region_weather():
    regions = {
        'limassol':'34.6786,33.0413',
        'larnaca':'34.9182,33.6201',
        'pafos':'34.7754,32.4218'
               }
    limassolWeather = prepare_data(regions['limassol'],'Limassol')
    larnacaWeather = prepare_data(regions['larnaca'],'Larnaca')
    pafosWeather = prepare_data(regions['pafos'],'Pafos')
    
    weatherdata = pd.concat([limassolWeather,larnacaWeather,pafosWeather]).reset_index(drop=True)
    return weatherdata

def save_db_data():
    weatherdata = region_weather()
    
    conn = mysql.connector.connect(user='root', password='your_password',
     
                                   host='127.0.0.1')
    if conn:
        
        cursor = conn.cursor()
        
        cursor.execute("""CREATE DATABASE IF NOT EXISTS weather_db""")
        print("Database 'weather_db' created or already exists")
    
        conn.database = 'weather_db'
        
    
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                location VARCHAR(255),
                date DATE,
                temperature FLOAT,
                weather VARCHAR(255)
            )""")
        cursor.execute("DELETE FROM weatherforecast.weather_data;")
        cursor.execute("ALTER TABLE weatherforecast.weather_data AUTO_INCREMENT = 1;")
            
        for row in weatherdata.itertuples(index=False):    
            insert_query = "INSERT INTO weatherforecast.weather_data (location, date, temperature, weather) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, row)
        conn.commit()
        cursor.close()
        conn.close()

# if __name__ == "__main__":
#     save_db_data()
    
