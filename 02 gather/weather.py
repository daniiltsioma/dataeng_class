import requests, json
from datetime import datetime

api_key = "e6319c3085028ffe17e5963d41c6653c"
lat = "45.523064"
lon = "-122.676483"

url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}"

# current weather
current_weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"

current_weather_response = requests.get(current_weather_url)

current_result = current_weather_response.json()

rainy = set(['Rain', 'Thunderstorm', 'Drizzle'])

if current_result['cod'] != '404':
    if current_result['weather'][0]['main'] in rainy:
        print("It is raining in Portland right now.")
    else:
        print("It is not raining in Portland right now.")


d = datetime.now().date()
wd = int(d.strftime('%w'))

weather_5d_url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}"

weather_5d_response = requests.get(weather_5d_url)
weather_5d_result = weather_5d_response.json()

l = weather_5d_result['list']

for i in l:
    wd = int(datetime.fromtimestamp(i['dt']).strftime('%w'))
    h = int(datetime.fromtimestamp(i['dt']).strftime('%H'))
    weekday = "Tuesday" if wd == 2 else "Thursday"

    if (wd == 2 or wd == 4) and (h >= 14 and h <= 16):
        if i['weather'][0]['main'] in rainy:
            print(f"It will be raining during the next class on {weekday}.")
        else:
            print(f"It will not be raining during the next class on {weekday}.")
