import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


class Geocoder:
    def get_lat_lon(self, city):
        if city == "Lviv":
            return [49.8397, 24.0297]
        if city == "Kharkiv":
            return [49.9935, 36.2304]
        if city == "Odesa":
            return [46.4825, 30.7233]
        if city == "Kyiv":
            return [50.4501, 30.5234]
        if city == "Zhmerynka":
            return [49.0391, 28.1086]
        raise ValueError(f"City {city} is not supported for geocoding")


def create_db():
    return PostgresOperator(
        task_id="create_db",
        sql="""
             CREATE TABLE IF NOT EXISTS weather (
                city VARCHAR(255),
                processed_at TIMESTAMP,
                humidity FLOAT,
                cloudiness FLOAT,
                wind_speed FLOAT,
                temperature FLOAT,
                PRIMARY KEY (city, processed_at));
        """
    )


def get_data(city):
    def fetch(ti, **kwargs):
        execution_date = kwargs['execution_date']
        url = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
        [lat, lon] = Geocoder().get_lat_lon(city)
        params = {"lat": lat, "lon": lon, "appid": Variable.get("WEATHER_API_KEY"), "dt": int(execution_date.timestamp())}
        response = requests.get(url, params).json()
        data = response["data"][0]
        ti.xcom_push("processed_at", data["dt"])
        ti.xcom_push("humidity", data["humidity"])
        ti.xcom_push("cloudiness", data["clouds"])
        ti.xcom_push("wind_speed", data["wind_speed"])
        ti.xcom_push("temperature", data["temp"])
    return PythonOperator(
        task_id=f"get_data_for_{city}",
        python_callable=fetch
    )


#

def insert_data(city):
    task_ids = 'get_data_for_'+city
    return PostgresOperator(
        task_id=f"insert_data_for_{city}",
        sql=f"""
            INSERT INTO weather VALUES (
                '{ city }',
                to_timestamp({{{{ ti.xcom_pull(key='processed_at', task_ids='{task_ids}') }}}}),
                {{{{ ti.xcom_pull(key='humidity', task_ids='{task_ids}') }}}},
                {{{{ ti.xcom_pull(key='cloudiness', task_ids='{task_ids}') }}}},
                {{{{ ti.xcom_pull(key='wind_speed', task_ids='{task_ids}') }}}},
                {{{{ ti.xcom_pull(key='temperature', task_ids='{task_ids}') }}}}
            );
        """
    )


with DAG(dag_id="weather_dag", schedule_interval="@daily", start_date=days_ago(2)) as dag:
    cities = ["Lviv", "Kyiv", "Kharkiv", "Odesa", "Zhmerynka"]
    create_db() >> [get_data(city) >> insert_data(city) for city in cities]

