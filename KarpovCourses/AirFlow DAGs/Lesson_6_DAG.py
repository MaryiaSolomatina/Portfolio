"""Задание ETL - создание DAG в Airflow, который будет считать метрики каждый день за вчера

1. Для каждого юзера ленты новостей расчитать число просмотров и лайков контента, а для пользователей сервиса сообщений  расчитать количество полученных и отправленных ообщений,
а также скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка в отдельном таске. Объединить две таблицы в одну

2. Для этой таблицы расчитать все метрики в разрезе по полу, возрасту и ОС устройства, создав три разных таска на каждый срез

3. Финальные данные со всеми метриками записать в отдельную таблицу в ClickHouse

4. Каждый день таблица должна дополняться новыми данными
"""

# импорт необходимых библиотек
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Подключение к ClickHouse
connection = {
    'host': '',
    'password': '',
    'user': '',
    'database': ''
}

connection_test = {'host': '',
                      'database':'',
                      'user':'', 
                      'password':''
}

# Дефолтные параметры, которые прокидываются в таски

default_args = {
    'owner': 'm_solomatina_17',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 5),
    'start_date': datetime(2023, 4, 14)
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

# Первый запрос (для каждого юзера подсчитать число просмотров и лайков контента за вчерашний день)
query_1 = """SELECT
                        DISTINCT user_id,
                        toDate(time) as event_date,
                        gender,
                        age,
                        os,
                        SUM(action = 'view') as views,
                        SUM(action = 'like') as likes
                FROM simulator_20230320.feed_actions
                WHERE toDate(time) = yesterday() 
                GROUP BY user_id, event_date, gender, age, os"""

# Второй запрос (для каждого юзера посчитать, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему - за вчерашний день)
query_2 = """SELECT event_date, 
                    user_id,
                    os,
                    gender,
                    age,
                    messages_received,
                    messages_sent,
                    users_received,
                    users_sent
            FROM

                    (SELECT 
                            MAX(toDate(time)) AS event_date,
                            user_id,
                            MAX(os) AS os,
                            MAX(gender) AS gender,
                            MAX(age) AS age,
                            count(reciever_id) AS messages_sent,
                            count(DISTINCT reciever_id) AS users_sent
                    FROM simulator_20230320.message_actions
                    GROUP BY user_id
                    HAVING toDate(time) = yesterday()) AS tab1

                    JOIN

                    (SELECT 
                            MAX(toDate(time)) AS event_date,
                            reciever_id,
                            count(user_id) AS messages_received,
                            count(DISTINCT user_id) AS users_received
                    FROM simulator_20230320.message_actions
                    GROUP BY reciever_id
                    HAVING toDate(time) = yesterday()) AS tab2

                    ON tab1.user_id = tab2.reciever_id"""

# Дефолтные параметры, которые используются дальше
common_metrics = ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']

# Создание DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def main_func():

    # Extract первого запроса (feed_actions)
    @task()
    def extract_fa():     
        df_fa = ph.read_clickhouse(query = query_1, connection=connection)
        return df_fa

    # Extract второго запроса (message_actions) 
    @task()
    def extract_ma():     
        df_ma = ph.read_clickhouse(query = query_2, connection=connection)
        return df_ma
    
    # Объединение двух запросов в один датасет
    @task()
    def merge_df(df_fa, df_ma):
        df_merged = df_fa.merge(df_ma, how = 'outer', on = ['user_id','os','gender','age','event_date']).fillna(0)
        return df_merged

    # Рассчет метрик в разрезе пола, возраста и ОС устройства
    
    # метрики пола
    @task
    def gender(df_merged):
        df_gender = df_merged.groupby(['event_date','gender'], as_index=False)[common_metrics].sum().rename(columns={'gender':'dimension_value'})
        df_gender.insert(loc=1, column='dimension', value='gender')
        return df_gender
    
    # метрики возраста
    @task
    def age(df_merged):
        df_age = df_merged.groupby(['event_date','age'], as_index=False)[common_metrics].sum().rename(columns={'age':'dimension_value'})
        df_age.insert(loc=1, column='dimension', value='age')
        return df_age
    # метрики ОС
    @task
    def os(df_merged):
        df_os = df_merged.groupby(['event_date','os'], as_index=False)[common_metrics].sum().rename(columns={'os':'dimension_value'})
        df_os.insert(loc=1, column='dimension', value='os')
        return df_os
    
    # Объединение срезов
    @task
    def concat(df_os, df_age, df_gender):
        final_df = pd.concat([df_os, df_age, df_gender])
        final_df [common_metrics] = final_df [common_metrics].astype('int64')
        return final_df
      
    #  Запись финальных данных со всеми метриками в отдельную таблицу в ClickHouse
    @task
    def to_clickhouse(final_df):
        query_3 = """
            CREATE TABLE IF NOT EXISTS test.m_solomatina (
            event_date String, 
            dimension String, 
            dimension_value String, 
            views Int64, 
            likes Int64, 
            messages_received Int64,
            messages_sent Int64, 
            users_received Int64, 
            users_sent Int64                    
            )
            engine = MergeTree()
            order by event_date"""
        ph.execute(query = query_3, connection = connection_test)
        ph.to_clickhouse(df = final_df, table = 'm_solomatina', connection = connection_test, index = False)

    df_fa = extract_fa()
    df_ma = extract_ma()
    df_merged = merge_df(df_fa, df_ma)
    df_gender = gender(df_merged)
    df_age = age(df_merged)
    df_os = os(df_merged)
    final_df = concat(df_os, df_age, df_gender)
    to_clickhouse(final_df)        
        
main_func = main_func()     

