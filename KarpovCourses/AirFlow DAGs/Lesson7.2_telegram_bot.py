"""Задание DAG в Airflow - автоматическая отправка аналитической сводки в телеграм каждое утро

Написать скрипт для сборки отчета по сервису ленты новостей. Отчет должен состоять из двух частей:

* текст с информацией о значениях ключевых метрик за предыдущий день
* график с значениями метрик за предыдущие 7 дней

Ключевые метрики: 
* DAU 
* Просмотры
* Лайки
* CTR
"""

# Импорт библиотек
import telegram
import io

import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, date

from airflow.decorators import dag, task, task_group

# Параметры бота
my_token = '' 
bot = telegram.Bot(token = my_token) 
chat_id = ''

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'm-solomatina-17',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta (minutes = 5),
    'start_date': datetime (2022, 4, 20),
}

# Интервал запуска DAG'а каждый день в 11:00
schedule_interval = "0 11 * * *"

# Соединение с clickhouse
connection = {
                'host': '',
                'password': '',
                'user': '',
                'database': ''
            }

# Запрос №10 - распределение количества действий пользователей за последние 7 дней (лайки, просмотры, сообщения) 
query_10 = """ WITH tab1 AS 
                (SELECT *, uniq(user_id) over (partition by date) as DAU
                              FROM
                                  (SELECT user_id,
                                          toDate(time) AS date,
                                          action,
                                          os,
                                          source
                                   FROM simulator_20230320.feed_actions
                                   UNION ALL SELECT user_id,
                                                    toDate(time) AS date,
                                                    CASE
                                                        WHEN reciever_id>0 THEN 'message'
                                                        ELSE 'n'
                                                    END AS action,
                                                    os,
                                                    source
                                   FROM simulator_20230320.message_actions)
                            WHERE (date BETWEEN yesterday()-6 AND yesterday())
                            ORDER BY date)
                SELECT toString(date) as date, 
                      action, 
                      uniq(user_id) as num_users, 
                      count(user_id) as num_actions,  
                      max(DAU) as DAU,
                      round(num_actions/DAU,2) as per_user
                FROM tab1
                GROUP BY date, action  
                ORDER BY date, action 
        """   
# Запрос №12 - распределение количества действий пользователей в разрезе ОС за последние 7 дней (iOS/Android) 
query_12 = """ WITH tab1 AS 
                (SELECT *, uniq(user_id) over (partition by date) as DAU
                              FROM
                                  (SELECT user_id,
                                          toDate(time) AS date,
                                          action,
                                          os,
                                          source
                                   FROM simulator_20230320.feed_actions
                                   UNION ALL SELECT user_id,
                                                    toDate(time) AS date,
                                                    CASE
                                                        WHEN reciever_id>0 THEN 'message'
                                                        ELSE 'n'
                                                    END AS action,
                                                    os,
                                                    source
                                   FROM simulator_20230320.message_actions)
                            WHERE (date BETWEEN yesterday()-6 AND yesterday())
                            ORDER BY date)
                SELECT toString(date) as date, 
                      os, 
                      uniq(user_id) as num_users, 
                      count(user_id) as num_actions,  
                      max(DAU) as DAU,
                      round(num_actions/DAU,2) as per_user
                FROM tab1
                GROUP BY date, os  
                ORDER BY date, os 
                """
# Запрос №13 - распределение количества действий пользователей в разрезе трафика за последние 7 дней (ads/organic) 
query_13 = """ WITH tab1 AS 
                (SELECT *, uniq(user_id) over (partition by date) as DAU
                              FROM
                                  (SELECT user_id,
                                          toDate(time) AS date,
                                          action,
                                          os,
                                          source
                                   FROM simulator_20230320.feed_actions
                                   UNION ALL SELECT user_id,
                                                    toDate(time) AS date,
                                                    CASE
                                                        WHEN reciever_id>0 THEN 'message'
                                                        ELSE 'n'
                                                    END AS action,
                                                    os,
                                                    source
                                   FROM simulator_20230320.message_actions)
                            WHERE (date BETWEEN yesterday()-6 AND yesterday())
                            ORDER BY date)
                SELECT toString(date) as date, 
                      source, 
                      uniq(user_id) as num_users, 
                      count(user_id) as num_actions,  
                      max(DAU) as DAU,
                      round(num_actions/DAU,2) as per_user
                FROM tab1
                GROUP BY date, source  
                ORDER BY date, source 
                """
# Запрос №12 - CTR 
query_14 = """ WITH tab1 AS 
                (SELECT *, uniq(user_id) over (partition by date) as DAU
                              FROM
                                  (SELECT user_id,
                                          toDate(time) AS date,
                                          action,
                                          os,
                                          source
                                   FROM simulator_20230320.feed_actions
                                   UNION ALL SELECT user_id,
                                                    toDate(time) AS date,
                                                    CASE
                                                        WHEN reciever_id>0 THEN 'message'
                                                        ELSE 'n'
                                                    END AS action,
                                                    os,
                                                    source
                                   FROM simulator_20230320.message_actions)
                            WHERE (date BETWEEN yesterday()-6 AND yesterday())
                            ORDER BY date)
                SELECT toString(date) as date, 
                        sum(likes) as likes,
                        sum(views) as views,
                        round(likes/views,3) as CTR
                FROM 
                (SELECT toString(date) as date, 
                      countIf(user_id, action='like') as likes,  
                      countIf(user_id, action='view') as views
                FROM tab1
                where action != 'message'
                GROUP BY date, action)
                GROUP BY date  
                ORDER BY date
                """

# Запрос №2 - распределение по неделям количества пользователей - новых, ушедших и оставшихся
query_2 = """ SELECT this_week AS this_week,
                     status AS status,
                     max(num_users) AS num_users
            FROM
              (SELECT this_week,
                      previous_week,
                      status, -uniq(user_id) as num_users
               FROM
                 (SELECT user_id,
                         groupUniqArray(toMonday(toDate(time))) as weeks_visited,
                         addWeeks(arrayJoin(weeks_visited), +1) this_week,
                         arrayJoin(weeks_visited) dd,
                         if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status,
                         addWeeks(this_week, -1) as previous_week
                  FROM simulator_20230320.feed_actions
                  group by user_id)
               where status = 'gone'
               group by this_week,
                        previous_week,
                        status
               HAVING this_week != addWeeks(toMonday(today()), +1)
               union all SELECT this_week,
                                previous_week,
                                status,
                                toInt64(uniq(user_id)) as num_users
               FROM
                 (SELECT user_id,
                         groupUniqArray(toMonday(toDate(time))) as weeks_visited,
                         arrayJoin(weeks_visited) this_week,
                         if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status,
                         addWeeks(this_week, -1) as previous_week
                  FROM simulator_20230320.feed_actions
                  group by user_id)
               group by this_week,
                        previous_week,
                        status) AS virtual_table
            WHERE this_week between (today()-60) and today()-5
            GROUP BY this_week,
                     status
            ORDER BY this_week ASC
"""
# Создание DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def lesson_7_telegram_bot_2():
    
    # Extract запроса №10
    @task(task_id='Extract_10')
    def extract_10():     
        df_10 = ph.read_clickhouse(query = query_10, connection=connection)
        return df_10
    # Extract запроса №12
    @task(task_id='Extract_12')
    def extract_12():     
        df_12 = ph.read_clickhouse(query = query_12, connection=connection)
        return df_12
    # Extract запроса №13
    @task(task_id='Extract_13')
    def extract_13():     
        df_13 = ph.read_clickhouse(query = query_13, connection=connection)
        return df_13
    # Extract запроса №14
    @task(task_id='Extract_14')
    def extract_14():     
        df_14 = ph.read_clickhouse(query = query_14, connection=connection)
        return df_14
    # Extract запроса №2
    @task(task_id='Extract_2')
    def extract_2():     
        df_2 = ph.read_clickhouse(query = query_2, connection=connection)
        return df_2
    
    # Вводное сообщение
    @task(task_id="Message")
    def mes_1():
        today = datetime.now().strftime('%d-%m-%Y')
        message = f'{today} Main metrics and plots for the previous 7 days'
        return message
    
    # Визуализация запроса №1
    @task(task_id='Visualization_1')
    def visualization_1(df_10, df_12, df_13):             
        fig, ax = plt.subplots(3,1,figsize=(20,15)) 
        # ax1
        plt.subplot(3, 1, 1)
        sns.barplot(x=df_10['date'], y = df_10['num_actions'], hue=df_10['action'])
        plt.ylabel('Number of actions')
        plt.title('In the context of actions (likes/views/messages)'+'\n', 
                      fontsize = 15, color='blue')

        ax1 = plt.twinx()
        x=df_10['date']
        y=df_10['DAU']
        ax1=plt.plot(x, y, marker='o', c='purple')
        plt.grid()

        #ax2
        plt.subplot(3, 1, 2)
        sns.barplot(x=df_12['date'], y = df_12['num_actions'], hue=df_12['os'])
        plt.ylabel('Number of actions')
        plt.title('In the context of OS (iOS/Android)'+'\n', 
                      fontsize = 15, color='blue')
        for container in ax[1].containers :
            ax[1].bar_label (container, color='red', fontsize = 13)

        ax1 = plt.twinx()
        x=df_12['date']
        y=df_12['DAU']
        ax1=plt.plot(x, y, marker='o', c='purple')
        plt.grid()

        #ax3
        plt.subplot(3, 1, 3)
        sns.barplot(x=df_13['date'], y = df_13['num_actions'], hue=df_13['source'])
        plt.ylabel('Number of actions')
        plt.title('In the context of traffic (Ads/Organic)'+'\n', 
                      fontsize = 15, color='blue')

        ax1 = plt.twinx()
        x=df_13['date']
        y=df_13['DAU']
        ax1=plt.plot(x, y, marker='o', c='purple')
        plt.grid()

        plt.suptitle('Number of actions for the last 7 days with the number of daily unique users (purple line)'+'\n', 
                      fontsize = 15, color='green')
        plt.tight_layout()
        
        plot_object_1 = io.BytesIO()
        plt.savefig(plot_object_1)
        plot_object_1.seek(0)
        plot_object_1.name = 'NA7d.png'
        plt.close()
        return plot_object_1
    
    @task(task_id='Visualization_2')
    def visualization_2(df_10, df_12, df_13):
        fig, ax = plt.subplots(3,1,figsize=(20,15)) 
        # ax1
        plt.subplot(3, 1, 1)
        sns.barplot(x=df_10['date'], y=df_10['per_user'], hue=df_10['action'])
        plt.xlabel('Date')
        plt.ylabel('Number of actions per user')
        plt.title('In the context of actions (likes/views/messages)'+'\n', 
                      fontsize = 15, color='blue')
        
        ax1 = plt.twinx()
        x=df_10['date']
        y=df_10['DAU']
        ax1=plt.plot(x, y, marker='o', c='purple')
        plt.grid()

        #ax2
        plt.subplot(3, 1, 2)
        sns.barplot(x=df_12['date'], y=df_12['per_user'], hue=df_12['os'])
        plt.xlabel('Date')
        plt.ylabel('Number of actions per user')
        plt.title('In the context of OS (iOS/Android)'+'\n', 
                      fontsize = 15, color='blue')
        
        ax1 = plt.twinx()
        x=df_12['date']
        y=df_12['DAU']
        ax1=plt.plot(x, y, marker='o', c='purple')
        plt.grid()

        #ax3
        plt.subplot(3, 1, 3)
        sns.barplot(x=df_13['date'], y = df_13['per_user'], hue=df_13['source'])
        plt.xlabel('Date')
        plt.ylabel('Number of actions per user')
        plt.title('In the context of traffic (Ads/Organic)'+'\n', 
                      fontsize = 15, color='blue')

        ax1 = plt.twinx()
        x=df_13['date']
        y=df_13['DAU']
        ax1=plt.plot(x, y, marker='o', c='purple')
        plt.grid()

        plt.suptitle('Number of actions per user for the last 7 days with the number of daily unique users (purple line)'+'\n', 
                      fontsize = 15, color='green')
        plt.tight_layout()
        
        plot_object_2 = io.BytesIO()
        plt.savefig(plot_object_2)
        plot_object_2.seek(0)
        plot_object_2.name = 'NA7dpu.png'
        plt.close()
        return plot_object_2

    @task(task_id='Visualization_3')
    def visualization_3(df_14):
        plt.figure(figsize=(15,5))
        x=df_14['date']
        y=df_14['CTR']
        plt.plot(x, y, marker='o', c='indigo')
        plt.title('CTR for the last 7 days (likes/views)'+'\n', c='green', fontsize=15)
        for a,b in zip(x,y): 
            plt.text(a, b, str(b), color='maroon', fontweight='bold', fontsize = 15)
        plt.grid()
        
        plot_object_3 = io.BytesIO()
        plt.savefig(plot_object_3)
        plot_object_3.seek(0)
        plot_object_3.name = 'NA7dpuj.png'
        plt.close()
        return plot_object_3
    
    # Визуализация запроса №2
    @task(task_id='Visualization_4')
    def visualization_4(df_2):
        df_2['this_week'] = df_2['this_week'].astype('str')
        fig, ax = plt.subplots(figsize=(15,5)) 
        fig.autofmt_xdate(rotation=45) 
        sns.barplot(x=df_2['this_week'], y=df_2['num_users'], hue=df_2['status'],  ax=ax)
        ax.grid()
        ax.set_xlabel('Start of week')
        ax.set_ylabel('Number of people')
        ax.set_title('New/Retained/Gone users for previous weeks'+'\n', c='green', fontsize=15)
        
        plot_object_4 = io.BytesIO()
        plt.savefig(plot_object_4)
        plot_object_4.seek(0)
        plot_object_4.name = 'NA7dpukj.png'
        plt.close()
        return plot_object_4
    
    @task(task_id='Load')
    def send_info(plot_object_1, plot_object_2, plot_object_3, plot_object_4, message, chat_id):
        bot.sendMessage(chat_id=chat_id, text=message)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_1)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_2)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_3)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_4)
    
    df_10 = extract_10()
    df_12 = extract_12()
    df_13 = extract_13()
    df_14 = extract_14()
    message = mes_1()
    plot_object_1 = visualization_1(df_10, df_12, df_13)
    plot_object_2 = visualization_2(df_10, df_12, df_13)
    plot_object_3 = visualization_3(df_14)
    df_2 = extract_2()
    plot_object_4 = visualization_4(df_2)
    send_info(plot_object_1, plot_object_2, plot_object_3, plot_object_4, message, chat_id)

lesson_7_telegram_bot_2 = lesson_7_telegram_bot_2()