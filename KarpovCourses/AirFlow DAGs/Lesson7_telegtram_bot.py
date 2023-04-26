"""Задание DAG в Airflow- собрать единый отчет по работе всего приложения. В отчете должна быть информация и по ленте новостей, и по сервису отправки сообщений. 
Отчет должен быть не просто набором графиков или текста, а помогать отвечать бизнесу на вопросы о работе всего приложения совокупно. 
Отчет должен приходить ежедневно в 11:00 в telegram чат
"""

# Импорт библиотек
import telegram
import io

import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, date

from airflow.decorators import dag, task

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
    'start_date': datetime (2022, 4, 16),
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

# Запрос DAU, likes, views, CTR
query_1 = """ 
                SELECT toDate(time) as date,
                       count(DISTINCT user_id) AS DAU,
                       countIf(action = 'like') as likes,
                       countIf(action = 'view') as views,
                       round((countIf(user_id, action = 'like')/countIf(user_id, action = 'view'))*100, 3) AS CTR,
                       countIf(distinct user_id, source = 'ads') as ads_traffic,
                       countIf(distinct user_id, source = 'organic') as organic_traffic,
                       countIf(distinct user_id, os = 'iOS') as ios_users,
                       countIf(distinct user_id, os = 'Android') as android_users
                FROM simulator_20230320.feed_actions
                WHERE toDate(time) BETWEEN (yesterday()-6) AND yesterday()
                GROUP BY date
            """

# Создание DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def lesson_7_telegram_bot():
    
    # Extract запроса (feed_actions)
    @task()
    def extract_fa():     
        df_fa = ph.read_clickhouse(query = query_1, connection=connection)
        return df_fa
    
    @task(task_id="Message")
    def yesterday_table_text(df_fa):
        df_2d = df_fa[(df_fa['date']==(date.today() - timedelta(days = 1)).strftime('%Y-%m-%d'))|
                      (df_fa['date']==(date.today() - timedelta(days = 2)).strftime('%Y-%m-%d'))] 
        list_changes = []
        for x in df_2d.columns[1:]:
            list_changes.append(round(((df_2d[x].iloc[-1]/df_2d[x].iloc[-2])*100-100),2))
            
        message = f""" {df_fa['date'].iloc[-1].strftime('%d-%m-%Y') + ' (short report on yesterday metrics)'}
        =======================================
        ***************************************
        Metric                     Value    (Change(%))
        ------------------------------------------------------
        DAU                   {df_fa.iloc[-1,1]}   ({list_changes[0]})
        Likes                 {df_fa.iloc[-1,2]}  ({list_changes[1]})
        Views                 {df_fa.iloc[-1,3]}  ({list_changes[2]})
        CTR                   {df_fa.iloc[-1,4]}%  ({list_changes[3]})
        Ads traffic           {df_fa.iloc[-1,5]}    ({list_changes[4]})
        Organic traffic       {df_fa.iloc[-1,6]}    ({list_changes[5]})
        iOS users             {df_fa.iloc[-1,7]}    ({list_changes[6]})
        Android users         {df_fa.iloc[-1,8]}   ({list_changes[7]})
        ****************************************
        ========================================
        """
        return message
    
    @task(task_id="Visualisation")
    def last_week_metrics(df_fa):
        fig, ax = plt.subplots(3,2, figsize=(25,16)) 

        x = df_fa ['date']
        fig.suptitle('Metrics for the last 7 days', c='maroon').set_size(fontsize=30) 

        params = {'lines.linewidth': 2,
                  'lines.markersize': 10,
                  'lines.marker': 'o',
                  'axes.titlesize': 20
                 }
        plt.rcParams.update(params)

        ax[0,0].plot(x, df_fa['DAU'], c='seagreen')
        ax[0,0].set_title('DAU', c = 'goldenrod')
        ax[0,0].set_xlabel('Day')
        ax[0,0].set_ylabel('Number of users')
        ax[0,0].grid()

        ax[0,1].plot(x, df_fa['CTR'], c='slateblue')
        ax[0,1].set_title('CTR', c = 'goldenrod')
        ax[0,1].set_xlabel('Day')
        ax[0,1].set_ylabel('likes/views (%)')
        ax[0,1].grid()

        ax[1,0].plot(x, df_fa['views'], c= 'teal')
        ax[1,0].set_title('Views', c = 'goldenrod')
        ax[1,0].set_xlabel('Day')
        ax[1,0].set_ylabel('Number of views')
        ax[1,0].grid()

        ax[1,1].plot(x, df_fa['likes'], c='violet')
        ax[1,1].set_title('Likes', c = 'goldenrod')
        ax[1,1].set_xlabel('Day')
        ax[1,1].set_ylabel('Number of likes')
        ax[1,1].grid()

        ax[2,0].plot(x, df_fa['ads_traffic'], c= 'olivedrab')
        ax[2,0].plot(x, df_fa['organic_traffic'], c= 'lightcoral')
        ax[2,0].legend(['ads_traffic', 'organic_traffic' ])
        ax[2,0].set_title('Traffic', c = 'goldenrod')
        ax[2,0].set_xlabel('Day')
        ax[2,0].set_ylabel('Number of users')
        ax[2,0].grid()

        ax[2,1].plot(x, df_fa['ios_users'], c= 'slategray')
        ax[2,1].plot(x, df_fa['android_users'], c= 'indigo')
        ax[2,1].legend(['ios_users', 'android_users'])
        ax[2,1].set_title('OS', c = 'goldenrod')
        ax[2,1].set_xlabel('Day')
        ax[2,1].set_ylabel('Number of users')
        ax[2,1].grid()
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        return plot_object
    
    @task(task_id="Load")
    def send_info(message, plot_object, chat_id:int):
        bot.sendMessage(chat_id=chat_id, text=message)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    df_fa = extract_fa()
    message = yesterday_table_text(df_fa)
    plot_object = last_week_metrics(df_fa)
    send_info(message, plot_object, chat_id)

lesson_7_telegram_bot = lesson_7_telegram_bot()