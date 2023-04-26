"""Задание DAG в Airflow - система должна с периодичность каждые 15 минут проверять ключевые метрики приложения,
такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений. 
При обнаружении аномалий в telegram чат должен приходить alert (сообщение с текстом и графиком)
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

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'm-solomatina-17',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta (minutes = 5),
    'start_date': datetime (2022, 4, 20),
}

# Интервал запуска DAG'а каждый день в 11:00
schedule_interval = '*/15 * * * *'

# Соединение с clickhouse
connection = {
                'host': '',
                'password': '',
                'user': '',
                'database': ''
            }

# Запрос №1 - количество пользователей, просмотров и лайков в ЛЕНТЕ с агрегацией по 15 минут
query_1 = """ SELECT 
                    toStartOfFifteenMinutes(time) as ts,
                    toDate(time) as date,
                    formatDateTime(ts,'%R') as hm,
                    uniqExact(user_id) as users_feed,
                    countIf(user_id, action = 'view') as views,
                    countIf(user_id, action = 'like') as likes,
                    round(likes/views,4) as ctr
              FROM simulator_20230320.feed_actions
              WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
              GROUP BY ts, date, hm     
              ORDER BY date, hm
        """

# Запрос №2 - количество пользователей сервиса сообшений и количество отправленных сообщений
query_2 = """ SELECT 
                    toStartOfFifteenMinutes(time) as ts,
                    toDate(time) as date,
                    formatDateTime(ts,'%R') as hm,
                    uniqExact(user_id) as num_users,
                    count(reciever_id) as num_messages
              FROM simulator_20230320.message_actions
              WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
              GROUP BY ts, date, hm     
              ORDER BY date, hm
        """
# Исследуемые метрики ленты новостей и сообщений 
metrics_list_feed = ['users_feed', 'views', 'likes', 'ctr']
metrics_list_feed_2 = ['"Количество пользователей сервиса ленты новостей"','"Количество просмотров постов"','"Количество лайков постов"','"CTR (лайки/просмотры)"']
metrics_list_message = ['num_users', 'num_messages']
metrics_list_message_2 = ['"Количество пользователей сервиса сообщений"','"Количество сообщений"']

# функция предлагает алгоритм поиска аномалий в данных (межквартильный размах)
def check_anomaly(df, metric, a=5, n=4):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

# Создание DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def lesson_8_alert_system():
    
    # извлечение данных из запросов
    @task(task_id='Extract_feed')
    def extract_feed():     
        df_feed = ph.read_clickhouse(query=query_1, connection=connection)
        return df_feed
    
    @task(task_id='Extract_message')
    def extract_message():     
        df_message = ph.read_clickhouse(query=query_2, connection=connection)
        return df_message
    # cистема алертов
    @task(task_id = 'Run_alerts')  
    def run_alerts(df, metrics_list, metrics_list_2, chat=None):
        # данные для бота
        chat_id = chat or ''
        bot = telegram.Bot(token = my_token)
            
        for num, metric in enumerate(metrics_list):
    
            df_metric = df[['ts', 'date', 'hm', metric]].copy()
            is_alert, df_metric = check_anomaly(df_metric, metric)

            if is_alert == 1:
                message = f'''Метрика {metrics_list_2[num]} в срезе {df_metric['ts'].iloc[-1]}:\nТекущее значение - {(df_metric[metric].iloc[-1]):,.2f}\nОтклонение от предыдущего значения {((1 - (df_metric[metric].iloc[-1] / df_metric[metric].iloc[-2]))*100):,.2f}%\nСсылка на оперативный дашборд: https://superset.lab.karpov.courses/superset/dashboard/3356/ '''

                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout()

                ax = sns.lineplot(x = df_metric['ts'], y = df_metric[metric], label = 'metric')
                ax = sns.lineplot(x = df_metric['ts'], y = df_metric['up'], label = 'up')
                ax = sns.lineplot(x = df_metric['ts'], y = df_metric['low'], label = 'low')

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 1 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel = 'time')
                ax.set(ylabel = metric)

                ax.set_title(metrics_list_2[num])
                ax.set(ylim = (0, None))

                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

                bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption = message )

    
    df_feed = extract_feed()
    df_message = extract_message()
    run_alerts(df_feed, metrics_list_feed, metrics_list_feed_2)
    run_alerts(df_message, metrics_list_message, metrics_list_message_2)

lesson_8_alert_system = lesson_8_alert_system()