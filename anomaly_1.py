# импортируем библиотеки
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import datetime, timedelta
import io
import sys
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# задаем параметры для DAG

default_args = {
    'owner': 'a.davydenko',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 13)
}

# Интервал запуска DAG'а каждые 15 минут
schedule_interval = '*/15 * * * *'

# подключение к CH
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'some_database',
                      'user':'student', 
                      'password':'some_database'
                     }

# задаем переменные бота и чата (для проверки bot_token и some_id_chat заменить на свои)
bot = telegram.Bot(token='bot_token')
chat_id = some_id_chat

# функция поиска аномалий
def check_anomaly(df, metric, a=3, n=5):
    # функция алгоритма поиска данных - межквартильный размах
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a * df['iqr']
    df['down'] = df['q25'] - a * df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['down'] = df['down'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['down'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

# функция запуска отправки сообщений и графиков
def run_alerts(data, chat_id):
    metrics_list = ['users_feed', 'views', 'likes', 'users_message', 'messages']
    for metric in metrics_list:
        print(metric)
        df = data[['ts', 'date', 'hs', metric]].copy()
        
        # далее для df мы будем применять алгоритм из check_anomaly
        is_alert, df = check_anomaly(df, metric)
        
        if is_alert == 1:
            msg = '''Метрика {metric}:\n текущее значение {current_val:.2f}\n отклонение от предыдущего значения {last_val_diff:.2%}'''.format(metric=metric, current_val=df[metric].iloc[-1], last_val_diff=abs(df[metric].iloc[-1]/df[metric].iloc[-2]))
            
            sns.set(rc={'figure.figsize': (16, 18)})
            plt.tight_layout()
            
            ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
            ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
            ax = sns.lineplot(x=df['ts'], y=df['down'], label='down')
            
            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
                    
            ax.set(xlabel='time')
            ax.set(ylabel=metric)
            
            ax.set_title(metric)
            ax.set(ylim=(0, None))
                        
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'Stats.png'
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    return 

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)  
def dag_anomaly_allert_a_davydenko():
    
    # Таск для выгрузки данных из feed_actions за неделю
    @task()
    def extract_feed_actions():
        query_1 = '''
        select toStartOfFifteenMinutes(time) as ts,
            toDate(time) as date,
            formatDateTime(ts, '%R') as hs,
            uniqExact(user_id) as users_feed,
            countIf(user_id, action='view') as views,
            countIf(user_id, action='like') as likes
        from {db}.feed_actions
        where time >= today() - 1 and time < toStartOfFifteenMinutes(now())
        group by ts, date, hs
        order by ts
        '''
        
        feed_actions = ph.read_clickhouse(query_1, connection=connection)
        return feed_actions
    
    # Таск для выгрузки данных из message_actions
    @task
    def extract_message():
        query_2 = '''
        SELECT toStartOfFifteenMinutes(time) as ts,
            toDate(time) as date,
            formatDateTime(ts, '%R') as hs,
            uniqExact(user_id) as users_message,
            count(user_id) as messages
        FROM {db}.message_actions
        where time >= today() - 1 and time < toStartOfFifteenMinutes(now())
        group by ts, date, hs
        order by ts
        '''

        message_actions = ph.read_clickhouse(query_2, connection=connection)
        return message_actions
    
     # объединение 2-х таблиц
    @task
    def transfrom_join(feed_actions, message_actions):
        data = feed_actions.merge(message_actions, how='outer', on=['ts', 'date', 'hs']).fillna(0)
        return data
    
    @task
    def run_anomaly_alerts(data, chat_id):
        run_alerts(data, chat_id)
        
        
    feed_actions = extract_feed_actions()
    message_actions = extract_message()
    data = transfrom_join(feed_actions, message_actions)
    run_anomaly_alerts(data, chat_id)

dag_anomaly_allert_a_davydenko = dag_anomaly_allert_a_davydenko()
