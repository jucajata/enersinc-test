import logging
import os
from datetime import timedelta

import pandas as pd
import psycopg2
import tweepy
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# se cargan las variables escondidas en el .env (seguridad)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(BASE_DIR, 'dags/.env')
load_dotenv(dotenv_path)


# argumentos de entrada para la variable default_args del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retray_delay': timedelta(minutes=1),
}


# función de Extracción de datos de la API de Twitter
def extract():
    logging.info("performing extraction")

    # se utilizan las credenciales para conectarse a la API de Twitter
    consumer_key = os.getenv('CONSUMER_KEY')
    consumer_secret = os.getenv('CONSUMER_SECRET')
    access_token = os.getenv('ACCESS_TOKEN')
    access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
    bearer_token = os.getenv('BEARER_TOKEN')
    client = tweepy.Client(
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        bearer_token=bearer_token)

    # lista de usuarios objetivos analizar
    usuarios_objetivo = ['elonmusk', 'lexfridman', 'AlejandroSanz', 'shakira', 'rihanna', 'KimKardashian']

    # se crea el df_total que almacenará todos los datos extraídos
    df_total = pd.DataFrame()

    # se itera sobre la lista de usuarios objetivos
    for usuario_objetivo in usuarios_objetivo:

        # se obtiene el id del usuario objetivo
        id_usuario_objetivo = client.get_user(username=usuario_objetivo).data.id

        # obtenemos los tweets a los que el usuario objetivo les dio like
        liked_tweets = client.get_liked_tweets(id=id_usuario_objetivo, max_results=10)

        # se itera sobre los tweets que el usuario objetivo dio like
        for i in range(len(liked_tweets.data)):

            # se obtinene otras variables dentro de cada tweet
            tweet_id = liked_tweets.data[i].id  # id del tweet
            tweet_text = liked_tweets.data[i].text  # texto del tweet
            liking_users = client.get_liking_users(id=tweet_id, max_results=None).meta['result_count']  # numero de usuarios que le dieron like a esa publicacón
            retweeters = client.get_retweeters(id=tweet_id, max_results=None).meta['result_count']  # número de retwits de esa publicación
            quote_tweets = client.get_quote_tweets(id=tweet_id, max_results=None).meta['result_count']  # número de citas de esa publicación

            # se almacena la información en un df temporal
            df_temp = pd.DataFrame({
                'username': usuario_objetivo,
                'id_tweet': tweet_id,
                'tweet': tweet_text,
                'tweet_likes': liking_users,
                'tweet_rts': retweeters,
                'quote_tweets': quote_tweets,
            }, index=[i])

            # se concatena el df total con el df temporal
            df_total = pd.concat([df_total, df_temp])
    
    # se guardan los datos extraidos en un csv inicial
    df_total.to_csv('extracted_data.csv', index=False)


# función de Transformación de datos de la API de Twitter
def transform():
    logging.info("performing transformation")

    # se obtiene el csv inicial para construir el df de transformación
    df = pd.read_csv('extracted_data.csv')

    # se obtiene la cantidad de palabras del tweet
    df['number_words'] = [len(x.split()) for x in df['tweet']]

    # se guardan los datos transformados en el csv final
    df.to_csv('transformed_data.csv', index=False)


def load():
    logging.info("performing loading")

    '''
    Antes de correr el DAG es necesario crear esta tabla en PostgreSQL:

    CREATE TABLE TWITTER_DATA (
        username VARCHAR(100) NOT NULL,
        id_tweet VARCHAR(50) NOT NULL,
        tweet VARCHAR(4000) NOT NULL,
        tweet_likes INTEGER,
        tweet_rts INTEGER,
        quote_tweets INTEGER,
        number_words INTEGER
    );
    '''

    # Conexión a la BD de PostgreSQL
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DBNAME'), 
        user=os.getenv('POSTGRES_USER'), 
        password=os.getenv('POSTGRES_PASSWORD'))

    # Definición de un cursos para realizar operaciones en la base de datos
    cur = conn.cursor()

    # Se lee el csv final que se almacenará en la base de datos
    df = pd.read_csv('transformed_data.csv')

    # se itera por cada fila del df para almacenarlo en una fila de la tabla twitter_data
    for row in df.index:
        username = str(df.iloc[row].username)
        id_tweet = str(df.iloc[row].id_tweet)
        tweet = str(df.iloc[row].tweet)
        tweet_likes = int(df.iloc[row].tweet_likes)
        tweet_rts = int(df.iloc[row].tweet_rts)
        quote_tweets = int(df.iloc[row].quote_tweets)
        number_words = int(df.iloc[row].number_words)

        sql = "INSERT INTO twitter_data (username, id_tweet, tweet, tweet_likes, tweet_rts, quote_tweets, number_words) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (username, id_tweet, tweet, tweet_likes, tweet_rts, quote_tweets, number_words)
        cur.execute(sql, val)
        conn.commit()

    conn.close()


# Creación del DAG con los argumentos default y las funciones definidas ETL
with DAG(
    'etl-twitter',
    default_args=default_args,
    description='DAG para ETL de API de Twitter',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['DAG para prueba como Ingeniero de Datos en Enersinc']
) as dag:
    extract_task = PythonOperator(task_id='extract', python_callable=extract)
    transform_task = PythonOperator(task_id='transform', python_callable=transform)
    load_task = PythonOperator(task_id='load', python_callable=load)

    extract_task >> transform_task >> load_task