# enersinc-test

1) En el archivo sql-test.sql se cargan las consultas del punto 1 de la prueba.

2) El DAG creado para la extraccion de datos de la API de Twitter se llama etl-test.py.

3) Se realizó la configuración necesaria de la base de datos para que almacenara los datos en PostgreSQL, en el archivo airflow.cfg, 
archivo que fue agregado al .gitignore por cuestiones de privacidad de información. Se recomienda cargar su propio archivo airflow.cfg.

4) Se creó una nueva base de datos llamada airflow_db y dentro de ella la configuración de todo apache-airflow, más la tabla twitter_api
la cual almacena la informacion obtenida luego de la transformación de los datos extraídos de la API.

5) La API obtiene los datos de las publicaciones que algunos usuarios objetivos han dado like, de estas publicaciones se obtiene el id,
el texto completo, el número de likes, el número de retuits, el número de citas, y por último se calcula el número de palabras que contiene
cada tweet. Toda esta informacion se almacena en la tabla twitter_data de la base de datos airflow_db.

6) Se agregan al .gitignore archivos que compromenten la seguridad y privacidad de la informacion. Se recomienda que en el .env se aloje en la
carpeta airflow/dags, justo al lado del archivo etl-test.py.

7) Las variables de entorno del .env son:

```
POSTGRES_DBNAME
POSTGRES_USER
POSTGRES_PASSWORD

CONSUMER_KEY
CONSUMER_SECRET
ACCESS_TOKEN
ACCESS_TOKEN_SECRET
BEARER_TOKEN
```
