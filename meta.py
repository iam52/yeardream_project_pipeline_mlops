from pathlib import Path
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import csv
import requests
from bs4 import BeautifulSoup
import datetime as dt
from datetime import datetime
from datetime import timedelta
import pandas as pd

dag = DAG(
    dag_id="meta",
    schedule_interval='@once',
    start_date=dt.datetime(2023, 10, 26, 6, 32),
)


def _metacritic_crawler1(**context):
    user_agent = {
    'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    output_path = context["templates_dict"]["output_path"]
    Path(output_path).parent.mkdir(exist_ok=True)

    first_names = []
    first_release_dates = []
    first_meta_scores = []
    first_recommendations = []

    first_total_pages = 200
    for index, page in enumerate(range(0, first_total_pages), 1):
        url = f'https://www.metacritic.com/browse/game/all/all/all-time/popular/?releaseYearMin=1910&releaseYearMax=2023&page={page}'
        response = requests.get(url, headers=user_agent)
        soup = BeautifulSoup(response.text, 'lxml')
        # name and number
        h3_tags = soup.find_all('h3', class_='c-finderProductCard_titleHeading')
        for tag in h3_tags:
            span_tags = tag.find_all('span')
            if len(span_tags) > 1:
                name = span_tags[1].get_text().replace(',', '')
                first_names.append(name)

        # release_date
        release_divs = soup.find_all('div', class_='c-finderProductCard_meta')
        for div in release_divs:
            span_tags = div.find_all('span', class_='u-text-uppercase')
            release_date = ''
            for span in span_tags:
                if 'Metascore' not in span.get_text(strip=True):
                    release_date = span.get_text(strip=True)
                    first_release_dates.append(release_date)

        # meta_score and recommendation
        metascore_elements = soup.find_all('div', class_='c-siteReviewScore')
        for element in metascore_elements:
            span = element.find('span')
            if span is not None:
                first_meta_scores.append(span.text.strip())
            else:
                first_meta_scores.append('')

            # Recommendation
            if 'c-siteReviewScore_green' in element['class']:
                first_recommendations.append('A score')
            elif 'c-siteReviewScore_yellow' in element['class']:
                first_recommendations.append('B score')
            elif 'c-siteReviewScore_red' in element['class']:
                first_recommendations.append('C score')

    # 데이터 프레임 생성
    df = pd.DataFrame(list(zip(first_names, first_release_dates, first_meta_scores, first_recommendations)), 
                      columns=['name', 'release_date', 'meta_score', 'recommendation'])

    # 특수문자 및 기호 제거
    df['name'] = df['name'].str.replace('[^\w\s]','')
    df['recommendation'] = df['recommendation'].str.replace('[^\w\s]','')

    # meta_score 숫자형으로 변환 (변환이 불가능한 값은 NaN)
    df['meta_score'] = pd.to_numeric(df['meta_score'], errors='coerce')

    # release_date 날짜형식으로 변환
    df['release_date'] = pd.to_datetime(df['release_date'], format='%b %d, %Y')

    # csv 파일로 저장
    df.to_csv(output_path, index=False)

def _metacritic_crawler2(**context):
    user_agent = {
    'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    output_path = context["templates_dict"]["output_path"]
    Path(output_path).parent.mkdir(exist_ok=True)

    second_names = []
    second_release_dates = []
    second_meta_scores = []
    second_recommendations = []


    second_total_pages = 343

    for index, page in enumerate(range(201, 544), 201):
#    for index, page in enumerate(range(11, 30), 11):

        url = f'https://www.metacritic.com/browse/game/all/all/all-time/popular/?releaseYearMin=1910&releaseYearMax=2023&page={page}'
        response = requests.get(url, headers=user_agent)
        soup = BeautifulSoup(response.text, 'lxml')

        # name and number
        h3_tags = soup.find_all('h3', class_='c-finderProductCard_titleHeading')
        for tag in h3_tags:
            span_tags = tag.find_all('span')
            if len(span_tags) > 1:
                name = span_tags[1].get_text().replace(',', '')
                second_names.append(name)

        # release_date
        release_divs = soup.find_all('div', class_='c-finderProductCard_meta')
        for div in release_divs:
            span_tags = div.find_all('span', class_='u-text-uppercase')
            release_date = ''
            for span in span_tags:
                if 'Metascore' not in span.get_text(strip=True):
                    release_date = span.get_text(strip=True)
                    second_release_dates.append(release_date)

        # meta_score and recommendation
        metascore_elements = soup.find_all('div', class_='c-siteReviewScore')
        for element in metascore_elements:
            span = element.find('span')
            if span is not None:
                second_meta_scores.append(span.text.strip())
            else:
                second_meta_scores.append('')

            # Recommendation
            if 'c-siteReviewScore_green' in element['class']:
                second_recommendations.append('A score')
            elif 'c-siteReviewScore_yellow' in element['class']:
                second_recommendations.append('B score')
            elif 'c-siteReviewScore_red' in element['class']:
                second_recommendations.append('C score')

    # 데이터 프레임 생성
    df = pd.DataFrame(list(zip(second_names, second_release_dates, second_meta_scores, second_recommendations)), 
                      columns=['name', 'release_date', 'meta_score', 'recommendation'])

    # 특수문자 및 기호 제거
    df['name'] = df['name'].str.replace('[^\w\s]','')
    df['recommendation'] = df['recommendation'].str.replace('[^\w\s]','')

    # meta_score 숫자형으로 변환 (변환이 불가능한 값은 NaN)
    df['meta_score'] = pd.to_numeric(df['meta_score'], errors='coerce')

    # release_date 날짜형식으로 변환
    df['release_date'] = pd.to_datetime(df['release_date'], format='%b %d, %Y')

    # csv 파일로 저장
    df.to_csv(output_path, index=False)            


metacritic_crawler1 = PythonOperator(
        task_id="metacritic_crawler1",
        python_callable=_metacritic_crawler1,
        templates_dict={
            "output_path": "/home/ubuntu/airflow/output/ois/batch_1.csv",
        },
        dag=dag,
)


metacritic_crawler2 = PythonOperator(
        task_id="metacritic_crawler2",
        python_callable=_metacritic_crawler2,
        templates_dict={
            "output_path": "/home/ubuntu/airflow/output/ois/batch_2.csv",
        },
        dag=dag,
)

create_meta_table = SQLExecuteQueryOperator(
    task_id='create_meta_table',
    sql="""
    CREATE TABLE airflow_meta (
        Name VARCHAR(100),
        Release_Date VARCHAR(100),
        Meta_Score VARCHAR(100),
        Recommendation VARCHAR(100)
    );
    """,
    conn_id='root',
    database='airflow',
    dag=dag,
)

send_files1 = BashOperator(
        task_id="send_files1",
        bash_command=(
            "sudo mv /home/ubuntu/airflow/output/ois/batch_1.csv /var/lib/mysql-files/"
        ),
        dag=dag,
)

send_files2 = BashOperator(
        task_id="send_files2",
        bash_command=(
            "sudo mv /home/ubuntu/airflow/output/ois/batch_2.csv /var/lib/mysql-files/"
        ),
        dag=dag,
)

load_data1 = SQLExecuteQueryOperator(
    task_id='load_data1',
    sql="""
    LOAD DATA INFILE '/var/lib/mysql-files/batch_1.csv'
    INTO TABLE airflow_meta
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS
    ;
    """,
    conn_id='root',
    database='airflow',
    dag=dag,
)

load_data2 = SQLExecuteQueryOperator(
    task_id='load_data2',
    sql="""
    LOAD DATA INFILE '/var/lib/mysql-files/batch_2.csv'
    INTO TABLE airflow_meta
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS
    ;
    """,
    conn_id='root',
    database='airflow',
    dag=dag,
)

metacritic_crawler1 >> metacritic_crawler2 >> create_meta_table >> send_files1 >> send_files2 >> load_data1 >> load_data2

