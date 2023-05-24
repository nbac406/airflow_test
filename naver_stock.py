from datetime import date, datetime 
import requests
import pandas as pd
import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator




dag = DAG(
    dag_id="get_stock",
    description="naver stock data",
    start_date=datetime(2023, 5, 24, 0, 0),  # 시작 날짜 및 시간 설정
    schedule_interval='30 16 * * *',  # 매일 오전 8시에 실행 (cron 표현식)
)


def _get_symbol():
    pathlib.Path("/tmp/stock").mkdir(parents=True, exist_ok=True)
    krx_url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    krx_payload = {"bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
                    "locale": "ko_KR",
                    "mktId": "ALL",
                    "share": "1",
                    "csvxls_isNo": "false",}

    r= requests.post(krx_url, data=krx_payload)
    pd.DataFrame(r.json()['OutBlock_1']).to_csv("/home/airflow/data/master.csv", index=False)


get_symbol = PythonOperator(
    task_id="get_symbol", python_callable=_get_symbol, dag=dag
)

def _get_data():
   
    today = str(date.today()).replace("-","")
    master = pd.read_csv("/home/data/master.csv")
    total = []
    for x in master['ISU_SRT_CD'][:10]:
        symbol = x
        url = f"https://api.finance.naver.com/siseJson.naver?symbol={symbol}&requestType=1&startTime={today}&endTime={today}&timeframe=day"
        df =pd.DataFrame(data=eval(requests.post(url).text.strip())[1:], columns=eval(requests.post(url).text.strip())[0])
        df['symbol'] = symbol
        total.append(df)
        
    pd.concat(total, ignore_index=True).to_csv(f"/home/airflow/data/{today}.csv", index=False)


get_data = PythonOperator(
    task_id="get_data", python_callable=_get_data, dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/stock/ | wc -l) images."',
    dag=dag,
)

get_symbol >> get_data >> notify
