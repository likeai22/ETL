import pandas as pd
from sklearn.impute import SimpleImputer
from dateutil.parser import parse
import psycopg2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def load_booking_data():
    return pd.read_csv("dags/data/booking.csv")


def load_client_data():
    return pd.read_csv("dags/data/client.csv")


def load_hotel_data():
    return pd.read_csv("dags/data/hotel.csv")


def check_invalid_columns(df):
    invalid_columns = []

    # Проверка числовых колонок
    numeric_columns = df.select_dtypes(include=["number"]).columns
    for col in numeric_columns:
        if df[col].isnull().sum() / len(df) > 0.5:
            invalid_columns.append(col)
        elif df[col].nunique() == 1:
            invalid_columns.append(col)
        # Дополнительная проверка на распределение данных
        elif df[col].std() == 0:
            invalid_columns.append(col)

    # Проверка нечисловых колонок
    non_numeric_columns = df.select_dtypes(exclude=["number"]).columns
    for col in non_numeric_columns:
        if df[col].isnull().sum() / len(df) > 0.5:
            invalid_columns.append(col)
        elif df[col].nunique() == 1:
            invalid_columns.append(col)

    return invalid_columns


def fill_missing_values(df, strategy="mean"):
    numeric_columns = df.select_dtypes(include=["number"]).columns
    non_numeric_columns = df.select_dtypes(exclude=["number"]).columns

    # Импутер для нечисловых колонок
    imputer_non_numeric = SimpleImputer(strategy="most_frequent")

    # Импутер для числовых колонок
    imputer_numeric = SimpleImputer(strategy=strategy)

    # Заполнение пропусков для числовых колонок
    if len(numeric_columns) > 0:
        df[numeric_columns] = imputer_numeric.fit_transform(df[numeric_columns])

    # Заполнение пропусков для нечисловых колонок
    if len(non_numeric_columns) > 0:
        df[non_numeric_columns] = imputer_non_numeric.fit_transform(
            df[non_numeric_columns]
        )

    return df


def transform_data(ti, fill_strategy="mean"):
    booking_df = ti.xcom_pull(task_ids="load_booking")
    client_df = ti.xcom_pull(task_ids="load_client")
    hotel_df = ti.xcom_pull(task_ids="load_hotel")

    merged_df = pd.merge(booking_df, client_df, on="client_id")
    merged_df = pd.merge(merged_df, hotel_df, on="hotel_id")

    # Приведение дат к одному виду
    merged_df["booking_date"] = pd.to_datetime(
        merged_df["booking_date"].apply(parse), format="%Y/%m/%d"
    )

    # Заполнение пропусков
    merged_df = fill_missing_values(merged_df, strategy=fill_strategy)

    # Приведение всех валют к одной
    currency_rates = {"EUR": 1.1, "GBP": 1.3, "USD": 1}
    merged_df["booking_cost"] = merged_df.apply(
        lambda row: row["booking_cost"] * currency_rates.get(row["currency"], 1), axis=1
    )
    merged_df.drop(columns=["currency"], inplace=True)

    # Проверка на невалидные колонки
    invalid_columns = check_invalid_columns(merged_df)
    if invalid_columns:
        merged_df = merged_df.drop(columns=invalid_columns)

    ti.xcom_push("transformed_data", merged_df.to_json())


def load_to_db(ti):
    # Получение данных из XCom
    json_data = ti.xcom_pull(key="transformed_data", task_ids="transform_data")
    transformed_data = pd.read_json(json_data)

    connection_string = "postgres://airflow:airflow@airflow_db:5432/airflow"
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS booking_data (
        client_id FLOAT,
        booking_date BIGINT,
        room_type TEXT,
        hotel_id FLOAT,
        booking_cost FLOAT,
        age FLOAT,
        name_x TEXT,
        type TEXT,
        name_y TEXT,
        address TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # Создание и выполнение SQL-запросов для вставки данных
    for index, row in transformed_data.iterrows():
        insert_query = f"""
        INSERT INTO booking_data (client_id, booking_date, room_type, hotel_id, booking_cost, age, name_x, type, name_y, address)
        VALUES ({row['client_id']}, {row['booking_date']}, '{row['room_type']}', {row['hotel_id']}, {row['booking_cost']}, {row['age']}, '{row['name_x']}', '{row['type']}', '{row['name_y']}', '{row['address']}');
        """
        cur.execute(insert_query)

    conn.commit()
    cur.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="etl_dag_hw8", default_args=default_args, schedule_interval="@daily"
) as dag:

    load_booking_task = PythonOperator(
        task_id="load_booking", python_callable=load_booking_data
    )

    load_client_task = PythonOperator(
        task_id="load_client", python_callable=load_client_data
    )

    load_hotel_task = PythonOperator(
        task_id="load_hotel", python_callable=load_hotel_data
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={"fill_strategy": "mean"},
    )

    load_to_db_task = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_db,
        provide_context=True,
        dag=dag,
    )

    (
        load_booking_task
        >> load_client_task
        >> load_hotel_task
        >> transform_data_task
        >> load_to_db_task
    )
