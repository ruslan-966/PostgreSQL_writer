import pandas as pd
import os
from model_param import *
from SQL_config import *
import psycopg2


def write_in_base():
    curr_dir = os.getcwd()
    file_name = f'{curr_dir}\\{TOTAL_DATA_FILE_NAME}'
    df = pd.read_csv(file_name, encoding='cp1251')
    # Сброс ограничений на число столбцов
    pd.set_option('display.max_columns', None)
    print(df.head(5))
    # удаление конфликтующих символов
    df["product_description"] = df["product_description"].str.replace("'", "")
    df["product_store"] = df["product_store"].str.replace("'", "")
    # Записывем данные в базу
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            port=port
        )
        table_name = 'aliexpress_smartfons'
        connection.autocommit = True
        # Создаем таблицу
        with connection.cursor() as cursor:
            cursor.execute(
                f"""CREATE TABLE IF NOT EXISTS {table_name}(
                id serial PRIMARY KEY,
                product_description varchar(220) NOT NULL,
                product_price decimal NOT NULL,
                product_store varchar(80) NOT NULL,
                product_delivery boolean NOT NULL)"""
            )
            print(f"Таблица {table_name} создана")
        "Вносим данные в таблицу"
        with connection.cursor() as cursor:
            for i in range(df.shape[0]):
                cursor.execute(
                    f"""INSERT INTO {table_name} (product_description, product_price, product_store, product_delivery)
                    VALUES ('{df.iloc[i]['product_description']}', {df.iloc[i]['product_price']}, 
                            '{df.iloc[i]['product_store']}', {df.iloc[i]['product_delivery']})"""

                )
                # print(f"Текущее значение номера строки: {i}.")
            print(f"Данные успешно внесены")
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if connection:
            connection.close()
            print("Соединение успешно разорвано")
    return


def main():
    write_in_base()


if __name__ == "__main__":
    main()
