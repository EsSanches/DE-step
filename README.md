# Примеры работы Spark

Генерация и анализ информации
[task1](https://github.com/EsSanches/DE-step/blob/main/Spark/task1_git.ipynb)

## Генератор синтетических данных
структура наполнения 
> - id: Уникальный идентификатор.
> - name: Случайное имя. Минимальное количество букв в имени - 5.
> - email: Email, сгенерированный на основе имени. Обязательно должна быть @ и ru/com.
> - city: Случайный город. Минимальное количество букв в городе - 7.
> - age: Случайный возраст. Минимальный возраст - 18. Максимальный возраст - 95.
> - salary: Случайная зарплата.
> - registration_date: Дата регистрации, зависящая от возраста. Тут дата регистрации очевидна не должна быть меньше, чем значение в поле age.

[Task_spark_2](https://github.com/EsSanches/DE-step/blob/main/Spark/Task_spark_2_git.ipynb)

# Пример обработки данных (Pandas)
Очистка и преобразование данных на примере [wb](https://github.com/EsSanches/DE-step/blob/main/Pandas/wb2.ipynb)

# Пример загрузки данных и расчета витрины

Описание скрипта
- создание таблиц фактов и справочников в Greenplum
- написал функции для загрузки справочников (с полной перезаписью данных), больших таблиц фактов (с подменой партиций) и расчета витрины

скрипт [Greenplum.sql](https://github.com/EsSanches/DE-step/edit/main/greenplum/final_project_adb.sql)

- создание DAGа для автоматизации загрузки в Airflow [load_date](https://github.com/EsSanches/DE-step/blob/main/greenplum/reload.py) 

# Пример разработки хранилища данных

Модель DateVaut
![Image alt](https://github.com/EsSanches/DE-step/blob/main/DV/DW.png)

скрипт на PostgreSQL [date_v_air](https://github.com/EsSanches/DE-step/blob/main/DV/DateVault_air.sql)




