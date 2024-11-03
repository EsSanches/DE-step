
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




