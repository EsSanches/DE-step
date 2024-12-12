# SQL задание
Имеем:

1. Таблица заявок application с полями id, product_id, client_id, app_date, app_value

2. Таблица клиентов clients с полями id, client_surname, client_name, client_ second_name, birth_date

3. Таблица products с полями id, product_name, product_group_name

Необходимо:

1. Составить таблицу с суммой заявок по месяцам и продуктам

2. Составить таблицу с суммой и количеством заявок по месяцам, продуктам, группам продуктов, где месяц заявки равен августу 2021 года.

3. Составить таблицу с суммой и количеством заявок по месяцам, продуктам, группам продуктов и группам возвратов клиентов с шагом 10 лет

4. Составить таблицу с суммой и количеством заявок накопительно по месяцам

[Ответ в виде sql кода](https://github.com/EsSanches/DE-step/blob/main/test_ab_bank/Script-2.sql)


# Проверка уровня знаний excel 

Вам предлагается выполнить следующие задания:
1. "По приведенным данным на листе "Заявки" построить сводную таблицу с разбивкой по месяцам следующих показателей:
- количество заявок в месяце;
- сумма выданных кредитов в месяце.
Поставить в качестве фильтра срок кредита."
2. "К приведенным данным на листе "Выбранные заявки" подтянуть данные из листа "Заявки":
- сумма кредита;
- срок кредита;
- дата заявки."
3. Разбить данные с листа "Заявки" по сроку кредита в соответствие с приведенными на листе "Разбивка по сроку кредита" данными.
  
1) [решение](https://github.com/EsSanches/DE-step/blob/main/test_ab_bank/Test_ab_bank_1.ipynb)
   ([таблица](https://github.com/EsSanches/DE-step/blob/main/test_ab_bank/%D0%BA%D0%BE%D0%BB-%D0%B2%D0%BE%20%D0%B8%20%D1%81%D1%83%D0%BC%D0%BC%D0%B0%20%D0%BA%D1%80%D0%B5%D0%B4%D0%B8%D1%82%D0%BE%D0%B2_1.xlsx))

3) [решение](https://github.com/EsSanches/DE-step/blob/main/test_ab_bank/test_ab_bank_2.ipynb)
   ([таблица](https://github.com/EsSanches/DE-step/blob/main/test_ab_bank/test_ab_bank_2.ipynb))
   
5) [решение](https://github.com/EsSanches/DE-step/blob/main/test_ab_bank/test_ab_bank_3.ipynb)
   ([таблица](https://github.com/EsSanches/DE-step/blob/main/test_ab_bank/%D0%A0%D0%B0%D0%B7%D0%B1%D0%B8%D0%B2%D0%BA%D0%B0%20%D0%BF%D0%BE%20%D1%81%D1%80%D0%BE%D0%BA%D1%83_3.xlsx))
   
