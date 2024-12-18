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


# Задание аналитику БД на знание sql-запросов

1. Вывести количество уникальных посетителей, которые заходили на сайт из разных городов
2. Найти город, с максимальным числом уникальных посетителей. Вывести в разрезе каждого
месяца
3. Найти среднее количество посещений сайта , потребовавшееся посетителям, чтобы принять
решение об открытии счета ( результат = одно число, только по посетителям, открывшим счет)
4. Найти среднее число дней , потребовавшееся посетителям, чтобы принять решение об
открытии счета ( Результат = одно число, только по посетителям, открывшим счет, считаем с
первого посещения сайта)
5. Вывести идентификаторы посетителей, которые открыли счет в день первого посещения сайта
6. Найти общую стоимость привлечения и количество посетителей, которые в течение года так и
не открыли счет
7. Найти общую стоимость затрат на посещения после открытия счета (в пустую потраченных
денег, с учетом того, что счет можно открыть только один раз)
8. Найти общую стоимость привлечения и количество посетителей, которые в течение года
открыли счет
9. Вывести уникальные даты посещения сайта посетителем, стоимость привлечения которого на
сайт (Событие "Посещение сайта", за весь год) оказалась самой высокой. Независимо от того,
открыл или не открыл счет
10.Найти ежемесячное изменение количества уникальных посетителей сайта (в процентах) .
Результат должен быть примерно таким январь (или 1) - NULL, февраль (или 2) - "-5" (по
отношению к январю), март (или 3) - "9%" (по отношению к февралю)

[Ответ в виде SQL](https://github.com/EsSanches/DE-step/blob/main/test_ab_bank/Script-test.sql)
   
