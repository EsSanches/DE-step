purchases = [
    {"item": "apple", "category": "fruit", "price": 1.2, "quantity": 10},
    {"item": "banana", "category": "fruit", "price": 0.5, "quantity": 5},
    {"item": "milk", "category": "dairy", "price": 1.5, "quantity": 2},
    {"item": "bread", "category": "bakery", "price": 2.0, "quantity": 3},
]

# выручка
def total_revenue(purchases):
    c = sum(i['price'] * i['quantity'] for i in purchases)
    return c

revenue = total_revenue(purchases)
print(f"Общая выручка: {revenue}")

# категории товаров
def items_by_category(purchases):
    category_dict = {}
    
    for purch in purchases:
        category = purch["category"]
        item = purch["item"]
        
        if category not in category_dict:
            category_dict[category] = []
        
        if item not in category_dict[category]:
            category_dict[category].append(item)
    
    return category_dict

cat_purch = items_by_category(purchases) 
print(f"Товары по категориям: {cat_purch}")


# покупки дороже заданного прайса
def expensive_purchases(purchases, min_price):
    cat_min_price = []
    for i in purchases:
        if i["price"] >= min_price:
            cat_min_price.append(i)
    return cat_min_price

min_price = 1
r_price = expensive_purchases(purchases, min_price)
print(f"Покупки дороже {min_price}: {r_price}")


# функция возвращает среднюю цену по категориям
def average_price_by_category(purchases):
    category_dict = {}
    for purch in purchases:
        category = purch["category"]
        price = purch["price"]

        if category not in category_dict:
            category_dict[category] = []
        
        if price not in category_dict[category]:
            category_dict[category].append(price)
    return {cat: sum(val)/len(val) for cat, val in category_dict.items()}

avg_cat = average_price_by_category(purchases)
print(f"Средняя цена по категориям: {avg_cat}")

# Функция возвращает категорию с наибольшим кол-м проданных товаров
def most_frequent_category(purchases):

    max_category_qnt = {}
    for purch in purchases:
        category = purch["category"]
        quantity = purch["quantity"]
        
        if category in max_category_qnt:
            max_category_qnt[category] += quantity
        else:
            max_category_qnt[category] = quantity

    return max(max_category_qnt) 

res_max = most_frequent_category(purchases)
print(f"Категория с наибольшим количеством проданных товаров: {res_max}")

