#!/usr/bin/env python3
"""
Создание тестовых данных для проверки улучшенной ML логики
"""

import pandas as pd
import random
import numpy as np

def create_realistic_test_data():
    """Создает реалистичные тестовые данные"""
    random.seed(42)
    np.random.seed(42)
    
    # Создаем клиентов
    clients = []
    names = ['Алия', 'Рамазан', 'Мария', 'Алексей', 'Айгуль', 'Данияр', 'Анна', 'Максим', 
             'Жанар', 'Арман', 'Елена', 'Дмитрий', 'Айгерим', 'Сергей', 'Ольга', 'Андрей',
             'Асель', 'Владимир', 'Татьяна', 'Игорь', 'Айнур', 'Павел', 'Наталья', 'Роман',
             'Айжан', 'Александр', 'Ирина', 'Николай', 'Светлана', 'Михаил', 'Екатерина', 'Антон',
             'Амина', 'Денис', 'Юлия', 'Артур', 'Валентина', 'Станислав', 'Людмила', 'Руслан']
    
    cities = ['Алматы', 'Астана', 'Шымкент', 'Актобе', 'Тараз', 'Павлодар', 'Семей', 'Усть-Каменогорск']
    statuses = ['Обычный', 'VIP', 'Премиальный']
    
    for i in range(50):
        age = random.randint(22, 65)
        balance = random.randint(50000, 2000000)
        city = random.choice(cities)
        status = random.choices(statuses, weights=[0.6, 0.3, 0.1])[0]
        
        clients.append({
            'client_code': 1000 + i,
            'name': random.choice(names),
            'age': age,
            'city': city,
            'status': status,
            'avg_monthly_balance_KZT': balance
        })
    
    # Создаем транзакции
    transactions = []
    categories = [
        'Продукты', 'Кафе и рестораны', 'Такси', 'Отели', 'Путешествия',
        'Играем дома', 'Смотрим дома', 'Едим дома', 'Одежда', 'Красота и здоровье',
        'Транспорт', 'Развлечения', 'Образование', 'Спорт', 'Дом и сад'
    ]
    
    for client in clients:
        # Количество транзакций зависит от возраста и баланса
        num_transactions = random.randint(20, 100)
        
        for _ in range(num_transactions):
            category = random.choice(categories)
            
            # Размер трат зависит от категории и профиля клиента
            base_amount = client['avg_monthly_balance_KZT'] * 0.01  # 1% от баланса как база
            
            # Модификаторы по категориям
            if category == 'Такси' and client['age'] < 35:
                base_amount *= 2  # Молодые больше ездят на такси
            elif category == 'Отели' and client['age'] > 40:
                base_amount *= 1.5  # Старше больше путешествуют
            elif 'дома' in category and client['age'] < 30:
                base_amount *= 1.5  # Молодые больше дома
            elif category == 'Красота и здоровье' and client['age'] > 30:
                base_amount *= 1.3  # Старше больше тратят на красоту
            elif category == 'Образование' and client['age'] < 25:
                base_amount *= 2  # Молодые учатся
            
            # Добавляем случайность
            amount = base_amount * random.uniform(0.1, 3.0)
            
            transactions.append({
                'client_code': client['client_code'],
                'category': category,
                'amount': amount,
                'currency': 'KZT',
                'product': 'Дебетовая карта'
            })
    
    # Создаем переводы
    transfers = []
    transfer_types = ['salary', 'p2p', 'atm', 'business', 'family']
    
    for client in clients:
        # Входящие переводы (зарплата, подарки)
        num_incoming = random.randint(1, 5)
        for _ in range(num_incoming):
            amount = client['avg_monthly_balance_KZT'] * random.uniform(0.1, 0.5)
            transfers.append({
                'client_code': client['client_code'],
                'direction': 'in',
                'amount': amount,
                'currency': 'KZT',
                'type': random.choice(transfer_types)
            })
        
        # Исходящие переводы (платежи, переводы)
        num_outgoing = random.randint(2, 8)
        for _ in range(num_outgoing):
            amount = client['avg_monthly_balance_KZT'] * random.uniform(0.05, 0.3)
            transfers.append({
                'client_code': client['client_code'],
                'direction': 'out',
                'amount': amount,
                'currency': 'KZT',
                'type': random.choice(transfer_types)
            })
    
    # Сохраняем данные
    pd.DataFrame(clients).to_csv('test_clients_realistic.csv', index=False)
    pd.DataFrame(transactions).to_csv('test_transactions_realistic.csv', index=False)
    pd.DataFrame(transfers).to_csv('test_transfers_realistic.csv', index=False)
    
    print(f"✅ Создано реалистичных данных:")
    print(f"  - {len(clients)} клиентов")
    print(f"  - {len(transactions)} транзакций")
    print(f"  - {len(transfers)} переводов")
    
    return clients, transactions, transfers

if __name__ == "__main__":
    create_realistic_test_data()
