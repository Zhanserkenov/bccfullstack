#!/usr/bin/env python3
"""
Тест улучшенной ML логики для проверки разнообразия рекомендаций
"""

import requests
import json
import pandas as pd
from collections import Counter

def test_improved_ml_logic():
    """Тестирует улучшенную ML логику"""
    base_url = "http://localhost:8080"
    
    print("🧪 Тестирование улучшенной ML логики...")
    print("=" * 50)
    
    # 1. Загружаем тестовые данные
    print("📊 Загрузка тестовых данных...")
    
    # Загружаем клиентов
    with open('diverse_results.csv', 'r') as f:
        clients_data = f.read()
    
    # Создаем тестовые данные клиентов
    test_clients = []
    for i in range(50):  # 50 тестовых клиентов
        age = 25 + (i % 40)  # Возраст от 25 до 64
        balance = 50000 + (i % 20) * 100000  # Баланс от 50k до 2M
        status = ['Обычный', 'VIP', 'Премиальный'][i % 3]
        
        test_clients.append({
            'client_code': 1000 + i,
            'name': f'ТестКлиент{i}',
            'age': age,
            'city': 'Алматы',
            'status': status,
            'avg_monthly_balance_KZT': balance
        })
    
    # Создаем тестовые транзакции
    test_transactions = []
    categories = ['Продукты', 'Кафе и рестораны', 'Такси', 'Отели', 'Играем дома', 'Смотрим дома', 'Едим дома']
    
    for client in test_clients:
        for category in categories:
            # Случайные траты в зависимости от возраста и баланса
            base_spending = client['avg_monthly_balance_KZT'] * 0.1
            if category == 'Такси' and client['age'] < 35:
                base_spending *= 2  # Молодые больше ездят на такси
            elif category == 'Отели' and client['age'] > 40:
                base_spending *= 1.5  # Старше больше путешествуют
            elif 'дома' in category and client['age'] < 30:
                base_spending *= 1.5  # Молодые больше дома
            
            amount = base_spending * (0.5 + (client['client_code'] % 10) * 0.1)
            test_transactions.append({
                'client_code': client['client_code'],
                'category': category,
                'amount': amount,
                'currency': 'KZT',
                'product': 'Дебетовая карта'
            })
    
    # Создаем тестовые переводы
    test_transfers = []
    for client in test_clients:
        # Входящие переводы
        test_transfers.append({
            'client_code': client['client_code'],
            'direction': 'in',
            'amount': client['avg_monthly_balance_KZT'] * 0.3,
            'currency': 'KZT',
            'type': 'salary'
        })
        # Исходящие переводы
        test_transfers.append({
            'client_code': client['client_code'],
            'direction': 'out',
            'amount': client['avg_monthly_balance_KZT'] * 0.2,
            'currency': 'KZT',
            'type': 'p2p'
        })
    
    # Сохраняем тестовые данные в CSV
    pd.DataFrame(test_clients).to_csv('test_clients.csv', index=False)
    pd.DataFrame(test_transactions).to_csv('test_transactions.csv', index=False)
    pd.DataFrame(test_transfers).to_csv('test_transfers.csv', index=False)
    
    print(f"✅ Создано {len(test_clients)} клиентов, {len(test_transactions)} транзакций, {len(test_transfers)} переводов")
    
    # 2. Загружаем данные в API
    print("\n📤 Загрузка данных в API...")
    
    # Загружаем клиентов
    with open('test_clients.csv', 'rb') as f:
        response = requests.post(f"{base_url}/upload/clients", files={'file': f})
        print(f"Клиенты: {response.status_code} - {response.json().get('message', 'Ошибка')}")
    
    # Загружаем транзакции
    with open('test_transactions.csv', 'rb') as f:
        response = requests.post(f"{base_url}/upload/transactions", files={'file': f})
        print(f"Транзакции: {response.status_code} - {response.json().get('message', 'Ошибка')}")
    
    # Загружаем переводы
    with open('test_transfers.csv', 'rb') as f:
        response = requests.post(f"{base_url}/upload/transfers", files={'file': f})
        print(f"Переводы: {response.status_code} - {response.json().get('message', 'Ошибка')}")
    
    # 3. Обрабатываем данные
    print("\n⚙️ Обработка данных...")
    response = requests.post(f"{base_url}/process")
    if response.status_code == 200:
        print(f"✅ Обработка: {response.json().get('message', 'Ошибка')}")
    else:
        print(f"❌ Ошибка обработки: {response.text}")
        return
    
    # 4. Получаем статистику
    print("\n📈 Анализ результатов...")
    response = requests.get(f"{base_url}/stats")
    if response.status_code == 200:
        stats = response.json()
        print(f"Всего клиентов: {stats['total_clients']}")
        print(f"С рекомендациями: {stats['clients_with_recommendations']}")
        
        print("\n🎯 Распределение по продуктам:")
        for product, count in stats['product_distribution'].items():
            percentage = (count / stats['total_clients']) * 100
            print(f"  {product}: {count} клиентов ({percentage:.1f}%)")
        
        # Анализ разнообразия
        total_recommendations = sum(stats['product_distribution'].values())
        unique_products = len(stats['product_distribution'])
        
        print(f"\n📊 Анализ разнообразия:")
        print(f"  Уникальных продуктов: {unique_products}/10")
        print(f"  Коэффициент разнообразия: {unique_products/10:.2f}")
        
        # Проверяем, что нет доминирования одного продукта
        max_count = max(stats['product_distribution'].values())
        max_percentage = (max_count / stats['total_clients']) * 100
        
        print(f"  Максимальная доля продукта: {max_percentage:.1f}%")
        
        if max_percentage < 60:  # Ни один продукт не должен доминировать более чем на 60%
            print("✅ Хорошее разнообразие рекомендаций!")
        else:
            print("⚠️ Слишком много рекомендаций одного продукта")
            
        # Проверяем, что есть рекомендации разных типов продуктов
        product_types = {
            'Депозиты': ['Депозит Сберегательный', 'Депозит Накопительный', 'Депозит Мультивалютный'],
            'Карты': ['Кредитная карта', 'Премиальная карта', 'Карта для путешествий'],
            'Инвестиции': ['Инвестиции', 'Золотые слитки'],
            'Другое': ['Кредит наличными', 'Мультивалютный счет']
        }
        
        print(f"\n🏷️ Распределение по типам продуктов:")
        for type_name, products in product_types.items():
            type_count = sum(stats['product_distribution'].get(p, 0) for p in products)
            type_percentage = (type_count / stats['total_clients']) * 100
            print(f"  {type_name}: {type_count} клиентов ({type_percentage:.1f}%)")
    
    else:
        print(f"❌ Ошибка получения статистики: {response.text}")
    
    # 5. Получаем детальные рекомендации для нескольких клиентов
    print(f"\n🔍 Детальные рекомендации (первые 5 клиентов):")
    for i in range(5):
        client_code = 1000 + i
        response = requests.get(f"{base_url}/recommendations/{client_code}")
        if response.status_code == 200:
            rec = response.json()
            print(f"\nКлиент {client_code} ({rec['client_name']}):")
            for j, product in enumerate(rec['recommendations'], 1):
                print(f"  {j}. {product['product']}: {product['benefit_kzt_per_month']:.0f} ₸/мес")
        else:
            print(f"❌ Ошибка получения рекомендаций для клиента {client_code}")

if __name__ == "__main__":
    test_improved_ml_logic()
