#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы сервера
"""

import requests
import json
import pandas as pd
import io

# Базовый URL сервера
BASE_URL = "http://localhost:8080"

def test_health():
    """Тест проверки состояния сервера"""
    print("🔍 Проверка состояния сервера...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        if response.status_code == 200:
            print("✅ Сервер работает")
            print(f"   Ответ: {response.json()}")
        else:
            print(f"❌ Ошибка: {response.status_code}")
    except Exception as e:
        print(f"❌ Не удалось подключиться к серверу: {e}")

def create_test_data():
    """Создание тестовых данных"""
    print("\n📊 Создание тестовых данных...")
    
    # Тестовые данные клиентов
    clients_data = {
        'client_code': [1, 2, 3, 4, 5],
        'name': ['Алия', 'Рамазан', 'Мария', 'Алексей', 'Айгуль'],
        'status': ['Премиальный клиент', 'Зарплатный клиент', 'Студент', 'Стандартный клиент', 'Зарплатный клиент'],
        'age': [35, 28, 22, 45, 31],
        'city': ['Алматы', 'Астана', 'Шымкент', 'Алматы', 'Астана'],
        'avg_monthly_balance_KZT': [5000000, 1500000, 500000, 2000000, 1800000]
    }
    
    # Тестовые данные транзакций
    transactions_data = {
        'date': ['2024-01-15', '2024-01-20', '2024-02-10', '2024-02-15', '2024-03-05', '2024-03-20', '2024-01-25', '2024-02-28', '2024-03-10'],
        'category': ['Такси', 'Кафе и рестораны', 'Путешествия', 'Продукты питания', 'Такси', 'Отели', 'Кафе и рестораны', 'Такси', 'Продукты питания'],
        'amount': [5000, 15000, 50000, 25000, 3000, 80000, 20000, 8000, 30000],
        'currency': ['KZT', 'KZT', 'KZT', 'KZT', 'KZT', 'KZT', 'KZT', 'KZT', 'KZT'],
        'client_code': [1, 1, 1, 2, 2, 3, 1, 2, 3],
        'product': ['', '', '', '', '', '', '', '', '']
    }
    
    # Тестовые данные переводов
    transfers_data = {
        'date': ['2024-01-01', '2024-01-15', '2024-02-01', '2024-02-15', '2024-03-01'],
        'type': ['salary_in', 'p2p_out', 'salary_in', 'atm_withdrawal', 'salary_in'],
        'direction': ['in', 'out', 'in', 'out', 'in'],
        'amount': [500000, 100000, 300000, 50000, 400000],
        'currency': ['KZT', 'KZT', 'KZT', 'KZT', 'KZT'],
        'client_code': [1, 1, 2, 2, 3]
    }
    
    # Создание CSV файлов в памяти
    clients_csv = pd.DataFrame(clients_data).to_csv(index=False)
    transactions_csv = pd.DataFrame(transactions_data).to_csv(index=False)
    transfers_csv = pd.DataFrame(transfers_data).to_csv(index=False)
    
    return clients_csv, transactions_csv, transfers_csv

def upload_data(clients_csv, transactions_csv, transfers_csv):
    """Загрузка тестовых данных на сервер"""
    print("\n📤 Загрузка данных на сервер...")
    
    # Загрузка клиентов
    print("   Загрузка клиентов...")
    response = requests.post(
        f"{BASE_URL}/upload/clients",
        files={'file': ('clients.csv', io.StringIO(clients_csv), 'text/csv')}
    )
    if response.status_code == 200:
        print("   ✅ Клиенты загружены")
    else:
        print(f"   ❌ Ошибка загрузки клиентов: {response.text}")
        return False
    
    # Загрузка транзакций
    print("   Загрузка транзакций...")
    response = requests.post(
        f"{BASE_URL}/upload/transactions",
        files={'file': ('transactions.csv', io.StringIO(transactions_csv), 'text/csv')}
    )
    if response.status_code == 200:
        print("   ✅ Транзакции загружены")
    else:
        print(f"   ❌ Ошибка загрузки транзакций: {response.text}")
        return False
    
    # Загрузка переводов
    print("   Загрузка переводов...")
    response = requests.post(
        f"{BASE_URL}/upload/transfers",
        files={'file': ('transfers.csv', io.StringIO(transfers_csv), 'text/csv')}
    )
    if response.status_code == 200:
        print("   ✅ Переводы загружены")
    else:
        print(f"   ❌ Ошибка загрузки переводов: {response.text}")
        return False
    
    return True

def process_data():
    """Обработка данных"""
    print("\n⚙️ Обработка данных...")
    try:
        response = requests.post(f"{BASE_URL}/process")
        if response.status_code == 200:
            print("✅ Данные обработаны")
            result = response.json()
            print(f"   Обработано клиентов: {result['clients_count']}")
        else:
            print(f"❌ Ошибка обработки: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False
    
    return True

def get_recommendations():
    """Получение рекомендаций"""
    print("\n🎯 Получение рекомендаций...")
    try:
        for client_code in [1, 2, 3]:
            response = requests.get(f"{BASE_URL}/recommendations/{client_code}")
            if response.status_code == 200:
                result = response.json()
                print(f"   Клиент {client_code} ({result['client_name']}):")
                for rec in result['recommendations']:
                    print(f"     - {rec['product']}: {rec['benefit_kzt_per_month']:,.0f} ₸/мес")
            else:
                print(f"   ❌ Ошибка для клиента {client_code}: {response.text}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def generate_push_notifications():
    """Генерация пуш-уведомлений"""
    print("\n📱 Генерация пуш-уведомлений...")
    try:
        response = requests.post(f"{BASE_URL}/push-notifications")
        if response.status_code == 200:
            print("✅ Пуш-уведомления сгенерированы")
            result = response.json()
            print(f"   Сгенерировано: {len(result['notifications'])} уведомлений")
            
            # Показываем первые несколько уведомлений
            for i, notification in enumerate(result['notifications'][:3]):
                print(f"\n   Уведомление {i+1}:")
                print(f"     Клиент: {notification['client_code']}")
                print(f"     Продукт: {notification['product']}")
                print(f"     Текст: {notification['push_notification']}")
        else:
            print(f"❌ Ошибка генерации: {response.text}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def export_csv():
    """Экспорт результатов"""
    print("\n📄 Экспорт результатов...")
    try:
        response = requests.get(f"{BASE_URL}/export/csv")
        if response.status_code == 200:
            print("✅ Результаты экспортированы")
            result = response.json()
            print(f"   Экспортировано записей: {len(result['sample'])}")
            
            # Сохраняем CSV файл
            with open('results.csv', 'w', encoding='utf-8') as f:
                f.write(result['csv_content'])
            print("   📁 Файл сохранен как 'results.csv'")
        else:
            print(f"❌ Ошибка экспорта: {response.text}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def get_stats():
    """Получение статистики"""
    print("\n📊 Получение статистики...")
    try:
        response = requests.get(f"{BASE_URL}/stats")
        if response.status_code == 200:
            print("✅ Статистика получена")
            result = response.json()
            print(f"   Всего клиентов: {result['total_clients']}")
            print(f"   С рекомендациями: {result['clients_with_recommendations']}")
            print("   Распределение по продуктам:")
            for product, count in result['product_distribution'].items():
                print(f"     - {product}: {count}")
        else:
            print(f"❌ Ошибка получения статистики: {response.text}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def main():
    """Основная функция тестирования"""
    print("🚀 Запуск тестирования Banking ML Server")
    print("=" * 50)
    
    # Проверка состояния сервера
    test_health()
    
    # Создание тестовых данных
    clients_csv, transactions_csv, transfers_csv = create_test_data()
    
    # Загрузка данных
    if not upload_data(clients_csv, transactions_csv, transfers_csv):
        print("❌ Не удалось загрузить данные. Завершение тестирования.")
        return
    
    # Обработка данных
    if not process_data():
        print("❌ Не удалось обработать данные. Завершение тестирования.")
        return
    
    # Получение рекомендаций
    get_recommendations()
    
    # Генерация пуш-уведомлений
    generate_push_notifications()
    
    # Экспорт результатов
    export_csv()
    
    # Получение статистики
    get_stats()
    
    print("\n" + "=" * 50)
    print("✅ Тестирование завершено успешно!")

if __name__ == "__main__":
    main()
