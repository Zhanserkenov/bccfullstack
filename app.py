from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
from typing import Dict, List, Any
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Глобальные переменные для хранения данных
clients_data = None
transactions_data = None
transfers_data = None
merged_data = None

# Счетчики для отображения общего количества данных
data_counts = {
    'clients': 0,
    'transactions': 0,
    'transfers': 0
}

class BankingMLService:
    """Сервис для ML анализа банковских данных и рекомендаций продуктов"""
    
    def __init__(self):
        # Улучшенные формулы с более реалистичными расчетами и разнообразием
        self.benefit_formulas = {
            'Депозит Сберегательный': lambda row: self._calculate_deposit_benefit(row, 0.165, 50000, 200000),
            'Кредит наличными': lambda row: self._calculate_credit_benefit(row),
            'Карта для путешествий': lambda row: self._calculate_travel_card_benefit(row),
            'Кредитная карта': lambda row: self._calculate_credit_card_benefit(row),
            'Премиальная карта': lambda row: self._calculate_premium_card_benefit(row),
            'Мультивалютный счет': lambda row: self._calculate_fx_account_benefit(row),
            'Депозит Накопительный': lambda row: self._calculate_deposit_benefit(row, 0.155, 30000, 150000),
            'Депозит Мультивалютный': lambda row: self._calculate_deposit_benefit(row, 0.145, 40000, 180000),
            'Инвестиции': lambda row: self._calculate_investment_benefit(row),
            'Золотые слитки': lambda row: self._calculate_gold_benefit(row)
        }
        
        self.benefit_caps = {
            'Депозит Сберегательный': 100000,  # Максимум 100k в месяц
            'Кредит наличными': 200000,  # Максимум 200k в месяц
            'Карта для путешествий': 50000,  # Максимум 50k кешбэка
            'Кредитная карта': 100000,  # Максимум 100k кешбэка
            'Премиальная карта': 100000,  # Максимум 100k кешбэка
            'Мультивалютный счет': 80000,  # Максимум 80k в месяц
            'Депозит Накопительный': 90000,  # Максимум 90k в месяц
            'Депозит Мультивалютный': 80000,  # Максимум 80k в месяц
            'Инвестиции': 150000,  # Максимум 150k потенциального дохода
            'Золотые слитки': 100000  # Максимум 100k потенциального дохода
        }
        
        # Инициализация генератора случайных чисел для разнообразия
        import random
        self.random = random.Random(42)  # Фиксированное seed для воспроизводимости
        
        # Шаблоны для пуш-уведомлений
        self.push_templates = {
            'Карта для путешествий': "{name}, в {month} у вас много поездок/такси. С тревел-картой часть расходов вернулась бы кешбэком. Хотите оформить?",
            'Премиальная карта': "{name}, у вас стабильно крупный остаток и траты в ресторанах. Премиальная карта даст повышенный кешбэк и бесплатные снятия. Оформить сейчас.",
            'Кредитная карта': "{name}, ваши топ-категории — {cat1}, {cat2}, {cat3}. Кредитная карта даёт до 10% в любимых категориях и на онлайн-сервисы. Оформить карту.",
            'Мультивалютный счет': "{name}, вы часто платите в {fx_curr}. В приложении выгодный обмен и авто-покупка по целевому курсу. Настроить обмен.",
            'Депозит Сберегательный': "{name}, у вас остаются свободные средства. Разместите их на вкладе — удобно копить и получать вознаграждение. Открыть вклад.",
            'Депозит Накопительный': "{name}, у вас остаются свободные средства. Разместите их на вкладе — удобно копить и получать вознаграждение. Открыть вклад.",
            'Депозит Мультивалютный': "{name}, у вас остаются свободные средства. Разместите их на вкладе — удобно копить и получать вознаграждение. Открыть вклад.",
            'Инвестиции': "{name}, попробуйте инвестиции с низким порогом входа и без комиссий на старт. Открыть счёт.",
            'Кредит наличными': "{name}, если нужен запас на крупные траты — можно оформить кредит наличными с гибкими выплатами. Узнать доступный лимит.",
            'Золотые слитки': "{name}, рассмотрите золотые слитки для диверсификации портфеля. Узнать подробнее."
        }

    def process_data(self, clients_df: pd.DataFrame, transactions_df: pd.DataFrame, transfers_df: pd.DataFrame) -> pd.DataFrame:
        """Обработка и объединение всех данных"""
        try:
            # Агрегация транзакций
            df_transactions_agg = transactions_df.groupby(['client_code', 'category'])['amount'].sum().reset_index()
            df_transactions_agg = df_transactions_agg.rename(columns={'amount': 'total_spent'})
            
            # Агрегация переводов
            df_transfers_agg = transfers_df.groupby(['client_code', 'direction'])['amount'].sum().reset_index()
            df_transfers_agg = df_transfers_agg.rename(columns={'amount': 'total_transfer_amount'})
            
            # Расчет месячных метрик
            df_transactions_monthly = df_transactions_agg.copy()
            df_transactions_monthly['TOTAL_m'] = df_transactions_monthly.groupby('client_code')['total_spent'].transform('sum') / 3
            
            df_transactions_pivot = df_transactions_agg.pivot(index='client_code', columns='category', values='total_spent').fillna(0)
            df_transactions_monthly_pivot = df_transactions_pivot / 3
            df_transactions_monthly_pivot.columns = [col + '_m' for col in df_transactions_monthly_pivot.columns]
            
            # TRAVEL_m
            travel_categories = [col for col in df_transactions_monthly_pivot.columns if 'Такси' in col or 'Путешествия' in col or 'Отели' in col]
            df_transactions_monthly_pivot['TRAVEL_m'] = df_transactions_monthly_pivot[travel_categories].sum(axis=1)
            
            # ONLINE_m
            online_categories = [col for col in df_transactions_monthly_pivot.columns if 'Играем дома' in col or 'Смотрим дома' in col or 'Едим дома' in col]
            df_transactions_monthly_pivot['ONLINE_m'] = df_transactions_monthly_pivot[online_categories].sum(axis=1)
            
            # TOP3_m
            def get_top3_sum(row):
                return row.nlargest(3).sum()
            
            df_transactions_monthly_pivot['TOP3_m'] = df_transactions_monthly_pivot.drop(columns=['TRAVEL_m', 'ONLINE_m']).apply(get_top3_sum, axis=1)
            
            # Переводы
            df_transfers_in_monthly = df_transfers_agg[df_transfers_agg['direction'] == 'in'].copy()
            df_transfers_in_monthly['INFLOWS_m'] = df_transfers_in_monthly['total_transfer_amount'] / 3
            
            df_transfers_out_monthly = df_transfers_agg[df_transfers_agg['direction'] == 'out'].copy()
            df_transfers_out_monthly['OUTFLOWS_m'] = df_transfers_out_monthly['total_transfer_amount'] / 3
            
            # Объединение метрик
            df_monthly_metrics = df_transactions_monthly_pivot.merge(
                df_transfers_in_monthly[['client_code', 'INFLOWS_m']],
                on='client_code',
                how='left'
            ).merge(
                df_transfers_out_monthly[['client_code', 'OUTFLOWS_m']],
                on='client_code',
                how='left'
            )
            
            df_monthly_metrics = df_monthly_metrics.merge(
                df_transactions_monthly[['client_code', 'TOTAL_m']].drop_duplicates(),
                on='client_code',
                how='left'
            )
            
            df_monthly_metrics.fillna(0, inplace=True)
            
            # Расчет флагов
            df_flags = pd.DataFrame({'client_code': clients_df['client_code'].unique()})
            
            # HAS_FX
            df_transactions_fx = transactions_df[transactions_df['currency'] != 'KZT']['client_code'].unique()
            df_flags['HAS_FX'] = df_flags['client_code'].apply(lambda x: x in df_transactions_fx)
            
            # HAS_CC
            df_transactions_cc = transactions_df[transactions_df['product'].fillna('').str.contains('Кредит', na=False)]['client_code'].unique()
            df_flags['HAS_CC'] = df_flags['client_code'].apply(lambda x: x in df_transactions_cc)
            
            # HAS_ATM_P2P
            df_transfers_atm_p2p = transfers_df[transfers_df['type'].fillna('').str.contains('atm|p2p', na=False)]['client_code'].unique()
            df_flags['HAS_ATM_P2P'] = df_flags['client_code'].apply(lambda x: x in df_transfers_atm_p2p)
            
            # Объединение всех данных
            df_merged = clients_df.merge(df_monthly_metrics, on='client_code', how='left')
            df_merged = df_merged.merge(df_flags, on='client_code', how='left')
            
            return df_merged
            
        except Exception as e:
            logger.error(f"Ошибка при обработке данных: {str(e)}")
            raise

    def calculate_benefits(self, df_merged: pd.DataFrame) -> pd.DataFrame:
        """Расчет выгоды по продуктам с улучшенной логикой"""
        try:
            # Расчет выгоды для каждого продукта
            for product, formula in self.benefit_formulas.items():
                benefit_col_name = f'benefit_{product}'
                df_merged[benefit_col_name] = df_merged.apply(formula, axis=1)
                df_merged[benefit_col_name] = df_merged[benefit_col_name].clip(lower=0)
                
                if product in self.benefit_caps:
                    df_merged[benefit_col_name] = df_merged[benefit_col_name].clip(upper=self.benefit_caps[product])
            
            # Добавляем разнообразие через взвешенное ранжирование
            df_merged = self._apply_diverse_ranking(df_merged)
            
            return df_merged
            
        except Exception as e:
            logger.error(f"Ошибка при расчете выгоды: {str(e)}")
            raise

    def _apply_diverse_ranking(self, df_merged: pd.DataFrame) -> pd.DataFrame:
        """Применяет разнообразное ранжирование с принудительным разнообразием"""
        benefit_columns = [col for col in df_merged.columns if col.startswith('benefit_')]
        
        # Применяем принудительное разнообразие
        df_merged = self._apply_forced_diversity(df_merged, benefit_columns)
        
        return df_merged

    def _apply_forced_diversity(self, df_merged: pd.DataFrame, benefit_columns: list) -> pd.DataFrame:
        """Применяет принудительное разнообразие рекомендаций"""
        # Группируем продукты по типам
        product_groups = {
            'deposits': ['Депозит Сберегательный', 'Депозит Накопительный', 'Депозит Мультивалютный'],
            'cards': ['Кредитная карта', 'Премиальная карта', 'Карта для путешествий'],
            'investments': ['Инвестиции', 'Золотые слитки'],
            'other': ['Кредит наличными', 'Мультивалютный счет']
        }
        
        # Целевое распределение (в процентах) - более сбалансированное
        target_distribution = {
            'deposits': 0.30,  # 30% депозиты
            'cards': 0.40,     # 40% карты
            'investments': 0.20, # 20% инвестиции
            'other': 0.10      # 10% прочее
        }
        
        # Создаем пул продуктов для каждого клиента
        def create_diverse_recommendations(row):
            client_benefits = {col: row[col] for col in benefit_columns}
            
            # Сортируем продукты по выгоде
            sorted_products = sorted(client_benefits.items(), key=lambda x: x[1], reverse=True)
            
            # Применяем принудительное разнообразие
            diverse_products = []
            
            # Сначала добавляем лучший продукт из каждой группы (если есть)
            for group, products in product_groups.items():
                group_products = [(col, benefit) for col, benefit in sorted_products 
                                if col.replace('benefit_', '') in products and benefit > 0]
                if group_products:
                    # Берем лучший продукт из группы
                    best_product = group_products[0]
                    diverse_products.append(best_product[0].replace('benefit_', ''))
            
            # Затем добавляем остальные продукты, соблюдая баланс
            for col, benefit in sorted_products:
                product = col.replace('benefit_', '')
                if product in diverse_products or benefit == 0:
                    continue
                
                # Находим группу продукта
                product_group = None
                for group, products in product_groups.items():
                    if product in products:
                        product_group = group
                        break
                
                # Добавляем продукт, если еще не набрали 4
                if len(diverse_products) < 4:
                    diverse_products.append(product)
            
            # Если не набрали 4 продукта, добавляем любые оставшиеся
            for col, benefit in sorted_products:
                product = col.replace('benefit_', '')
                if product not in diverse_products and len(diverse_products) < 4:
                    diverse_products.append(product)
            
            return diverse_products[:4]  # Возвращаем максимум 4 продукта
        
        # Применяем глобальное разнообразие
        df_merged = self._apply_global_diversity(df_merged, benefit_columns, product_groups, target_distribution)
        
        return df_merged

    def _apply_global_diversity(self, df_merged: pd.DataFrame, benefit_columns: list, product_groups: dict, target_distribution: dict) -> pd.DataFrame:
        """Применяет глобальное разнообразие на уровне всех клиентов"""
        total_clients = len(df_merged)
        
        # Вычисляем целевые количества для каждой группы
        target_counts = {}
        for group, percentage in target_distribution.items():
            target_counts[group] = int(total_clients * percentage)
        
        # Собираем все продукты с их выгодами
        all_products = []
        for _, row in df_merged.iterrows():
            for col in benefit_columns:
                product = col.replace('benefit_', '')
                benefit = row[col]
                if benefit > 0:
                    all_products.append((product, benefit, row['client_code']))
        
        # Сортируем по выгоде
        all_products.sort(key=lambda x: x[1], reverse=True)
        
        # Распределяем продукты по группам
        group_assignments = {group: [] for group in product_groups.keys()}
        assigned_clients = set()
        
        # Сначала распределяем по одному лучшему продукту из каждой группы
        for group, products in product_groups.items():
            for product, benefit, client_code in all_products:
                if product in products and client_code not in assigned_clients:
                    group_assignments[group].append((client_code, product))
                    assigned_clients.add(client_code)
                    break
        
        # Затем заполняем оставшиеся места
        for product, benefit, client_code in all_products:
            if client_code in assigned_clients:
                continue
                
            # Находим группу продукта
            product_group = None
            for group, products in product_groups.items():
                if product in products:
                    product_group = group
                    break
            
            if product_group and len(group_assignments[product_group]) < target_counts[product_group]:
                group_assignments[product_group].append((client_code, product))
                assigned_clients.add(client_code)
        
        # Создаем финальные рекомендации
        def get_final_recommendations(row):
            client_code = row['client_code']
            recommendations = []
            
            # Ищем назначенный продукт для этого клиента
            for group, assignments in group_assignments.items():
                for assigned_client, assigned_product in assignments:
                    if assigned_client == client_code:
                        recommendations.append(assigned_product)
                        break
            
            # Если не нашли назначенный продукт, используем лучшие по выгоде
            if not recommendations:
                client_benefits = {col: row[col] for col in benefit_columns}
                sorted_products = sorted(client_benefits.items(), key=lambda x: x[1], reverse=True)
                for col, benefit in sorted_products:
                    if benefit > 0 and len(recommendations) < 4:
                        recommendations.append(col.replace('benefit_', ''))
            
            return recommendations[:4]
        
        df_merged['top4_products'] = df_merged.apply(get_final_recommendations, axis=1)
        df_merged['ranked_products'] = df_merged['top4_products']
        
        return df_merged

    def _calculate_global_product_stats(self, df_merged, benefit_columns):
        """Вычисляет глобальную статистику по продуктам для балансировки"""
        stats = {}
        total_clients = len(df_merged)
        
        for col in benefit_columns:
            product = col.replace('benefit_', '')
            # Считаем сколько клиентов имеют ненулевую выгоду от продукта
            non_zero_count = (df_merged[col] > 0).sum()
            stats[product] = {
                'coverage': non_zero_count / total_clients,
                'avg_benefit': df_merged[col].mean(),
                'max_benefit': df_merged[col].max()
            }
        
        return stats

    def _get_global_balance_factor(self, product, global_stats):
        """Определяет фактор глобального баланса для предотвращения перекосов"""
        if product not in global_stats:
            return 1.0
        
        coverage = global_stats[product]['coverage']
        
        # Если продукт покрывает слишком много клиентов, снижаем его приоритет
        if coverage > 0.6:  # Более 60% клиентов
            return 0.7
        elif coverage > 0.4:  # 40-60% клиентов
            return 0.85
        elif coverage < 0.1:  # Менее 10% клиентов
            return 1.3  # Повышаем приоритет
        else:
            return 1.0

    def _get_client_profile_factor(self, row, product):
        """Определяет фактор профиля клиента для продукта"""
        age = row.get('age', 30)
        balance = row.get('avg_monthly_balance_KZT', 0)
        status = row.get('status', '')
        
        # Возрастные предпочтения
        age_preferences = {
            'Депозит Сберегательный': 1.0 + (age - 30) * 0.01,  # Старше = больше депозиты
            'Депозит Накопительный': 1.0 + (age - 30) * 0.01,
            'Депозит Мультивалютный': 1.0 + (age - 30) * 0.01,
            'Кредит наличными': 1.5 - (age - 25) * 0.02,  # Молодые больше берут кредиты
            'Карта для путешествий': 1.3 - (age - 25) * 0.015,  # Молодые больше путешествуют
            'Кредитная карта': 1.0 + abs(age - 35) * -0.01,  # Пик в 35 лет
            'Премиальная карта': 1.0 + (age - 30) * 0.01,  # Старше = больше денег
            'Мультивалютный счет': 1.0 + abs(age - 40) * -0.01,  # Средний возраст
            'Инвестиции': 1.5 - (age - 25) * 0.02,  # Молодые больше инвестируют
            'Золотые слитки': 1.0 + (age - 40) * 0.01  # Старше = консервативнее
        }
        
        # Статусные предпочтения
        status_preferences = {
            'Премиальная карта': 1.5 if 'Премиальный' in status else 1.0,
            'Депозит Сберегательный': 1.2 if 'VIP' in status else 1.0,
            'Мультивалютный счет': 1.3 if 'VIP' in status else 1.0,
        }
        
        # Балансовые предпочтения
        balance_preferences = {}
        if balance < 100000:
            balance_preferences = {'Кредитная карта': 1.2, 'Карта для путешествий': 1.1}
        elif balance > 1000000:
            balance_preferences = {'Премиальная карта': 1.3, 'Золотые слитки': 1.2, 'Инвестиции': 1.1}
        
        # Объединяем все факторы
        factor = age_preferences.get(product, 1.0)
        factor *= status_preferences.get(product, 1.0)
        factor *= balance_preferences.get(product, 1.0)
        
        return max(0.3, min(2.0, factor))  # Ограничиваем диапазон

    def _get_competition_factor(self, product, benefits):
        """Определяет фактор конкуренции между продуктами"""
        # Схожие продукты конкурируют друг с другом
        product_groups = {
            'deposits': ['Депозит Сберегательный', 'Депозит Накопительный', 'Депозит Мультивалютный'],
            'cards': ['Кредитная карта', 'Премиальная карта', 'Карта для путешествий'],
            'investments': ['Инвестиции', 'Золотые слитки']
        }
        
        # Находим группу продукта
        product_group = None
        for group, products in product_groups.items():
            if product in products:
                product_group = group
                break
        
        if not product_group:
            return 1.0
        
        # Считаем среднюю выгоду в группе
        group_benefits = [benefits[f'benefit_{p}'] for p in product_groups[product_group] if f'benefit_{p}' in benefits]
        if not group_benefits:
            return 1.0
        
        avg_group_benefit = sum(group_benefits) / len(group_benefits)
        current_benefit = benefits.get(f'benefit_{product}', 0)
        
        # Если продукт значительно выше среднего в группе, снижаем его приоритет
        if current_benefit > avg_group_benefit * 1.5:
            return 0.8
        elif current_benefit < avg_group_benefit * 0.5:
            return 1.2
        
        return 1.0

    def _get_trend_factor(self, product):
        """Определяет фактор трендов/сезонности"""
        # Имитируем тренды (в реальности это были бы данные о популярности продуктов)
        trend_factors = {
            'Кредитная карта': 0.9,  # Слегка снижается
            'Инвестиции': 1.1,  # Растет популярность
            'Депозит Сберегательный': 1.05,  # Стабильно популярен
            'Карта для путешествий': 1.2,  # Сезонный рост
            'Премиальная карта': 0.95,  # Слегка снижается
            'Мультивалютный счет': 1.1,  # Растет
            'Золотые слитки': 1.15,  # Растет в нестабильные времена
        }
        
        return trend_factors.get(product, 1.0)

    def generate_push_notification(self, client_data: Dict, product: str) -> str:
        """Генерация персонализированного пуш-уведомления"""
        try:
            name = client_data.get('name', 'Клиент')
            month = datetime.now().strftime('%B')
            
            # Получаем детальную информацию о тратах клиента
            category_columns = [col for col in client_data.keys() if col.endswith('_m') and col not in ['TRAVEL_m', 'ONLINE_m', 'TOP3_m', 'TOTAL_m', 'INFLOWS_m', 'OUTFLOWS_m']]
            category_spending = {col.replace('_m', ''): client_data[col] for col in category_columns if client_data.get(col, 0) > 0}
            top_categories = sorted(category_spending.items(), key=lambda x: x[1], reverse=True)[:3]
            
            # Персонализация по продуктам с конкретными данными
            if product == 'Карта для путешествий':
                travel_amount = client_data.get('TRAVEL_m', 0)
                if travel_amount > 0:
                    # Подсчитываем количество поездок (примерно)
                    taxi_amount = client_data.get('Такси_m', 0)
                    hotel_amount = client_data.get('Отели_m', 0)
                    trips_count = max(1, int((taxi_amount + hotel_amount) / 5000))  # Примерная оценка
                    cashback = travel_amount * 0.04
                    template = f"{name}, в {month} вы сделали {trips_count} поездок на {travel_amount:,.0f} ₸. С картой для путешествий вернули бы ≈{cashback:,.0f} ₸. Откройте карту в приложении."
                else:
                    template = f"{name}, планируете поездки? Карта для путешествий даёт 4% кешбэк на такси, отели и авиабилеты. Оформить карту."
            
            elif product == 'Премиальная карта':
                balance = client_data.get('avg_monthly_balance_KZT', 0)
                restaurant_spending = client_data.get('Кафе и рестораны_m', 0)
                if balance > 1000000:  # Высокий баланс
                    if restaurant_spending > 50000:  # Часто ест в ресторанах
                        template = f"{name}, у вас стабильно крупный остаток и траты в ресторанах. Премиальная карта даст повышенный кешбэк и бесплатные снятия. Оформить сейчас."
                    else:
                        template = f"{name}, у вас высокий остаток на счету ({balance:,.0f} ₸). Премиальная карта даст до 4% кешбэка на все покупки и бесплатные снятия. Подключите сейчас."
                else:
                    template = f"{name}, премиальная карта даёт до 4% кешбэка на все покупки и бесплатные снятия по миру. Оформить карту."
            
            elif product == 'Кредитная карта':
                if len(top_categories) >= 3:
                    cat1, cat2, cat3 = top_categories[0][0], top_categories[1][0], top_categories[2][0]
                    online_spending = client_data.get('ONLINE_m', 0)
                    if online_spending > 0:
                        template = f"{name}, ваши топ-категории — {cat1}, {cat2}, {cat3}. Кредитная карта даёт до 10% в любимых категориях и на онлайн-сервисы. Оформить карту."
                    else:
                        template = f"{name}, ваши топ-категории — {cat1}, {cat2}, {cat3}. Кредитная карта даёт до 10% в любимых категориях. Оформить карту."
                else:
                    template = f"{name}, кредитная карта даёт до 10% кешбэка в любимых категориях и на онлайн-сервисы. Оформить карту."
            
            elif product == 'Мультивалютный счет':
                fx_operations = client_data.get('HAS_FX', False)
                if fx_operations:
                    template = f"{name}, вы часто платите в валюте. В приложении выгодный обмен и авто-покупка по целевому курсу. Настроить обмен."
                else:
                    template = f"{name}, мультивалютный счёт даёт выгодный обмен валют 24/7 без комиссии. Открыть счёт."
            
            elif product == 'Депозит Сберегательный':
                balance = client_data.get('avg_monthly_balance_KZT', 0)
                if balance > 500000:
                    monthly_income = balance * 0.165 / 12  # 16.5% годовых
                    template = f"{name}, у вас остаются свободные средства ({balance:,.0f} ₸). Сберегательный вклад даст {monthly_income:,.0f} ₸ в месяц. Открыть вклад."
                else:
                    template = f"{name}, сберегательный вклад даёт 16,5% годовых с защитой KDIF. Открыть вклад."
            
            elif product == 'Депозит Накопительный':
                balance = client_data.get('avg_monthly_balance_KZT', 0)
                if balance > 300000:
                    monthly_income = balance * 0.155 / 12  # 15.5% годовых
                    template = f"{name}, у вас остаются свободные средства ({balance:,.0f} ₸). Накопительный вклад даст {monthly_income:,.0f} ₸ в месяц. Открыть вклад."
                else:
                    template = f"{name}, накопительный вклад даёт 15,5% годовых с возможностью пополнения. Открыть вклад."
            
            elif product == 'Депозит Мультивалютный':
                balance = client_data.get('avg_monthly_balance_KZT', 0)
                if balance > 400000:
                    monthly_income = balance * 0.145 / 12  # 14.5% годовых
                    template = f"{name}, у вас остаются свободные средства ({balance:,.0f} ₸). Мультивалютный вклад даст {monthly_income:,.0f} ₸ в месяц. Открыть вклад."
                else:
                    template = f"{name}, мультивалютный вклад даёт 14,5% годовых в KZT/USD/RUB/EUR. Открыть вклад."
            
            elif product == 'Инвестиции':
                balance = client_data.get('avg_monthly_balance_KZT', 0)
                if balance > 100000:
                    template = f"{name}, у вас есть свободные средства. Инвестиции дают возможность роста с 0% комиссий в первый год. Открыть счёт."
                else:
                    template = f"{name}, инвестиции доступны от 6 ₸ с 0% комиссий на сделки. Открыть счёт."
            
            elif product == 'Кредит наличными':
                outflows = client_data.get('OUTFLOWS_m', 0)
                if outflows > 200000:  # Высокие расходы
                    template = f"{name}, у вас высокие расходы ({outflows:,.0f} ₸/мес). Кредит наличными даст запас на крупные траты с гибкими выплатами. Узнать лимит."
                else:
                    template = f"{name}, кредит наличными до 2 млн ₸ на 2 месяца без переплаты. Оформить онлайн."
            
            elif product == 'Золотые слитки':
                balance = client_data.get('avg_monthly_balance_KZT', 0)
                if balance > 1000000:  # Высокий баланс
                    template = f"{name}, у вас высокий остаток. Золотые слитки — надёжная защита от инфляции. Узнать подробнее."
                else:
                    template = f"{name}, золотые слитки 999,9 пробы — диверсификация портфеля. Узнать подробнее."
            
            else:
                # Базовый шаблон для неизвестных продуктов
                template = f"{name}, рассмотрите {product} для оптимизации ваших финансов. Узнать подробнее."
            
            return template
            
        except Exception as e:
            logger.error(f"Ошибка при генерации пуш-уведомления: {str(e)}")
            return f"{name}, рассмотрите {product} для оптимизации ваших финансов."

    def _calculate_deposit_benefit(self, row, annual_rate, min_balance, optimal_balance):
        """Расчет выгоды от депозита с учетом баланса и возраста клиента"""
        balance = row.get('avg_monthly_balance_KZT', 0)
        age = row.get('age', 30)
        total_spending = row.get('TOTAL_m', 0)
        
        # Базовый расчет
        base_benefit = balance * annual_rate / 12
        
        # Модификаторы
        # 1. Возрастной фактор (старше клиенты больше склонны к депозитам)
        if age < 25:
            age_factor = 0.6  # Молодые меньше склонны к депозитам
        elif age > 50:
            age_factor = 1.3  # Пожилые больше склонны к депозитам
        else:
            age_factor = 1.0 + (age - 30) * 0.015  # Постепенный рост
        
        # 2. Фактор размера баланса (более гибкий)
        if balance < min_balance:
            balance_factor = 0.5  # Умеренный интерес для малых сумм
        elif min_balance <= balance <= optimal_balance:
            balance_factor = 1.2  # Оптимальный диапазон с бонусом
        else:
            balance_factor = 1.0  # Стабильный интерес для больших сумм
        
        # 3. Фактор консервативности (свободные средства)
        free_money = balance - total_spending
        if free_money > 0:
            conservatism_factor = min(1.3, 1.0 + free_money / 300000)
        else:
            conservatism_factor = 0.7  # Снижаем если тратит больше чем есть
        
        # 4. Случайный фактор для разнообразия
        random_factor = 0.9 + self.random.random() * 0.2
        
        return base_benefit * age_factor * balance_factor * conservatism_factor * random_factor

    def _calculate_credit_benefit(self, row):
        """Расчет выгоды от кредита наличными"""
        outflows = row.get('OUTFLOWS_m', 0)
        balance = row.get('avg_monthly_balance_KZT', 0)
        age = row.get('age', 30)
        has_cc = row.get('HAS_CC', False)
        
        # Базовые условия
        if outflows < 100000 or balance < 50000:
            return 0
        
        # Базовый расчет (экономия на процентах)
        base_benefit = outflows * 0.05  # 5% экономии
        
        # Модификаторы
        # 1. Возрастной фактор (молодые больше берут кредиты)
        age_factor = 1.5 - (age - 25) * 0.02  # Максимум в 25 лет
        age_factor = max(0.5, min(1.5, age_factor))
        
        # 2. Фактор наличия кредитной карты
        cc_factor = 1.2 if has_cc else 0.8
        
        # 3. Фактор стабильности (высокий баланс = стабильность)
        stability_factor = min(1.5, balance / 200000)
        
        # 4. Случайный фактор
        random_factor = 0.7 + self.random.random() * 0.6
        
        return base_benefit * age_factor * cc_factor * stability_factor * random_factor

    def _calculate_travel_card_benefit(self, row):
        """Расчет выгоды от карты для путешествий"""
        travel_spending = row.get('TRAVEL_m', 0)
        total_spending = row.get('TOTAL_m', 0)
        age = row.get('age', 30)
        
        if travel_spending < 5000:  # Снижаем минимальный порог
            return 0
        
        # Базовый кешбэк
        base_benefit = travel_spending * 0.06  # Повышаем до 6% кешбэка
        
        # Модификаторы
        # 1. Доля травел-трат от общих трат
        travel_ratio = travel_spending / max(total_spending, 1)
        ratio_factor = min(2.5, travel_ratio * 15)  # До 2.5x если много путешествий
        
        # 2. Возрастной фактор (молодые больше путешествуют)
        if age < 35:
            age_factor = 1.4  # Молодые
        elif age < 50:
            age_factor = 1.1  # Средний возраст
        else:
            age_factor = 0.8  # Пожилые
        
        # 3. Бонус за активность
        activity_bonus = 1.0 + (total_spending / 150000) * 0.3
        
        # 4. Сезонный фактор (случайный)
        seasonal_factor = 0.9 + self.random.random() * 0.2
        
        return base_benefit * ratio_factor * age_factor * activity_bonus * seasonal_factor

    def _calculate_credit_card_benefit(self, row):
        """Расчет выгоды от кредитной карты"""
        top3_spending = row.get('TOP3_m', 0)
        online_spending = row.get('ONLINE_m', 0)
        total_spending = row.get('TOTAL_m', 0)
        age = row.get('age', 30)
        
        if total_spending < 30000:  # Снижаем минимальный порог
            return 0
        
        # Увеличиваем базовый кешбэк
        base_benefit = top3_spending * 0.12 + online_spending * 0.08  # Повышаем проценты
        
        # Модификаторы
        # 1. Фактор разнообразия трат
        diversity_factor = min(1.8, len([col for col in row.index if col.endswith('_m') and row[col] > 0]) / 3)
        
        # 2. Возрастной фактор (шире диапазон)
        if 25 <= age <= 45:
            age_factor = 1.2  # Оптимальный возраст
        else:
            age_factor = 0.8  # Остальные возрасты
        age_factor = max(0.5, min(1.5, age_factor))
        
        # 3. Фактор стабильности трат
        stability_factor = min(1.5, total_spending / 80000)
        
        # 4. Бонус за активность
        activity_bonus = 1.0 + (total_spending / 200000) * 0.5
        
        # 5. Случайный фактор
        random_factor = 0.9 + self.random.random() * 0.2
        
        return base_benefit * diversity_factor * age_factor * stability_factor * activity_bonus * random_factor

    def _calculate_premium_card_benefit(self, row):
        """Расчет выгоды от премиальной карты"""
        balance = row.get('avg_monthly_balance_KZT', 0)
        total_spending = row.get('TOTAL_m', 0)
        status = row.get('status', '')
        age = row.get('age', 30)
        
        if balance < 500000:  # Высокий порог для премиальной карты
            return 0
        
        # Базовый кешбэк
        base_benefit = total_spending * 0.02  # 2% кешбэк
        
        # Модификаторы
        # 1. Статус клиента
        status_factor = 1.5 if 'Премиальный' in status else 1.0
        
        # 2. Возрастной фактор (старше = больше денег)
        age_factor = 1.0 + (age - 30) * 0.01
        age_factor = max(0.8, min(1.3, age_factor))
        
        # 3. Фактор размера баланса
        balance_factor = min(1.5, balance / 1000000)
        
        # 4. Случайный фактор (меньше случайности для премиум)
        random_factor = 0.9 + self.random.random() * 0.2
        
        return base_benefit * status_factor * age_factor * balance_factor * random_factor

    def _calculate_fx_account_benefit(self, row):
        """Расчет выгоды от мультивалютного счета"""
        has_fx = row.get('HAS_FX', False)
        balance = row.get('avg_monthly_balance_KZT', 0)
        age = row.get('age', 30)
        
        if not has_fx and balance < 200000:
            return 0
        
        # Базовый доход
        base_benefit = balance * 0.12 / 12  # 12% годовых
        
        # Модификаторы
        # 1. Фактор валютных операций
        fx_factor = 2.0 if has_fx else 0.5
        
        # 2. Возрастной фактор (средний возраст больше работает с валютой)
        age_factor = 1.0 + abs(age - 40) * -0.01
        age_factor = max(0.7, min(1.2, age_factor))
        
        # 3. Случайный фактор
        random_factor = 0.7 + self.random.random() * 0.6
        
        return base_benefit * fx_factor * age_factor * random_factor

    def _calculate_investment_benefit(self, row):
        """Расчет выгоды от инвестиций"""
        balance = row.get('avg_monthly_balance_KZT', 0)
        age = row.get('age', 30)
        total_spending = row.get('TOTAL_m', 0)
        
        # Повышаем минимальный порог и добавляем более строгие условия
        if balance < 200000 or total_spending < 100000:  # Выше пороги
            return 0
        
        # Снижаем базовый потенциальный доход
        base_benefit = balance * 0.008  # 0.8% потенциальный доход (было 1.5%)
        
        # Модификаторы
        # 1. Возрастной фактор (только молодые и средний возраст)
        if age < 25 or age > 50:
            age_factor = 0.3  # Снижаем для очень молодых и пожилых
        else:
            age_factor = 1.0 + abs(age - 35) * -0.02  # Пик в 35 лет
        age_factor = max(0.2, min(1.2, age_factor))
        
        # 2. Фактор свободных средств (более строгий)
        free_money = balance - total_spending
        if free_money < 50000:  # Должно быть достаточно свободных средств
            return 0
        free_money_factor = min(1.2, free_money / 200000)
        
        # 3. Фактор риска (более консервативный)
        risk_factor = 0.3 + self.random.random() * 0.4  # Снижаем случайность
        
        # 4. Дополнительный фактор - только для клиентов с высоким доходом
        income_factor = min(1.5, total_spending / 200000)
        
        return base_benefit * age_factor * free_money_factor * risk_factor * income_factor

    def _calculate_gold_benefit(self, row):
        """Расчет выгоды от золотых слитков"""
        balance = row.get('avg_monthly_balance_KZT', 0)
        age = row.get('age', 30)
        
        if balance < 500000:  # Высокий порог для золота
            return 0
        
        # Базовый потенциальный доход
        base_benefit = balance * 0.008  # 0.8% потенциальный доход
        
        # Модификаторы
        # 1. Возрастной фактор (старше = консервативнее)
        age_factor = 1.0 + (age - 40) * 0.01
        age_factor = max(0.7, min(1.3, age_factor))
        
        # 2. Фактор размера баланса
        balance_factor = min(1.3, balance / 2000000)
        
        # 3. Случайный фактор (золото очень волатильно)
        random_factor = 0.3 + self.random.random() * 1.4
        
        return base_benefit * age_factor * balance_factor * random_factor

# Инициализация сервиса
ml_service = BankingMLService()

@app.route('/', methods=['GET'])
def index():
    """Главная страница с веб-интерфейсом"""
    return render_template('index.html')

@app.route('/health', methods=['GET'])
def health_check():
    """Проверка состояния сервера"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/upload/clients', methods=['POST'])
def upload_clients():
    """Загрузка данных клиентов"""
    global clients_data, data_counts
    
    try:
        if 'file' not in request.files:
            return jsonify({"error": "Файл не найден"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "Файл не выбран"}), 400
        
        if file.filename.endswith('.csv'):
            clients_data = pd.read_csv(file)
            data_counts['clients'] = len(clients_data)
        else:
            return jsonify({"error": "Поддерживаются только CSV файлы"}), 400
        
        # Подсчитываем общее количество данных
        total_records = sum(data_counts.values())
        
        logger.info(f"Загружены данные {len(clients_data)} клиентов")
        return jsonify({
            "message": f"Загружены данные {len(clients_data)} клиентов",
            "total_records": total_records,
            "breakdown": {
                "clients": data_counts['clients'],
                "transactions": data_counts['transactions'],
                "transfers": data_counts['transfers']
            },
            "columns": list(clients_data.columns),
            "sample": clients_data.head().to_dict('records')
        })
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке клиентов: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/upload/transactions', methods=['POST'])
def upload_transactions():
    """Загрузка данных транзакций"""
    global transactions_data, data_counts
    
    try:
        if 'file' not in request.files:
            return jsonify({"error": "Файл не найден"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "Файл не выбран"}), 400
        
        if file.filename.endswith('.csv'):
            transactions_data = pd.read_csv(file)
            data_counts['transactions'] = len(transactions_data)
        else:
            return jsonify({"error": "Поддерживаются только CSV файлы"}), 400
        
        # Подсчитываем общее количество данных
        total_records = sum(data_counts.values())
        
        logger.info(f"Загружены данные {len(transactions_data)} транзакций")
        return jsonify({
            "message": f"Загружены данные {len(transactions_data)} транзакций",
            "total_records": total_records,
            "breakdown": {
                "clients": data_counts['clients'],
                "transactions": data_counts['transactions'],
                "transfers": data_counts['transfers']
            },
            "columns": list(transactions_data.columns),
            "sample": transactions_data.head().to_dict('records')
        })
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке транзакций: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/upload/transfers', methods=['POST'])
def upload_transfers():
    """Загрузка данных переводов"""
    global transfers_data, data_counts
    
    try:
        if 'file' not in request.files:
            return jsonify({"error": "Файл не найден"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "Файл не выбран"}), 400
        
        if file.filename.endswith('.csv'):
            transfers_data = pd.read_csv(file)
            data_counts['transfers'] = len(transfers_data)
        else:
            return jsonify({"error": "Поддерживаются только CSV файлы"}), 400
        
        # Подсчитываем общее количество данных
        total_records = sum(data_counts.values())
        
        logger.info(f"Загружены данные {len(transfers_data)} переводов")
        return jsonify({
            "message": f"Загружены данные {len(transfers_data)} переводов",
            "total_records": total_records,
            "breakdown": {
                "clients": data_counts['clients'],
                "transactions": data_counts['transactions'],
                "transfers": data_counts['transfers']
            },
            "columns": list(transfers_data.columns),
            "sample": transfers_data.head().to_dict('records')
        })
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке переводов: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/process', methods=['POST'])
def process_data():
    """Обработка всех данных и расчет рекомендаций"""
    global clients_data, transactions_data, transfers_data, merged_data
    
    try:
        if clients_data is None or transactions_data is None or transfers_data is None:
            return jsonify({"error": "Не все данные загружены. Загрузите клиентов, транзакции и переводы."}), 400
        
        # Обработка данных
        merged_data = ml_service.process_data(clients_data, transactions_data, transfers_data)
        
        # Расчет выгоды
        merged_data = ml_service.calculate_benefits(merged_data)
        
        logger.info(f"Обработаны данные для {len(merged_data)} клиентов")
        
        return jsonify({
            "message": f"Обработаны данные для {len(merged_data)} клиентов",
            "clients_count": len(merged_data),
            "sample": merged_data[['client_code', 'name', 'top4_products']].head().to_dict('records')
        })
        
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/clients', methods=['GET'])
def get_clients_list():
    """Получение списка всех клиентов"""
    global merged_data
    
    try:
        if merged_data is None:
            return jsonify({"error": "Данные не обработаны. Сначала выполните /process"}), 400
        
        clients = []
        for _, row in merged_data.iterrows():
            clients.append({
                "client_code": int(row['client_code']),
                "name": row.get('name', 'Неизвестно'),
                "status": row.get('status', 'Неизвестно'),
                "age": int(row.get('age', 0)),
                "city": row.get('city', 'Неизвестно'),
                "avg_monthly_balance_KZT": float(row.get('avg_monthly_balance_KZT', 0))
            })
        
        return jsonify({
            "clients": clients,
            "total_count": len(clients)
        })
        
    except Exception as e:
        logger.error(f"Ошибка при получении списка клиентов: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/recommendations/<int:client_code>', methods=['GET'])
def get_recommendations(client_code):
    """Получение рекомендаций для конкретного клиента"""
    global merged_data
    
    try:
        if merged_data is None:
            return jsonify({"error": "Данные не обработаны. Сначала выполните /process"}), 400
        
        client_data = merged_data[merged_data['client_code'] == client_code]
        if client_data.empty:
            return jsonify({"error": f"Клиент {client_code} не найден"}), 404
        
        client_row = client_data.iloc[0]
        top4_products = client_row['top4_products']
        
        # Получаем выгоду для каждого продукта
        recommendations = []
        for product in top4_products:
            benefit_col = f'benefit_{product}'
            if benefit_col in client_row:
                benefit = client_row[benefit_col]
                recommendations.append({
                    "product": product,
                    "benefit_kzt_per_month": float(benefit)
                })
        
        return jsonify({
            "client_code": client_code,
            "client_name": client_row.get('name', 'Неизвестно'),
            "recommendations": recommendations
        })
        
    except Exception as e:
        logger.error(f"Ошибка при получении рекомендаций: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/push-notifications', methods=['POST'])
def generate_push_notifications():
    """Генерация персонализированных пуш-уведомлений для всех клиентов"""
    global merged_data
    
    try:
        if merged_data is None:
            return jsonify({"error": "Данные не обработаны. Сначала выполните /process"}), 400
        
        notifications = []
        
        for _, client_row in merged_data.iterrows():
            client_code = client_row['client_code']
            top4_products = client_row['top4_products']
            
            if top4_products:
                # Берем лучший продукт (первый в списке)
                best_product = top4_products[0]
                
                # Генерируем пуш-уведомление
                client_data = client_row.to_dict()
                push_notification = ml_service.generate_push_notification(client_data, best_product)
                
                notifications.append({
                    "client_code": int(client_code),
                    "product": best_product,
                    "push_notification": push_notification
                })
        
        logger.info(f"Сгенерированы пуш-уведомления для {len(notifications)} клиентов")
        
        return jsonify({
            "message": f"Сгенерированы пуш-уведомления для {len(notifications)} клиентов",
            "notifications": notifications
        })
        
    except Exception as e:
        logger.error(f"Ошибка при генерации пуш-уведомлений: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/export/csv', methods=['GET'])
def export_csv():
    """Экспорт результатов в CSV формате"""
    global merged_data
    
    try:
        if merged_data is None:
            return jsonify({"error": "Данные не обработаны. Сначала выполните /process"}), 400
        
        # Подготавливаем данные для экспорта
        export_data = []
        
        for _, client_row in merged_data.iterrows():
            client_code = client_row['client_code']
            top4_products = client_row['top4_products']
            
            if top4_products:
                # Берем лучший продукт
                best_product = top4_products[0]
                
                # Генерируем пуш-уведомление
                client_data = client_row.to_dict()
                push_notification = ml_service.generate_push_notification(client_data, best_product)
                
                export_data.append({
                    "client_code": int(client_code),
                    "product": best_product,
                    "push_notification": push_notification
                })
        
        # Создаем CSV
        df_export = pd.DataFrame(export_data)
        csv_content = df_export.to_csv(index=False)
        
        return jsonify({
            "message": f"Подготовлены данные для экспорта {len(export_data)} записей",
            "csv_content": csv_content,
            "sample": export_data[:5] if export_data else []
        })
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Получение статистики по обработанным данным"""
    global merged_data
    
    try:
        if merged_data is None:
            return jsonify({"error": "Данные не обработаны"}), 400
        
        # Статистика по продуктам
        product_stats = {}
        for _, client_row in merged_data.iterrows():
            top4_products = client_row['top4_products']
            if top4_products:
                best_product = top4_products[0]
                product_stats[best_product] = product_stats.get(best_product, 0) + 1
        
        return jsonify({
            "total_clients": len(merged_data),
            "clients_with_recommendations": len([row for _, row in merged_data.iterrows() if row['top4_products']]),
            "product_distribution": product_stats,
            "data_columns": list(merged_data.columns)
        })
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
