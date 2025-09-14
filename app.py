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

class BankingMLService:
    """Сервис для ML анализа банковских данных и рекомендаций продуктов"""
    
    def __init__(self):
        self.benefit_formulas = {
            'Депозит Сберегательный': lambda row: 0.165 / 12 * row['avg_monthly_balance_KZT'],  # 16.5% годовых
            'Кредит наличными': lambda row: 0.12 / 12 * row['avg_monthly_balance_KZT'] if row['OUTFLOWS_m'] > 200000 else 0,  # 12% годовых
            'Карта для путешествий': lambda row: 0.04 * row['TRAVEL_m'],  # 4% кешбэк
            'Кредитная карта': lambda row: 0.10 * row['TOP3_m'] + 0.10 * row['ONLINE_m'],  # 10% на топ-3 и онлайн
            'Премиальная карта': lambda row: 0.02 * row['TOTAL_m'] if row['avg_monthly_balance_KZT'] > 1000000 else 0.01 * row['TOTAL_m'],  # 2% или 1%
            'Мультивалютный счет': lambda row: 0.145 / 12 * row['avg_monthly_balance_KZT'] if row['HAS_FX'] else 0,  # 14.5% годовых
            'Депозит Накопительный': lambda row: 0.155 / 12 * row['avg_monthly_balance_KZT'],  # 15.5% годовых
            'Депозит Мультивалютный': lambda row: 0.145 / 12 * row['avg_monthly_balance_KZT'],  # 14.5% годовых
            'Инвестиции': lambda row: 0.02 * row['avg_monthly_balance_KZT'] if row['avg_monthly_balance_KZT'] > 100000 else 0,  # 2% потенциальный доход
            'Золотые слитки': lambda row: 0.01 * row['avg_monthly_balance_KZT'] if row['avg_monthly_balance_KZT'] > 1000000 else 0  # 1% потенциальный доход
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
        """Расчет выгоды по продуктам"""
        try:
            # Расчет выгоды для каждого продукта
            for product, formula in self.benefit_formulas.items():
                benefit_col_name = f'benefit_{product}'
                df_merged[benefit_col_name] = df_merged.apply(formula, axis=1)
                df_merged[benefit_col_name] = df_merged[benefit_col_name].clip(lower=0)
                
                if product in self.benefit_caps:
                    df_merged[benefit_col_name] = df_merged[benefit_col_name].clip(upper=self.benefit_caps[product])
            
            # Ранжирование продуктов
            benefit_columns = [col for col in df_merged.columns if col.startswith('benefit_')]
            
            def rank_products_by_benefit(row):
                benefit_values = row[benefit_columns]
                ranked_products = benefit_values.sort_values(ascending=False)
                return ranked_products.index.tolist()
            
            df_merged['ranked_products'] = df_merged.apply(rank_products_by_benefit, axis=1)
            df_merged['ranked_products'] = df_merged['ranked_products'].apply(
                lambda product_list: [product.replace('benefit_', '') for product in product_list]
            )
            
            # Топ-4 продукта
            df_merged['top4_products'] = df_merged['ranked_products'].apply(lambda x: x[:4])
            
            return df_merged
            
        except Exception as e:
            logger.error(f"Ошибка при расчете выгоды: {str(e)}")
            raise

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
    global clients_data
    
    try:
        if 'file' not in request.files:
            return jsonify({"error": "Файл не найден"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "Файл не выбран"}), 400
        
        if file.filename.endswith('.csv'):
            clients_data = pd.read_csv(file)
        else:
            return jsonify({"error": "Поддерживаются только CSV файлы"}), 400
        
        logger.info(f"Загружены данные {len(clients_data)} клиентов")
        return jsonify({
            "message": f"Загружены данные {len(clients_data)} клиентов",
            "columns": list(clients_data.columns),
            "sample": clients_data.head().to_dict('records')
        })
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке клиентов: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/upload/transactions', methods=['POST'])
def upload_transactions():
    """Загрузка данных транзакций"""
    global transactions_data
    
    try:
        if 'file' not in request.files:
            return jsonify({"error": "Файл не найден"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "Файл не выбран"}), 400
        
        if file.filename.endswith('.csv'):
            transactions_data = pd.read_csv(file)
        else:
            return jsonify({"error": "Поддерживаются только CSV файлы"}), 400
        
        logger.info(f"Загружены данные {len(transactions_data)} транзакций")
        return jsonify({
            "message": f"Загружены данные {len(transactions_data)} транзакций",
            "columns": list(transactions_data.columns),
            "sample": transactions_data.head().to_dict('records')
        })
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке транзакций: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/upload/transfers', methods=['POST'])
def upload_transfers():
    """Загрузка данных переводов"""
    global transfers_data
    
    try:
        if 'file' not in request.files:
            return jsonify({"error": "Файл не найден"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "Файл не выбран"}), 400
        
        if file.filename.endswith('.csv'):
            transfers_data = pd.read_csv(file)
        else:
            return jsonify({"error": "Поддерживаются только CSV файлы"}), 400
        
        logger.info(f"Загружены данные {len(transfers_data)} переводов")
        return jsonify({
            "message": f"Загружены данные {len(transfers_data)} переводов",
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
