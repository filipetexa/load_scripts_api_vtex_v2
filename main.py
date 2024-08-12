from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from scripts.fetch_orders import fetch_orders_for_day
from scripts.fetch_catalog_and_pricing import fetch_catalog_and_pricing

def main():
    start_date = datetime.strptime('2024-06-01', '%Y-%m-%d')
    end_date = datetime.strptime('2024-06-02', '%Y-%m-%d')
    dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    # Criação de dois ThreadPoolExecutors para executar ambos processos em paralelo
    executor_orders = ThreadPoolExecutor(max_workers=1)
    executor_catalog = ThreadPoolExecutor(max_workers=1)

    # Submissão das tarefas para execução paralela
    future_orders = executor_orders.map(fetch_orders_for_day, [date.strftime('%Y-%m-%d') for date in dates])
    future_catalog = executor_catalog.submit(fetch_catalog_and_pricing(100))

    # Fechamento dos executores após o término das tarefas
    executor_orders.shutdown(wait=True)
    executor_catalog.shutdown(wait=True)

if __name__ == "__main__":
    main()
