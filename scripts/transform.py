import sqlite3
import os

# Pistas 
# 1. Conectarse a la base de datos donde están las tablas Silver
# 2. Guarda los queries realizados en el trabajo pasado como un string => Gold

def transform_data():
    db_path = '/opt/airflow/dags/data/ecommerce.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    log.info("Ejecutando queries para crear tablas Gold...")

    # Eliminar las tablas si ya existen
    cursor.execute("DROP TABLE IF EXISTS gold_top_states;")
    cursor.execute("DROP TABLE IF EXISTS gold_delivery_comparison;")

    # Query 1: Top 10 estados con mayor ingreso
    query1 = """
    CREATE TABLE gold_top_states AS
    SELECT 
        c.customer_state AS state,
        SUM(p.payment_value) AS total_revenue
    FROM silver_olist_orders o
    JOIN silver_olist_order_payments p ON o.order_id = p.order_id
    JOIN silver_olist_customers c ON o.customer_id = c.customer_id
    GROUP BY state
    ORDER BY total_revenue DESC
    LIMIT 10;
    """

        # Query 2: Comparación entre tiempo estimado y tiempo real por mes y año
    query2 = """
    CREATE TABLE gold_delivery_comparison AS
    SELECT 
        strftime('%Y', order_delivered_customer_date) AS year,
        strftime('%m', order_delivered_customer_date) AS month,
        AVG(julianday(order_delivered_customer_date) - julianday(order_approved_at)) AS actual_delivery_days,
        AVG(julianday(order_estimated_delivery_date) - julianday(order_approved_at)) AS estimated_delivery_days
    FROM silver_olist_orders
    WHERE order_delivered_customer_date IS NOT NULL
    GROUP BY year, month
    ORDER BY year, month;
    """

    print("🚀 Ejecutando queries para crear tablas Gold...")
    cursor.executescript(query1)
    cursor.executescript(query2)

    conn.commit()
    conn.close()
    print("✅ Tablas Gold creadas en ecommerce.db: 'gold_top_states' y 'gold_delivery_comparison'")
