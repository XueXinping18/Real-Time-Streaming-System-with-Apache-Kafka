import json
import psycopg2
from kafka import KafkaConsumer

def run_consumer():
    """
    Consumes post-trade messages from Kafka and inserts them into PostgreSQL.
    Simulates the operations team's database for trade lifecycle management.
    """
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "trades",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="trades-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")
        
        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        # Create trades table with post-trade specific fields
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                trade_id VARCHAR(50) PRIMARY KEY,
                asset_class VARCHAR(50),
                instrument VARCHAR(50),
                side VARCHAR(10),
                quantity NUMERIC(15, 2),
                price NUMERIC(15, 6),
                notional_value NUMERIC(20, 2),
                counterparty VARCHAR(100),
                status VARCHAR(50),
                settlement_venue VARCHAR(50),
                trade_date DATE,
                settlement_date DATE,
                brokerage_fee NUMERIC(12, 2),
                clearing_fee NUMERIC(12, 2),
                exchange_fee NUMERIC(12, 2),
                total_fees NUMERIC(12, 2),
                priority VARCHAR(20),
                stp_eligible BOOLEAN,
                timestamp TIMESTAMP,
                processed_by VARCHAR(100)
            );
            """
        )
        print("[Consumer] ✓ Table 'trades' ready for post-trade processing.")
        print("[Consumer] 🎧 Listening for trade messages...\n")

        message_count = 0
        for message in consumer:
            try:
                trade_data = message.value
                
                insert_query = """
                    INSERT INTO trades (
                        trade_id, asset_class, instrument, side, quantity, price,
                        notional_value, counterparty, status, settlement_venue,
                        trade_date, settlement_date, brokerage_fee, clearing_fee,
                        exchange_fee, total_fees, priority, stp_eligible,
                        timestamp, processed_by
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_id) DO NOTHING;
                """
                
                cur.execute(
                    insert_query,
                    (
                        trade_data["trade_id"],
                        trade_data["asset_class"],
                        trade_data["instrument"],
                        trade_data["side"],
                        trade_data["quantity"],
                        trade_data["price"],
                        trade_data["notional_value"],
                        trade_data["counterparty"],
                        trade_data["status"],
                        trade_data["settlement_venue"],
                        trade_data["trade_date"],
                        trade_data["settlement_date"],
                        trade_data["brokerage_fee"],
                        trade_data["clearing_fee"],
                        trade_data["exchange_fee"],
                        trade_data["total_fees"],
                        trade_data["priority"],
                        trade_data["stp_eligible"],
                        trade_data["timestamp"],
                        trade_data["processed_by"],
                    ),
                )
                
                message_count += 1
                
                # Status-based logging
                status_icon = "✅" if trade_data["stp_eligible"] else "❌"
                print(f"[Consumer] {status_icon} #{message_count} Processed: "
                      f"{trade_data['trade_id']} | {trade_data['instrument']} | "
                      f"${trade_data['notional_value']:,.2f} | "
                      f"{trade_data['status']} | CP: {trade_data['counterparty']}")
                
            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue
                
    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_consumer()
