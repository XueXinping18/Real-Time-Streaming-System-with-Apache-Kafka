import time
import json
import uuid
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def generate_post_trade_transaction():
    """
    Generates synthetic post-trade transactions similar to what a hedge fund processes.
    Includes: confirmations, settlements, breaks, and reconciliation statuses.
    """
    
    # Asset classes typical in hedge funds
    asset_classes = ["Equity", "Fixed Income", "Derivative", "FX", "Commodity"]
    
    # Trade sides
    sides = ["Buy", "Sell"]
    
    # Counterparties (prime brokers, custodians)
    counterparties = [
        "Goldman Sachs", "JP Morgan", "Morgan Stanley", "BNP Paribas", 
        "State Street", "Northern Trust", "Citi", "Credit Suisse"
    ]
    
    # Post-trade statuses
    statuses = [
        "Pending Confirmation",  # 40%
        "Confirmed",             # 35%
        "Settlement Pending",    # 15%
        "Settled",              # 8%
        "Break - Mismatch",     # 1.5% (errors)
        "Break - Missing Trade"  # 0.5% (errors)
    ]
    
    status_weights = [0.40, 0.35, 0.15, 0.08, 0.015, 0.005]
    
    # Instruments
    instruments = {
        "Equity": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "JPM", "BAC", "GS"],
        "Fixed Income": ["US10Y", "US30Y", "CORP_AAA", "CORP_BBB", "MUNI"],
        "Derivative": ["SPX_CALL", "SPX_PUT", "VIX_FUT", "ES_FUT", "SWAP_5Y"],
        "FX": ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD"],
        "Commodity": ["GC_FUT", "CL_FUT", "NG_FUT", "SI_FUT"]
    }
    
    # Settlement venues
    venues = ["DTC", "Euroclear", "Clearstream", "CME", "ICE", "OCC"]
    
    # Generate transaction
    asset_class = random.choice(asset_classes)
    instrument = random.choice(instruments[asset_class])
    side = random.choice(sides)
    counterparty = random.choice(counterparties)
    status = random.choices(statuses, weights=status_weights)[0]
    venue = random.choice(venues)
    
    # Quantity and price logic
    if asset_class == "Equity":
        quantity = random.randint(100, 50000)
        price = round(random.uniform(50, 500), 2)
    elif asset_class == "Fixed Income":
        quantity = random.randint(100000, 10000000)  # Notional
        price = round(random.uniform(95, 105), 4)  # Bond price
    elif asset_class == "Derivative":
        quantity = random.randint(1, 100)  # Contracts
        price = round(random.uniform(1, 50), 2)
    elif asset_class == "FX":
        quantity = random.randint(100000, 5000000)  # Notional
        price = round(random.uniform(0.5, 1.5), 6)
    else:  # Commodity
        quantity = random.randint(1, 500)
        price = round(random.uniform(50, 2000), 2)
    
    notional_value = quantity * price
    
    # Fees (basis points on notional)
    brokerage_fee = round(notional_value * random.uniform(0.0001, 0.0015), 2)
    clearing_fee = round(notional_value * random.uniform(0.00005, 0.0003), 2)
    exchange_fee = round(notional_value * random.uniform(0.00003, 0.0002), 2)
    total_fees = round(brokerage_fee + clearing_fee + exchange_fee, 2)
    
    # Trade date and settlement date (T+1, T+2, T+3 depending on asset class)
    trade_date = datetime.now() - timedelta(days=random.randint(0, 3))
    
    settlement_days = {
        "Equity": 2,
        "Fixed Income": 1,
        "Derivative": 1,
        "FX": 2,
        "Commodity": 1
    }
    
    settlement_date = trade_date + timedelta(days=settlement_days[asset_class])
    
    # Priority (for operations team)
    priority = "High" if "Break" in status else "Normal"
    if notional_value > 1000000:
        priority = "High"
    
    # STP (Straight-Through Processing) eligible
    stp_eligible = status not in ["Break - Mismatch", "Break - Missing Trade"]
    
    return {
        "trade_id": str(uuid.uuid4())[:12],
        "asset_class": asset_class,
        "instrument": instrument,
        "side": side,
        "quantity": quantity,
        "price": price,
        "notional_value": round(notional_value, 2),
        "counterparty": counterparty,
        "status": status,
        "settlement_venue": venue,
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "settlement_date": settlement_date.strftime("%Y-%m-%d"),
        "brokerage_fee": brokerage_fee,
        "clearing_fee": clearing_fee,
        "exchange_fee": exchange_fee,
        "total_fees": total_fees,
        "priority": priority,
        "stp_eligible": stp_eligible,
        "timestamp": datetime.now().isoformat(),
        "processed_by": fake.name()  # Operations analyst
    }

def run_producer():
    """Kafka producer that sends post-trade transactions to the 'trades' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")
        print("[Producer] 🏦 Starting Post-Trade Transaction Stream...")
        print("-" * 80)
        
        count = 0
        while True:
            trade = generate_post_trade_transaction()
            
            # Color code by status
            status_emoji = {
                "Pending Confirmation": "⏳",
                "Confirmed": "✓",
                "Settlement Pending": "📋",
                "Settled": "✅",
                "Break - Mismatch": "❌",
                "Break - Missing Trade": "⚠️"
            }
            
            emoji = status_emoji.get(trade["status"], "•")
            
            print(f"[Producer] {emoji} Trade #{count}: {trade['trade_id']} | "
                  f"{trade['instrument']} | {trade['side']} {trade['quantity']} | "
                  f"${trade['notional_value']:,.2f} | {trade['status']}")
            
            future = producer.send("trades", value=trade)
            record_metadata = future.get(timeout=10)
            
            producer.flush()
            count += 1
            
            # Variable speed: faster during market hours simulation
            sleep_time = random.uniform(0.3, 1.5)
            time.sleep(sleep_time)
            
    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_producer()
