import csv
import dataclasses
import json
import argparse
import random
import sys
import uuid
import logging
from datetime import datetime, date, timedelta, timezone
from typing import List, TypeVar
from data_model import Product, Customer, Order, OrderLine

DataClass = TypeVar("DataClass")

log = logging.getLogger("order-generator")
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))


def load_csv_data(file_path: str, data_class: DataClass) -> List[DataClass]:
    """Load data from a CSV file into a list of data classes"""
    result = []
    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            result.append(data_class.from_dict(row))

    return result


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def write_json_data(file_path: str, data: List):
    """Write a list of data classes to a JSON file"""
    with open(file_path, "w") as jf:
        for item in data:
            json.dump(dataclasses.asdict(item), jf, default=json_serial)
            jf.write("\n")


def generate_historical_data(
    start_date: datetime.date,
    end_date: datetime.date,
    max_orders_per_day: int,
    customers: List[Customer],
    products: List[Product],
) -> List[Order]:
    """Generate historical data for the given date range"""
    current_date = start_date
    orders = []
    while current_date <= end_date:
        orders_for_date = random.randint(int(max_orders_per_day / 2), max_orders_per_day)
        seconds_per_order = 86400 / orders_for_date
        for o in range(orders_for_date):
            customer_position = random.randint(0, len(customers) - 1)
            seconds_since_midnight = o * seconds_per_order
            timestamp = datetime.combine(
                current_date, datetime.min.time(), timezone.utc
            ) + timedelta(seconds=seconds_since_midnight)
            order = generate_order(customers[customer_position], products, timestamp)
            orders.append(order)
        current_date = current_date + timedelta(days=1)

    return orders


def generate_order_lines(total_lines, products: List[Product]) -> List[OrderLine]:
    """Generate a list of order lines for a given order"""
    order_lines = []
    for _ in range(total_lines):
        product_index = random.randint(0, len(products) - 1)
        product = products[product_index]
        product_id = product.product_id
        price = product.price
        volume = random.randint(0, 100)
        order_line = OrderLine(product_id, volume, price)
        order_lines.append(order_line)

    return order_lines


def generate_order(customer: Customer, products: List[Product], timestamp: datetime) -> Order:
    """Generate a single order"""
    order_id = str(uuid.uuid4())
    customer_id = customer.customer_id
    total_order_lines = random.randint(1, 100)
    order_lines = generate_order_lines(total_order_lines, products)
    amount = sum(ol.volume * ol.price for ol in order_lines)
    return Order(
        order_id=order_id,
        customer_id=customer_id,
        order_lines=order_lines,
        amount=amount,
        timestamp=timestamp,
    )


def run(customers_path: str, products_path: str):
    """Run the order generator"""
    customers = load_csv_data(customers_path, Customer)
    products = load_csv_data(products_path, Product)

    log.info("Generating historical data")
    orders = generate_historical_data(
        # Yesterday
        date.today() - timedelta(days=1),
        date.today(),
        max_orders_per_day=1000,
        customers=customers,
        products=products,
    )
    orders_file = "../data/recent_orders.json"
    log.info(f"Generated {len(orders)} orders. Writing them to {orders_file}")
    write_json_data(orders_file, orders)
    log.info("Writing done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Order generator")
    parser.add_argument(
        "--customers",
        dest="customers_path",
        required=True,
        help="Path to csv file with customers",
    )
    parser.add_argument(
        "--products",
        dest="products_path",
        required=True,
        help="Path to csv file with products",
    )
    args = parser.parse_args()

    run(
        customers_path=args.customers_path,
        products_path=args.products_path,
    )
