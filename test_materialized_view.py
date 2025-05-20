#!/usr/bin/env python3
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import base64
import tempfile
import os
import time
from datetime import datetime

# Configuration
ASTRA_DB_ID = "your-database-id"        # Replace with your database ID
ASTRA_DB_REGION = "us-east1"            # Replace with your database region
ASTRA_DB_KEYSPACE = "test_keyspace"     # Replace with your keyspace
ASTRA_DB_TOKEN = "AstraCS:your-token"   # Replace with your Astra DB token

# This would come from your secure bundle download, base64 encoded in your application
# For testing, we'll create a placeholder - replace with your actual bundle in production
DUMMY_BUNDLE_CONTENT = b"This would be your actual secure bundle content"
SECURE_BUNDLE_B64 = base64.b64encode(DUMMY_BUNDLE_CONTENT).decode('utf-8')

def setup_connection():
    """Setup connection to the Cassandra/Astra DB cluster"""
    print("Connecting to Astra DB...")
    
    # Create a temporary file for the secure bundle
    bundle_file = None
    try:
        # Create a temporary file with the decoded bundle content
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp:
            temp.write(base64.b64decode(SECURE_BUNDLE_B64))
            bundle_file = temp.name
        
        # Connect to the cluster
        auth_provider = PlainTextAuthProvider(
            username='token',
            password=ASTRA_DB_TOKEN
        )
        
        cluster = Cluster(
            cloud={
                'secure_connect_bundle': bundle_file
            },
            auth_provider=auth_provider
        )
        
        session = cluster.connect()
        
        # Set the default keyspace
        session.set_keyspace(ASTRA_DB_KEYSPACE)
        print(f"Connected successfully to keyspace: {ASTRA_DB_KEYSPACE}")
        
        return cluster, session
    
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        if bundle_file and os.path.exists(bundle_file):
            os.unlink(bundle_file)
        raise
    
def cleanup(cluster, bundle_file=None):
    """Clean up resources"""
    if cluster:
        print("Shutting down cluster connection...")
        cluster.shutdown()
    
    if bundle_file and os.path.exists(bundle_file):
        print("Removing temporary bundle file...")
        os.unlink(bundle_file)

def create_materialized_view_example(session):
    """Create and demonstrate a materialized view"""
    print("\n--- Creating Materialized View Example ---")
    
    # 1. Create the base table first
    print("Creating base table: orders")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS orders (
        order_id uuid PRIMARY KEY,
        customer_id uuid,
        order_date timestamp,
        status text,
        total decimal
    )
    """
    session.execute(create_table_query)
    print("Table orders created or verified successfully")
    
    # 2. Create the materialized view
    print("\nCreating materialized view: orders_by_customer_status")
    try:
        create_mv_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS orders_by_customer_status AS
        SELECT order_id, customer_id, order_date, status, total
        FROM orders
        WHERE customer_id IS NOT NULL AND status IS NOT NULL AND order_id IS NOT NULL
        PRIMARY KEY ((customer_id, status), order_id)
        """
        session.execute(create_mv_query)
        print("Materialized view orders_by_customer_status created or verified successfully")
    except Exception as e:
        print(f"Error creating materialized view: {e}")
        # Some environments like Astra DB have limitations on MV creation
        print("Note: Some cloud providers may have restrictions on creating materialized views")
    
    # 3. Insert sample data
    print("\nInserting sample data into orders table")
    insert_query = """
    INSERT INTO orders (order_id, customer_id, order_date, status, total)
    VALUES (?, ?, ?, ?, ?)
    """
    prepared = session.prepare(insert_query)
    
    # Create some sample orders for different customers with different statuses
    sample_data = [
        # Customer 1 with various order statuses
        (uuid.uuid4(), uuid.UUID('11111111-1111-1111-1111-111111111111'), datetime.now(), "pending", 99.99),
        (uuid.uuid4(), uuid.UUID('11111111-1111-1111-1111-111111111111'), datetime.now(), "shipped", 149.99),
        (uuid.uuid4(), uuid.UUID('11111111-1111-1111-1111-111111111111'), datetime.now(), "delivered", 199.99),
        
        # Customer 2 with various order statuses
        (uuid.uuid4(), uuid.UUID('22222222-2222-2222-2222-222222222222'), datetime.now(), "pending", 299.99),
        (uuid.uuid4(), uuid.UUID('22222222-2222-2222-2222-222222222222'), datetime.now(), "shipped", 399.99),
        (uuid.uuid4(), uuid.UUID('22222222-2222-2222-2222-222222222222'), datetime.now(), "cancelled", 499.99),
    ]
    
    for data in sample_data:
        session.execute(prepared, data)
    
    print(f"Inserted {len(sample_data)} sample orders")
    
    # 4. Query using the base table
    print("\n--- Querying using the base table (by order_id) ---")
    print("Note: This would be inefficient if you wanted to find all orders for a customer with a specific status")
    
    # Selecting all orders
    rows = session.execute("SELECT * FROM orders LIMIT 3")
    
    print("\nSample orders from base table:")
    for i, row in enumerate(rows):
        print(f"\n  Order {i+1}:")
        print(f"    Order ID: {row.order_id}")
        print(f"    Customer ID: {row.customer_id}")
        print(f"    Status: {row.status}")
        print(f"    Total: {row.total}")
    
    # 5. Query using the materialized view
    print("\n--- Querying using the materialized view (by customer_id and status) ---")
    print("Note: This is efficient for finding all orders for a customer with a specific status")
    
    try:
        # Get all pending orders for customer 1
        customer_id = uuid.UUID('11111111-1111-1111-1111-111111111111')
        status = "pending"
        
        mv_query = """
        SELECT * FROM orders_by_customer_status 
        WHERE customer_id = ? AND status = ?
        """
        prepared_mv = session.prepare(mv_query)
        
        rows = session.execute(prepared_mv, (customer_id, status))
        
        print(f"\nPending orders for customer {customer_id}:")
        count = 0
        for row in rows:
            count += 1
            print(f"  Order ID: {row.order_id}")
            print(f"  Total: {row.total}")
            print(f"  Date: {row.order_date}")
        
        print(f"\nFound {count} pending orders for customer {customer_id}")
        
        # Get all shipped orders for customer 2
        customer_id = uuid.UUID('22222222-2222-2222-2222-222222222222')
        status = "shipped"
        
        rows = session.execute(prepared_mv, (customer_id, status))
        
        print(f"\nShipped orders for customer {customer_id}:")
        count = 0
        for row in rows:
            count += 1
            print(f"  Order ID: {row.order_id}")
            print(f"  Total: {row.total}")
            print(f"  Date: {row.order_date}")
        
        print(f"\nFound {count} shipped orders for customer {customer_id}")
        
    except Exception as e:
        print(f"Error querying materialized view: {e}")
        print("Note: If you couldn't create the MV earlier, this query would fail")

def main():
    """Main function to run the examples"""
    cluster = None
    bundle_file = None
    
    try:
        # Setup the database connection
        cluster, session = setup_connection()
        
        # Get the bundle file path
        bundle_file = cluster.metadata.cloud_options['secure_connect_bundle']
        
        # Run the materialized view example
        create_materialized_view_example(session)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up resources
        cleanup(cluster, bundle_file)

if __name__ == "__main__":
    main() 