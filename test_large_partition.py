#!/usr/bin/env python3
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import base64
import tempfile
import os
import time
from datetime import datetime, timedelta
import random

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

def create_user_activity_table(session):
    """Create the user_activity table for large partition example"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_activity (
        user_id text,
        activity_timestamp timestamp,
        activity_type text,
        details text,
        PRIMARY KEY (user_id, activity_timestamp)
    ) WITH CLUSTERING ORDER BY (activity_timestamp DESC)
    """
    session.execute(create_table_query)
    print("Table user_activity created or verified successfully")

def generate_large_partition(session, num_rows=1000):
    """
    Generate a large partition by inserting many rows for a single user_id
    
    Args:
        session: The Cassandra session
        num_rows: Number of rows to generate (default: 1000)
    """
    print(f"\n--- Generating Large Partition with {num_rows} rows ---")
    
    # Create the table if it doesn't exist
    create_user_activity_table(session)
    
    # Prepare the insert statement
    insert_query = """
    INSERT INTO user_activity (user_id, activity_timestamp, activity_type, details)
    VALUES (?, ?, ?, ?)
    """
    prepared = session.prepare(insert_query)
    
    # Use a fixed user_id to create a large partition
    user_id = "large_partition_user"
    
    # Generate activity types
    activity_types = ["click", "view", "search", "purchase", "login"]
    
    # Starting timestamp (1 day ago)
    start_time = datetime.now() - timedelta(days=1)
    
    # Batch size for inserts (to avoid overwhelming the database)
    batch_size = 100
    batches = num_rows // batch_size
    
    print(f"Inserting {num_rows} activities for user_id: {user_id}")
    
    for batch in range(batches):
        print(f"Batch {batch+1}/{batches}...", end="\r")
        
        for i in range(batch_size):
            activity_timestamp = start_time + timedelta(
                seconds=random.randint(1, 86400)  # Random time within 24 hours
            )
            activity_type = random.choice(activity_types)
            details = f"Sample activity {batch*batch_size + i} details"
            
            # Execute the prepared statement
            session.execute(prepared, (user_id, activity_timestamp, activity_type, details))
    
    print("\nLarge partition data generation complete")

def query_large_partition(session):
    """Query the large partition and demonstrate proper time-slice technique"""
    user_id = "large_partition_user"
    
    print("\n--- Querying Large Partition ---")
    
    # 1. BAD QUERY: Query all rows for the user (will be inefficient for large partitions)
    print("\n❌ PROBLEMATIC QUERY (accesses entire partition):")
    query_bad = "SELECT count(*) FROM user_activity WHERE user_id = ?"
    prepared_bad = session.prepare(query_bad)
    
    start_time = time.time()
    result = session.execute(prepared_bad, (user_id,))
    end_time = time.time()
    
    count = result.one().count
    print(f"  Retrieved {count} rows in {end_time - start_time:.4f} seconds")
    
    # 2. GOOD QUERY: Query with time slice constraints
    print("\n✅ BETTER QUERY (with time slice constraints):")
    # Using a 1-hour time slice
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)
    
    query_good = """
    SELECT count(*) FROM user_activity 
    WHERE user_id = ? 
    AND activity_timestamp >= ? 
    AND activity_timestamp < ?
    """
    prepared_good = session.prepare(query_good)
    
    start_time = time.time()
    result = session.execute(prepared_good, (user_id, one_hour_ago, now))
    end_time = time.time()
    
    count = result.one().count
    print(f"  Retrieved {count} rows in {end_time - start_time:.4f} seconds")
    print(f"  (Time slice: past hour)")
    
    # 3. Sample some data to show the contents
    print("\n📊 Sample data from the time slice:")
    sample_query = """
    SELECT * FROM user_activity 
    WHERE user_id = ? 
    AND activity_timestamp >= ? 
    AND activity_timestamp < ?
    LIMIT 5
    """
    prepared_sample = session.prepare(sample_query)
    
    rows = session.execute(prepared_sample, (user_id, one_hour_ago, now))
    
    for i, row in enumerate(rows):
        print(f"\n  Sample {i+1}:")
        print(f"    User ID: {row.user_id}")
        print(f"    Timestamp: {row.activity_timestamp}")
        print(f"    Activity: {row.activity_type}")
        print(f"    Details: {row.details}")

def main():
    """Main function to run the examples"""
    cluster = None
    bundle_file = None
    
    try:
        # Setup the database connection
        cluster, session = setup_connection()
        
        # Get the bundle file path
        bundle_file = cluster.metadata.cloud_options['secure_connect_bundle']
        
        # Generate a large partition
        generate_large_partition(session, num_rows=1000)
        
        # Query the large partition
        query_large_partition(session)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up resources
        cleanup(cluster, bundle_file)

if __name__ == "__main__":
    main() 