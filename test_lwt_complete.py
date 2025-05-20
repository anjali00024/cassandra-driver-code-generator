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

def create_table(session):
    """Create the test table if it doesn't exist"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        username text PRIMARY KEY,
        email text,
        created_at timestamp
    )
    """
    session.execute(create_table_query)
    print("Table users created or verified successfully")

def run_lwt_example(session):
    """Run a Lightweight Transaction (LWT) example"""
    # First, create the table if it doesn't exist
    create_table(session)
    
    # Clear any existing data for our test users
    session.execute("DELETE FROM users WHERE username = 'alice'")
    session.execute("DELETE FROM users WHERE username = 'bob'")
    
    print("\n--- Running LWT INSERT Example ---")
    
    # Prepare the INSERT with IF NOT EXISTS statement (LWT)
    insert_query = """
    INSERT INTO users (username, email, created_at)
    VALUES (?, ?, ?)
    IF NOT EXISTS
    """
    prepared = session.prepare(insert_query)
    
    # First attempt should succeed (user doesn't exist)
    username = "alice"
    email = "alice@example.com"
    created_at = datetime.now()
    
    result = session.execute(prepared, (username, email, created_at))
    
    # The first row of the result contains a boolean [applied] field
    if result[0].applied:
        print(f"✅ First INSERT succeeded: User '{username}' was created")
    else:
        print(f"❌ First INSERT failed: User '{username}' already exists")
    
    # Second attempt with the same username should fail (user already exists)
    result = session.execute(prepared, (username, "alice_new@example.com", datetime.now()))
    
    if result[0].applied:
        print(f"✅ Second INSERT succeeded: User '{username}' was created")
    else:
        print(f"❌ Second INSERT failed: User '{username}' already exists")
    
    print("\n--- Running LWT UPDATE Example ---")
    
    # Prepare the UPDATE with IF condition
    update_query = """
    UPDATE users
    SET email = ?
    WHERE username = ?
    IF email = ?
    """
    prepared_update = session.prepare(update_query)
    
    # Update should succeed if current email matches
    result = session.execute(prepared_update, 
                             ("alice_updated@example.com", username, email))
    
    if result[0].applied:
        print(f"✅ UPDATE succeeded: Email for '{username}' was updated")
    else:
        print(f"❌ UPDATE failed: Current email doesn't match condition")
        print(f"   Current values: {dict(result[0])}")
    
    # Verify the update
    select_query = "SELECT * FROM users WHERE username = ?"
    prepared_select = session.prepare(select_query)
    rows = session.execute(prepared_select, (username,))
    
    for row in rows:
        print(f"\nUser data after update:")
        print(f"  Username: {row.username}")
        print(f"  Email: {row.email}")
        print(f"  Created at: {row.created_at}")

def main():
    """Main function to run the examples"""
    cluster = None
    bundle_file = None
    
    try:
        # Setup the database connection
        cluster, session = setup_connection()
        
        # Get the bundle file path
        bundle_file = cluster.metadata.cloud_options['secure_connect_bundle']
        
        # Run the LWT example
        run_lwt_example(session)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up resources
        cleanup(cluster, bundle_file)

if __name__ == "__main__":
    main() 