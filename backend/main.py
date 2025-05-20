from typing_extensions import Literal
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any, Union
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import cassandra
import uuid
import datetime
import tempfile
import os
import base64
import re
from cassandra.cluster import NoHostAvailable, AuthenticationFailed
import random
import time

app = FastAPI(
    title="Astra DB Query Interface",
    description="API for executing CQL queries on Astra DB",
    version="1.0.0"
)

# Enable CORS with more specific configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://localhost:3002", "http://localhost:3003", "http://localhost:3004"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600
)

VALID_MODES = ["driver", "execute", "natural_language"]
VALID_DRIVER_TYPES = ["java", "python"]

class ConnectionConfig(BaseModel):
    database_id: Optional[str] = ""
    token: str
    keyspace: str
    region: str
    secure_bundle: str

    @validator('database_id')
    def validate_database_id(cls, v):
        # No validation needed if it's empty
        if not v or not v.strip():
            return ""
        return v.strip()

    @validator('token')
    def validate_token(cls, v):
        if not v or not v.strip():
            raise ValueError("Token cannot be empty")
        return v.strip()

    @validator('keyspace')
    def validate_keyspace(cls, v):
        if not v or not v.strip():
            raise ValueError("Keyspace cannot be empty")
        return v.strip()

    @validator('region')
    def validate_region(cls, v):
        if not v or not v.strip():
            raise ValueError("Region cannot be empty")
        return v.strip()

    @validator('secure_bundle')
    def validate_secure_bundle(cls, v):
        if not v or not v.strip():
            raise ValueError("Secure bundle cannot be empty")
        try:
            base64.b64decode(v)
        except Exception:
            raise ValueError("Secure bundle must be a valid base64 encoded string")
        return v.strip()

class QueryMode(BaseModel):
    mode: str
    driver_type: Optional[str] = None
    consistency_level: Optional[str] = "LOCAL_QUORUM"
    retry_policy: Optional[str] = "DEFAULT_RETRY_POLICY"
    load_balancing_policy: Optional[str] = "TOKEN_AWARE"
    
    @validator('mode')
    def validate_mode(cls, v):
        if v not in VALID_MODES:
            raise ValueError(f"Mode must be one of {VALID_MODES}")
        return v

    @validator('driver_type')
    def validate_driver_type(cls, v, values):
        if values.get('mode') in ['driver', 'natural_language']:
            if not v or v not in VALID_DRIVER_TYPES:
                raise ValueError(f"Driver type must be one of {VALID_DRIVER_TYPES} when mode is 'driver' or 'natural_language'")
        elif v is not None and values.get('mode') == 'execute':
            raise ValueError("Driver type should not be specified when mode is 'execute'")
        return v
        
    @validator('consistency_level')
    def validate_consistency_level(cls, v):
        valid_consistency_levels = [
            "LOCAL_ONE", "LOCAL_QUORUM", "ALL", "QUORUM", "ONE"
        ]
        if v and v not in valid_consistency_levels:
            raise ValueError(f"Consistency level must be one of {valid_consistency_levels}")
        return v
        
    @validator('retry_policy')
    def validate_retry_policy(cls, v):
        valid_retry_policies = [
            "DEFAULT_RETRY_POLICY", "DOWNGRADING_CONSISTENCY_RETRY_POLICY", 
            "FALLTHROUGH_RETRY_POLICY", "NEVER_RETRY_POLICY"
        ]
        if v and v not in valid_retry_policies:
            raise ValueError(f"Retry policy must be one of {valid_retry_policies}")
        return v
        
    @validator('load_balancing_policy')
    def validate_load_balancing_policy(cls, v):
        valid_load_balancing_policies = [
            "TOKEN_AWARE", "ROUND_ROBIN", "DC_AWARE_ROUND_ROBIN"
        ]
        if v and v not in valid_load_balancing_policies:
            raise ValueError(f"Load balancing policy must be one of {valid_load_balancing_policies}")
        return v

class QueryRequest(BaseModel):
    query: str
    config: ConnectionConfig
    mode: Dict[str, str]

    @validator('query')
    def validate_query(cls, v):
        if not v or not v.strip():
            raise ValueError("Query cannot be empty")
        return v.strip()

def parse_natural_language_query(query: str, keyspace: str, driver_type: str) -> str:
    """
    Convert natural language statements into CQL queries and/or code templates.
    """
    import re  # Import re module locally to ensure it's available
    query = query.lower().strip()
    
    # Detect Large Partition requests
    if "large partition" in query or "big partition" in query or "test partition" in query or ("partition" in query and "size" in query) or ("generate" in query and "partition" in query) or "create a large partition" in query or "large partition example" in query or query.strip() == "create large partition" or "partition large" in query:
        # Extract number of rows if specified
        num_rows_match = re.search(r'(\d+)\s+rows', query)
        num_rows = int(num_rows_match.group(1)) if num_rows_match else 1000
        
        # Extract table name if specified or use default
        table_name = "user_activity"
        table_match = re.search(r'table\s+(\w+)', query)
        if table_match:
            table_name = table_match.group(1)
            
        # Extract partition key if specified or use default
        partition_key = "user_id"
        key_match = re.search(r'key\s+(\w+)', query)
        if key_match:
            partition_key = key_match.group(1)
            
        # Use a fixed partition key value to ensure all rows go to same partition
        partition_value = "large_partition_user"
        
        if driver_type == "python":
            return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

import uuid
import random
import time
from datetime import datetime, timedelta

# Create a table with a simple schema for the large partition test
create_table_query = \"\"\"
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    {partition_key} text,
    activity_timestamp timestamp,
    activity_type text,
    details text,
    PRIMARY KEY ({partition_key}, activity_timestamp)
) WITH CLUSTERING ORDER BY (activity_timestamp DESC)
\"\"\"
session.execute(create_table_query)
print(f"Table {keyspace}.{table_name} created or verified successfully")

# Define activity types for random selection
activity_types = ["login", "click", "view", "purchase", "logout", "scroll", "search"]
detail_templates = [
    "Viewed product: {{}}",
    "Clicked on: {{}}",
    "Searched for: {{}}",
    "Added to cart: {{}}",
    "Purchased item: {{}}",
    "Visited page: {{}}"
]
products = ["laptop", "phone", "headphones", "keyboard", "mouse", "monitor", "tablet", "camera", "speaker", "watch"]

# Use a fixed partition key value to ensure all rows go to same partition
partition_value = "large_partition_user"

# Prepare the insert statement for better performance
prepared_stmt = session.prepare(\"\"\"
    INSERT INTO {keyspace}.{table_name} ({partition_key}, activity_timestamp, activity_type, details)
    VALUES (?, ?, ?, ?)
\"\"\")

# Track timing
start_time = time.time()
batch_size = 100  # Insert in batches for better performance
total_batches = {num_rows} // batch_size + (1 if {num_rows} % batch_size != 0 else 0)

print(f"Generating {num_rows} rows in the same partition ('{partition_value}')...")
print(f"This will create a large partition to test partition size limitations.")

for batch in range(total_batches):
    # Show progress periodically
    if batch % 10 == 0 or batch == total_batches - 1:
        print(f"Processing batch {{batch+1}}/{{total_batches}} ({{(batch+1)*100/total_batches:.1f}}%)")
    
    batch_rows = min(batch_size, {num_rows} - batch * batch_size)
    
    # Process each row in the current batch
    for i in range(batch_rows):
        # Create a random timestamp in the past 30 days
        random_minutes = random.randint(1, 30 * 24 * 60)  # 30 days in minutes
        timestamp = datetime.now() - timedelta(minutes=random_minutes)
        
        # Generate random activity data
        activity_type = random.choice(activity_types)
        detail_template = random.choice(detail_templates)
        detail = detail_template.format(random.choice(products))
        
        # Execute the prepared statement
        session.execute(prepared_stmt, [partition_value, timestamp, activity_type, detail])
    
    # Small delay to avoid overwhelming the system
    if batch < total_batches - 1:
        time.sleep(0.01)

elapsed_time = time.time() - start_time
print(f"Successfully inserted {num_rows} rows into the same partition.")
print(f"All rows have '{partition_key}' = '{partition_value}'")
print(f"Total time: {{elapsed_time:.2f}} seconds")

# Demonstrate proper querying techniques for large partitions
print("\\n--- Querying Large Partition ---")

# 1. BAD QUERY: Query all rows for the user (will be inefficient for large partitions)
print("\\n❌ PROBLEMATIC QUERY (accesses entire partition):")
query_bad = f"SELECT count(*) FROM {keyspace}.{table_name} WHERE {partition_key} = ?"
prepared_bad = session.prepare(query_bad)

start_time = time.time()
result = session.execute(prepared_bad, [partition_value])
end_time = time.time()

count = result.one().count
print(f"  Retrieved {{count}} rows in {{end_time - start_time:.4f}} seconds")

# 2. GOOD QUERY: Query with time slice constraints
print("\\n✅ BETTER QUERY (with time slice constraints):")
# Using a 1-hour time slice
now = datetime.now()
one_hour_ago = now - timedelta(hours=1)

query_good = f\"\"\"
SELECT count(*) FROM {keyspace}.{table_name} 
WHERE {partition_key} = ? 
AND activity_timestamp >= ? 
AND activity_timestamp < ?
\"\"\"
prepared_good = session.prepare(query_good)

start_time = time.time()
result = session.execute(prepared_good, [partition_value, one_hour_ago, now])
end_time = time.time()

count = result.one().count
print(f"  Retrieved {{count}} rows in {{end_time - start_time:.4f}} seconds")
print(f"  (Time slice: past hour)")

# 3. Sample some data to show the contents
print("\\n📊 Sample data from the time slice:")
sample_query = f\"\"\"
SELECT * FROM {keyspace}.{table_name} 
WHERE {partition_key} = ? 
AND activity_timestamp >= ? 
AND activity_timestamp < ?
LIMIT 5
\"\"\"
prepared_sample = session.prepare(sample_query)

rows = session.execute(prepared_sample, [partition_value, one_hour_ago, now])

for i, row in enumerate(rows):
    print(f"\\n  Sample {{i+1}}:")
    print(f"    {partition_key}: {{row.{partition_key}}}")
    print(f"    Timestamp: {{row.activity_timestamp}}")
    print(f"    Activity: {{row.activity_type}}")
    print(f"    Details: {{row.details}}")

print("\\nTo query this large partition, use: SELECT * FROM {keyspace}.{table_name} WHERE {partition_key} = '{partition_value}';")
"""
        else:  # Java
            return "Java driver code for large partition example"
    
    # Detect Lightweight Transactions (LWT) requests
    if "lightweight" in query or "lwt" in query or "if not exists" in query or "conditional" in query or "create lwt" in query or "lwt example" in query or query.strip() == "lwt" or "use lwt" in query:
        # Default to a users table if not specified
        table_name = "users"
        table_match = re.search(r'table\s+(\w+)', query)
        if table_match:
            table_name = table_match.group(1)
            
        # Determine operation type
        operation_type = "insert"
        if "update" in query:
            operation_type = "update"
            
        if driver_type == "python":
            if operation_type == "insert":
                return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

import uuid
from datetime import datetime

# Create the users table if it doesn't exist
create_table_query = \"\"\"
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    username text PRIMARY KEY,
    email text,
    created_at timestamp
)
\"\"\"
session.execute(create_table_query)
print(f"Table {keyspace}.{table_name} created or verified successfully")

# Clear any existing data for our test users to ensure clean results
session.execute(f"DELETE FROM {keyspace}.{table_name} WHERE username = 'alice'")
session.execute(f"DELETE FROM {keyspace}.{table_name} WHERE username = 'bob'")

print("\\n--- Running LWT INSERT Example ---")

# Prepare the INSERT with IF NOT EXISTS statement (LWT)
insert_query = f\"\"\"
INSERT INTO {keyspace}.{table_name} (username, email, created_at)
VALUES (?, ?, ?)
IF NOT EXISTS
\"\"\"
prepared = session.prepare(insert_query)

# First attempt should succeed (user doesn't exist)
username = "alice"
email = "alice@example.com"
created_at = datetime.now()

result = session.execute(prepared, [username, email, created_at])

# The first row of the result contains a boolean [applied] field
if result[0].applied:
    print(f"✅ First INSERT succeeded: User '{{username}}' was created")
else:
    print(f"❌ First INSERT failed: User '{{username}}' already exists")

# Second attempt with the same username should fail (user already exists)
result = session.execute(prepared, [username, "alice_new@example.com", datetime.now()])

if result[0].applied:
    print(f"✅ Second INSERT succeeded: User '{{username}}' was created")
else:
    print(f"❌ Second INSERT failed: User '{{username}}' already exists")
    
# Let's also try with a different user
username = "bob"
email = "bob@example.com"
created_at = datetime.now()

result = session.execute(prepared, [username, email, created_at])
print(f"✅ INSERT for user '{{username}}' applied: {{result[0].applied}}")

print("\\n--- Running LWT UPDATE Example ---")

# Prepare the UPDATE with IF condition
update_query = f\"\"\"
UPDATE {keyspace}.{table_name}
SET email = ?
WHERE username = ?
IF email = ?
\"\"\"
prepared_update = session.prepare(update_query)

# Update should succeed if current email matches
username = "alice"
old_email = "alice@example.com"
new_email = "alice_updated@example.com"

result = session.execute(prepared_update, [new_email, username, old_email])

if result[0].applied:
    print(f"✅ UPDATE succeeded: Email for '{{username}}' was updated")
else:
    print(f"❌ UPDATE failed: Current email doesn't match condition")
    print(f"   Current values: {{dict(result[0])}}")

# Verify the update with a SELECT query
select_query = f"SELECT * FROM {keyspace}.{table_name} WHERE username = ?"
prepared_select = session.prepare(select_query)
rows = session.execute(prepared_select, [username])

for row in rows:
    print(f"\\nUser data after update:")
    print(f"  Username: {{row.username}}")
    print(f"  Email: {{row.email}}")
    print(f"  Created at: {{row.created_at}}")

print("\\n--- Running LWT DELETE Example ---")

# Prepare DELETE with IF condition
delete_query = f\"\"\"
DELETE FROM {keyspace}.{table_name}
WHERE username = ?
IF email = ?
\"\"\"
prepared_delete = session.prepare(delete_query)

# Try to delete with correct condition
result = session.execute(prepared_delete, [username, new_email])

if result[0].applied:
    print(f"✅ DELETE succeeded: User '{{username}}' was deleted")
else:
    print(f"❌ DELETE failed: Email condition not met")
    print(f"   Current values: {{dict(result[0])}}")

# Verify deletion
rows = session.execute(prepared_select, [username])
if not list(rows):
    print(f"Verified: User '{{username}}' has been deleted")
else:
    print(f"User '{{username}}' still exists in the database")

print("\\nLightweight Transaction (LWT) operations complete!")
"""
            elif operation_type == "update":
                return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

from datetime import datetime

# First, ensure we have the table and some data to work with
create_table_query = \"\"\"
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    username text PRIMARY KEY,
    email text,
    created_at timestamp
)
\"\"\"
session.execute(create_table_query)
print(f"Table {keyspace}.{table_name} created or verified successfully")

# Create a user if it doesn't exist
session.execute(\"\"\"
INSERT INTO {keyspace}.{table_name} (username, email, created_at)
VALUES ('alice', 'alice@example.com', toTimestamp(now()))
IF NOT EXISTS
\"\"\")

print("\\n--- Running LWT UPDATE Example ---")

# Show current value
select_query = f"SELECT * FROM {keyspace}.{table_name} WHERE username = 'alice'"
rows = session.execute(select_query)
row = rows.one()
print(f"Current email for alice: {{row.email if row else 'not found'}}")

# Perform conditional update - should succeed
update_query = f\"\"\"
UPDATE {keyspace}.{table_name}
SET email = ?
WHERE username = ?
IF email = ?
\"\"\"
prepared_update = session.prepare(update_query)

result = session.execute(prepared_update, ["alice_updated@example.com", "alice", "alice@example.com"])

if result[0].applied:
    print(f"✅ UPDATE succeeded: Email for alice was updated")
else:
    print(f"❌ UPDATE failed: Current email doesn't match condition")
    print(f"   Current values: {{dict(result[0])}}")

# Try with wrong condition - should fail
result = session.execute(prepared_update, ["alice_updated_again@example.com", "alice", "wrong@example.com"])

if result[0].applied:
    print(f"✅ UPDATE succeeded: Email for alice was updated again")
else:
    print(f"❌ UPDATE failed: Email condition not met")
    print(f"   Current values: {{dict(result[0])}}")

# Verify final value
rows = session.execute(select_query)
row = rows.one()
print(f"Final email for alice: {{row.email if row else 'not found'}}")
"""
        else:  # Java driver
            return "Java driver code for LWT example"
            
    # Detect INSERT requests with row count
    if ("insert" in query or "add" in query or "create" in query) and ("row" in query or "record" in query or "data" in query):
        # Extract number of rows if specified
        num_rows_match = re.search(r'(\d+)', query)
        num_rows = int(num_rows_match.group(1)) if num_rows_match else 10
        
        # Extract table name if specified or use default
        table_name = "users"
        table_match = re.search(r'table\s+(\w+)|into\s+(\w+)', query)
        if table_match:
            table_name = table_match.group(1) if table_match.group(1) else table_match.group(2)
            
        if driver_type == "python":
            return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

import uuid
import random
from datetime import datetime, timedelta

# Define sample data generators
first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", 
               "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"]
last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", 
              "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson"]
domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com", "mail.org"]

# Create the users table if it doesn't exist
create_table_query = \"\"\"
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    id uuid PRIMARY KEY,
    name text,
    email text,
    created_at timestamp
)
\"\"\"
session.execute(create_table_query)
print(f"Table {keyspace}.{table_name} created or verified successfully")

# Prepare the insert statement for better performance
prepared_stmt = session.prepare(\"\"\"
    INSERT INTO {keyspace}.{table_name} (id, name, email, created_at)
    VALUES (?, ?, ?, ?)
\"\"\")

print(f"Inserting {num_rows} rows into {keyspace}.{table_name}...")

# Track timing
start_time = datetime.now()

for i in range({num_rows}):
    # Generate random user data
    user_id = uuid.uuid4()
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    name = f"{{first_name}} {{last_name}}"
    email = f"{{first_name.lower()}}.{{last_name.lower()}}@{{random.choice(domains)}}"
    
    # Random date in the past year
    days_ago = random.randint(0, 365)
    created_at = datetime.now() + timedelta(days=days_ago)
    
    # Insert the row
    session.execute(prepared_stmt, [user_id, name, email, created_at])
    
    # Show progress for larger inserts
    if {num_rows} > 100 and i % ({num_rows} // 10) == 0:
        print(f"Progress: {{i+1}}/{num_rows} rows inserted ({{(i+1)*100/{num_rows}:.1f}}%)")

elapsed_time = (datetime.now() - start_time).total_seconds()
print(f"✅ Successfully inserted {num_rows} rows into {keyspace}.{table_name}")
print(f"Total time: {{elapsed_time:.2f}} seconds")

# Sample the data we just inserted
print("\\n--- Sample Data ---")
rows = session.execute(f"SELECT * FROM {keyspace}.{table_name} LIMIT 5")

for i, row in enumerate(rows):
    print(f"\\nRow {{i+1}}:")
    print(f"  ID: {{row.id}}")
    print(f"  Name: {{row.name}}")
    print(f"  Email: {{row.email}}")
    print(f"  Created At: {{row.created_at}}")

print(f"\\nTo view all data: SELECT * FROM {keyspace}.{table_name};")
"""
        else:  # Java driver
            return "Java driver code for inserting rows"
            
    # Detect SELECT/query requests
    if "select" in query or "query" in query or "get" in query or "retrieve" in query or "show" in query or "list" in query or "all rows" in query or "all records" in query:
        # Extract table name if specified or use default
        table_name = "users"
        table_match = re.search(r'from\s+(\w+)|table\s+(\w+)', query)
        if table_match:
            table_name = table_match.group(1) if table_match.group(1) else table_match.group(2)
        
        # Check if we need to count or retrieve rows
        count_query = "count" in query
        
        # Define limit if specified or use default
        limit_match = re.search(r'limit\s+(\d+)', query)
        limit = int(limit_match.group(1)) if limit_match else 10
        
        if driver_type == "python":
            if count_query:
                return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

# Count rows in the table
count_query = f"SELECT COUNT(*) FROM {keyspace}.{table_name}"
result = session.execute(count_query)
count = result.one().count  # The count is in the first column of the first row

print(f"Total rows in {keyspace}.{table_name}: {{count}}")
"""
            else:
                return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

import time

# Execute query to select data
query = f"SELECT * FROM {keyspace}.{table_name} LIMIT {limit}"
print(f"Executing query: {{query}}")

# Track query performance
start_time = time.time()
rows = session.execute(query)
end_time = time.time()

# Convert rows to a list to get row count (this consumes the iterator)
results = list(rows)
count = len(results)

print(f"Query executed in {{end_time - start_time:.4f}} seconds")
print(f"Retrieved {{count}} rows from {keyspace}.{table_name}")

# Print column names
if count > 0:
    print("\\nColumns:")
    for col_name in results[0]._fields:
        print(f"  - {{col_name}}")

# Print the data
print("\\nData:")
for i, row in enumerate(results):
    print(f"\\nRow {{i+1}}:")
    for field_name in row._fields:
        print(f"  {{field_name}}: {{getattr(row, field_name)}}")

# Show CQL for more queries
print(f"\\nMore query examples:")
print(f"1. Count rows: SELECT COUNT(*) FROM {keyspace}.{table_name};")
print(f"2. Get specific columns: SELECT id, name FROM {keyspace}.{table_name} LIMIT 5;")
print(f"3. Filter by a column: SELECT * FROM {keyspace}.{table_name} WHERE <column> = <value> ALLOW FILTERING;")
"""
        else:  # Java driver
            return "Java driver code for selecting data"
    
    # Handle other query types as default
    return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

# Execute the query
query = "{query}"  # This is your natural language query
print(f"Executing query: {{query}}")

# This is a placeholder. In a real application, you would perform
# appropriate actions based on the query intent.
result = session.execute("SELECT * FROM system.local")
print("Query executed")

# Print some example results
for row in result:
    print(row)
"""

def generate_driver_code(query_request: QueryRequest) -> dict:
    # Code to generate driver code based on the query
    pass

# Function to connect to Astra DB
def connect_to_astra(config: ConnectionConfig):
    # Code to connect to Astra DB
    pass

# Function to execute a query
async def execute_query(query: str, config: ConnectionConfig):
    # Code to execute a query
    pass

# API endpoint for executing queries
@app.post("/api/execute-query")
async def handle_query(request: QueryRequest):
    # Code to handle query requests
    query = request.query.strip()
    
    # Log the query for debugging
    print(f"Received query request: {query}")
    
    # Process based on mode
    mode = request.mode.get('mode', 'execute')
    
    if mode == 'natural_language':
        driver_type = request.mode.get('driver_type', 'python')
        response = parse_natural_language_query(query, request.config.keyspace, driver_type)
        print(f"Generated natural language response for: {query}")
        return {"original_query": query, "driver_code": response}
        
    # Other mode handling...
    
    return {"original_query": query, "result": "Query processed successfully"}

# Root endpoint
@app.get("/")
async def root():
    return {"message": "Astra DB Query Interface API"}

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "ok"}

# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {str(exc)}"}
    ) 