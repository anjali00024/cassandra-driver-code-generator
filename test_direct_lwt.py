import requests
import json
import base64

# The backend API endpoint
url = "http://localhost:8000/api/execute-query"

# Create a dummy base64 secure bundle
dummy_bundle = base64.b64encode(b"test-secure-bundle-content").decode('utf-8')

# Example config with valid base64 secure bundle
config = {
    "database_id": "test-db",
    "token": "test-token",
    "keyspace": "test_keyspace",
    "region": "us-east1",
    "secure_bundle": dummy_bundle
}

# This query directly includes LWT syntax (IF NOT EXISTS)
direct_lwt_query = """
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

# LWT operation: Insert only if the user doesn't already exist
query = \"\"\"
    INSERT INTO test_keyspace.users (username, email, created_at)
    VALUES (?, ?, ?)
    IF NOT EXISTS
\"\"\"
prepared = session.prepare(query)

# Execute the LWT operation
result = session.execute(prepared, ("test_user", "test@example.com", datetime.utcnow()))

# Check if applied
first_row = result[0]
print(f"LWT applied: {first_row.applied}")
"""

payload = {
    "query": direct_lwt_query,
    "config": config,
    "mode": {
        "mode": "driver",  # Use driver mode to skip the natural language parsing
        "driver_type": "python"
    }
}

try:
    print("Testing direct LWT query (bypassing natural language parser):")
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        result = response.json()
        code = result.get("driver_code", "")
        
        print(f"Response status: {response.status_code}")
        print(f"Driver code length: {len(code)} bytes")
        
        # Check if IF NOT EXISTS is preserved in the generated code
        if "IF NOT EXISTS" in code:
            print("✅ LWT syntax preserved in driver code")
            # Print relevant section
            if_not_exists_idx = code.find("IF NOT EXISTS")
            start_idx = max(0, code.rfind('\n', 0, if_not_exists_idx))
            end_idx = min(len(code), code.find('\n', if_not_exists_idx + 20))
            print(f"... {code[start_idx:end_idx]} ...")
        else:
            print("❌ LWT syntax NOT preserved!")
            print("First 300 chars of code:")
            print(code[:300])
    else:
        print(f"❌ Error: {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"❌ Exception: {str(e)}")

print("\nNow testing natural language mode with explicit 'lwt' keyword:")

natural_query = "use lwt"
natural_payload = {
    "query": natural_query,
    "config": config,
    "mode": {
        "mode": "natural_language",
        "driver_type": "python"
    }
}

try:
    response = requests.post(url, json=natural_payload)
    if response.status_code == 200:
        result = response.json()
        code = result.get("driver_code", "")
        
        print(f"Response status: {response.status_code}")
        print(f"Driver code length: {len(code)} bytes")
        
        # Check for IF NOT EXISTS in generated code
        if "IF NOT EXISTS" in code:
            print("✅ LWT syntax in generated natural language code")
        else:
            print("❌ LWT syntax NOT generated from natural language!")
            print("First 300 chars of code:")
            print(code[:300])
    else:
        print(f"❌ Error: {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"❌ Exception: {str(e)}")

print("\nTest completed.") 