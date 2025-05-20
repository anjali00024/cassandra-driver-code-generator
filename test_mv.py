import requests
import json
import base64

# Test different patterns for Materialized Views
test_queries = [
    "materialized view",
    "create materialized view",
    "create mv",
    "mv",
    "create a materialized view",
    "create materialized view for table orders",
    "materialized view with customer_id primary key"
]

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

# Test each query pattern
for query in test_queries:
    print(f"\n{'='*40}")
    print(f"Testing: '{query}'")
    print(f"{'='*40}")
    
    payload = {
        "query": query,
        "config": config,
        "mode": {
            "mode": "natural_language",
            "driver_type": "python"
        }
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        
        response_data = response.json()
        
        # Check if the response contains driver code
        if "driver_code" in response_data:
            print("✅ Successfully detected as Materialized View command")
            # Print a snippet from the generated code
            code_snippet = response_data["driver_code"].split("\n")[:10]
            print("\nCode snippet from generated driver code:")
            print("\n".join(code_snippet))
            
            # Check if the code contains the MV-specific syntax
            if "CREATE MATERIALIZED VIEW" in response_data["driver_code"]:
                print("\n✅ Contains Materialized View creation syntax")
            else:
                print("\n❌ Does not contain Materialized View creation syntax")
        else:
            print("❌ Failed to detect as Materialized View command")
            print(f"Response: {response_data}")
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error: {e}")
        
print("\nTest completed!") 