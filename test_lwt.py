import requests
import json
import base64

# Test different patterns
test_queries = [
    "use lwt",
    "lwt",
    "create lwt example",
    "conditional update",
    "large partition"
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
        if response.status_code == 200:
            result = response.json()
            
            # Check what type of pattern was detected
            code = result.get("driver_code", "")
            
            # Print original query and code length for debugging
            print(f"Original query: {result.get('original_query', 'N/A')}")
            print(f"Driver code length: {len(code)} bytes")
            
            # Look for key patterns
            if "IF NOT EXISTS" in code:
                print("✅ LWT pattern detected!")
                # Print relevant LWT code sections
                print("\nLWT related code snippets:")
                
                # Find the IF NOT EXISTS section
                if_not_exists_idx = code.find("IF NOT EXISTS")
                if if_not_exists_idx != -1:
                    start_idx = max(0, code.rfind('\n', 0, if_not_exists_idx))
                    end_idx = min(len(code), code.find('\n', if_not_exists_idx + 20))
                    print(f"... {code[start_idx:end_idx]} ...")
            elif "large partition" in code.lower():
                print("✅ Large partition pattern detected!")
            else:
                print("❌ No special pattern detected")
                # Print first 300 chars to see what's being generated
                print("\nFirst part of response code:")
                print(f"{code[:300]}...")
                
                # Also check for any IF clause
                if " IF " in code:
                    print("\nFound IF clause but not LWT pattern:")
                    if_idx = code.find(" IF ")
                    start_idx = max(0, code.rfind('\n', 0, if_idx))
                    end_idx = min(len(code), code.find('\n', if_idx + 10))
                    print(f"... {code[start_idx:end_idx]} ...")
        else:
            print(f"❌ Error: {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        
print("\nTest completed.") 