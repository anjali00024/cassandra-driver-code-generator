import requests
import json
import base64

# Test the full API flow for LWT detection
API_URL = "http://localhost:8000/api/execute-query"

# Create a test configuration
config = {
    "database_id": "test-db",
    "token": "test-token",
    "keyspace": "test_keyspace",
    "region": "us-east1",
    "secure_bundle": base64.b64encode(b"test-secure-bundle-content").decode('utf-8')
}

# Test a few LWT patterns
test_patterns = [
    "use lwt",
    "create lwt example",
    "lightweight transaction"
]

print("\nTesting LWT detection through the API:")
print("=" * 50)

for pattern in test_patterns:
    print(f"Testing pattern: '{pattern}'")
    
    # Create the request payload
    payload = {
        "query": pattern,
        "config": config,
        "mode": {
            "mode": "natural_language",
            "driver_type": "python"
        }
    }
    
    try:
        # Send the request to the API
        response = requests.post(API_URL, json=payload)
        
        # Print the response status code
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            # Parse the response JSON
            result = response.json()
            
            # Check if the response contains LWT-specific code
            if "driver_code" in result:
                lwt_detected = "IF NOT EXISTS" in result["driver_code"] or "LWT operation" in result["driver_code"]
                print(f"LWT detected in driver code: {lwt_detected}")
                
                if not lwt_detected:
                    print("WARNING: LWT pattern not detected properly in API response!")
                
                # Show a snippet of the driver code
                code_snippet = result["driver_code"].split('\n')[:5]
                print("Driver code snippet:")
                for line in code_snippet:
                    print(f"  {line}")
            else:
                print("WARNING: No driver_code field in response!")
                print(f"Response: {result}")
        else:
            print(f"API error: {response.text}")
        
        print("=" * 50)
    except Exception as e:
        print(f"Error testing pattern '{pattern}': {e}")
        print("=" * 50) 