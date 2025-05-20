import sys
import os

# Add the backend directory to the path
backend_dir = os.path.join(os.path.dirname(__file__), 'backend')
sys.path.append(backend_dir)

# Import the function directly from main
try:
    from main import parse_natural_language_query
except ImportError as e:
    print(f"Error importing from main.py: {e}")
    sys.exit(1)

# LWT patterns to test
patterns = [
    "use lwt",
    "lwt",
    "lightweight transaction",
    "create lwt example",
    "if not exists",
    "conditional update",
    "use lightweight transactions"
]

# Test each pattern with the function
print("\nTesting LWT pattern detection:")
print("=" * 50)

for pattern in patterns:
    print(f"Testing pattern: '{pattern}'")
    try:
        # Call the function with the python driver type
        result = parse_natural_language_query(pattern, "test_keyspace", "python")
        
        # Check if result contains LWT-specific strings
        lwt_detected = any(marker in result for marker in ["IF NOT EXISTS", "LWT operation"])
        
        print(f"LWT detected: {lwt_detected}")
        if not lwt_detected:
            print("WARNING: LWT pattern not detected properly!")
        
        # Print just a snippet of the result to confirm
        snippet = result.split('\n')[:5]
        print("Result snippet:")
        for line in snippet:
            print(f"  {line}")
        
        print("=" * 50)
    except Exception as e:
        print(f"Error testing pattern '{pattern}': {e}")
        print("=" * 50) 