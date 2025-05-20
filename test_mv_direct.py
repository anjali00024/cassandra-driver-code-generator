import sys
import os
# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

# Import the function directly
from main import parse_natural_language_query

# Test queries
queries = [
    "materialized view",
    "create mv",
    "create materialized view for table orders"
]

# Test the function directly
for query in queries:
    print(f"\n{'='*60}")
    print(f"Testing: '{query}'")
    print(f"{'='*60}")
    
    # Call the function directly with Python driver type
    result = parse_natural_language_query(query, "test_keyspace", "python")
    
    # Print the first 15 lines to check if it's correctly generating the code
    lines = result.split('\n')
    print('\n'.join(lines[:15]))
    
    # Check if it contains materialized view syntax
    if "CREATE MATERIALIZED VIEW" in result:
        print("\n✅ Contains Materialized View syntax")
    else:
        print("\n❌ Missing Materialized View syntax") 