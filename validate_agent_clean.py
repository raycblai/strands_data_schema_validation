import csv
import time
import boto3
import json
import os
from dotenv import load_dotenv
from strands import Agent, tool

# Load environment variables from .env file
load_dotenv()

@tool
def validate_record(record: dict) -> str:
    """
    Validate a single record against schema rules.
    
    Args:
        record: Dictionary containing name, age, and city fields
        
    Returns:
        String: "VALID" or "INVALID: reason"
    """
    # Check if name is non-empty string
    if not record.get('name') or record['name'].strip() == '':
        return "INVALID: 'name' must be non-empty string"
    
    # Check if age is convertible to integer
    try:
        age = int(record['age'])
    except (ValueError, TypeError):
        return "INVALID: 'age' must be convertible to integer"
    
    # Check if city is non-empty string
    if not record.get('city') or record['city'].strip() == '':
        return "INVALID: 'city' must be non-empty string"
    
    return "VALID"

@tool
def write_db(record: dict) -> str:
    """
    Write a VALID record to DynamoDB table ETLDB.
    
    Args:
        record: Dictionary containing name, age, and city fields
        
    Returns:
        String: Success or error message
    """
    try:
        # Use environment credentials (set in main function)
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table = dynamodb.Table('ETLDB')
        
        # Convert age to integer for storage
        item = {
            'name': record['name'],
            'age': int(record['age']),
            'city': record['city']
        }
        
        table.put_item(Item=item)
        return f"SUCCESS: Record written to ETLDB"
    except Exception as e:
        return f"ERROR: Failed to write to ETLDB - {str(e)}"

def main():
    start_time = time.time()
    
    # Create agent with validation and database tools
    agent = Agent(
        model="us.amazon.nova-lite-v1:0",
        tools=[validate_record, write_db]
    )

    # Read last 15 records from CSV
    with open('test_data.csv', 'r') as f:
        reader = csv.DictReader(f)
        records = list(reader)[-15:]
    
    valid_count = 0
    invalid_count = 0
    written_count = 0
    
    # Process each record
    for i, record in enumerate(records, 1):
        prompt = f"Please validate this record using the validate_record tool: {record}. If VALID, also write it to the database using write_db tool."
        
        result = agent(prompt)
        result_text = result.message['content'][0]['text']
        
        # Check if validation failed
        if 'invalid' in result_text.lower() or 'cannot be written' in result_text.lower():
            invalid_count += 1
        else:
            valid_count += 1
            if 'successfully written' in result_text or 'SUCCESS' in result_text:
                written_count += 1

    end_time = time.time()
    execution_time = end_time - start_time

    # Return JSON response
    response = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "message": "Success",
            "data": {
                "records_inspected": len(records),
                "valid_records": valid_count,
                "invalid_records": invalid_count,
                "records_written": written_count,
                "execution_time": round(execution_time, 2)
            }
        }),
        "isBase64Encoded": False
    }
    
    print(json.dumps(response, indent=2))

if __name__ == "__main__":
    main()
