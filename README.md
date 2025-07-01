# DynamoDB Attribute Cleanup Script

This script removes unused attributes from all items in a DynamoDB table, handling rate limiting and throttling for large datasets (2000+ items).

## Features

- ✅ Batch processing with configurable batch sizes
- ✅ Exponential backoff for handling throttling
- ✅ Progress tracking and detailed logging
- ✅ Graceful error handling
- ✅ Environment variable configuration
- ✅ Pre-filtering to only process items that need updates
- ✅ Concurrent processing within batches

## Prerequisites

- Node.js 16+ 
- AWS credentials configured (via AWS CLI, IAM roles, or environment variables)
- DynamoDB table with read/write permissions

## Installation

1. Clone or download the script files
2. Install dependencies:
   ```bash
   npm install
   ```

3. Configure your environment variables in `.env` file:
   ```bash
   cp .env.example .env
   # Edit .env with your table name and attributes
   ```

## Configuration

### Required Environment Variables

- `DYNAMODB_TABLE_NAME`: Name of your DynamoDB table
- `DYNAMODB_PARTITION_KEY`: Name of your DynamoDB table partition key
- `ATTRIBUTES_TO_REMOVE`: Comma-separated list of attribute names to remove

### Optional Environment Variables

- `AWS_REGION`: AWS region (default: us-east-1)
- `BATCH_SIZE`: Number of items to process per batch (default: 25, max: 25)
- `MAX_RETRIES`: Maximum retry attempts for throttled requests (default: 5)
- `INITIAL_DELAY`: Initial delay in milliseconds for exponential backoff (default: 100)

### AWS Credentials

The script uses the standard AWS SDK credential chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM roles (recommended for EC2/Lambda)

## Required IAM Permissions

Your AWS credentials need the following permissions for the target table:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:Scan",
                "dynamodb:UpdateItem"
            ],
            "Resource": "arn:aws:dynamodb:region:account:table/your-table-name"
        }
    ]
}
```

## Usage

1. **Configure your environment**:
   ```bash
   # Edit .env file
   DYNAMODB_TABLE_NAME=my-devices-table
   ATTRIBUTES_TO_REMOVE=obsoleteField,unusedAttribute,deprecatedData
   AWS_REGION=us-west-2
   ```

2. **Run the script**:
   ```bash
   npm start
   # or
   node cleanup-dynamodb-attributes.js
   ```

3. **Monitor progress**: The script provides detailed logging:
   ```
   Starting DynamoDB attribute removal process...
   Table: my-devices-table
   Attributes to remove: obsoleteField, unusedAttribute, deprecatedData
   Region: us-west-2
   Batch size: 25
   ---
   Starting scan of table: my-devices-table
   Scan 1: Retrieved 100 items (Total: 100)
   Scan 2: Retrieved 100 items (Total: 200)
   ...
   Total items in table: 2547
   Items that need updates: 1834
   Items skipped (no target attributes): 713
   Processing 1834 items in 74 batches of 25
   Processing batch 1/74 (25 items)
   Batch 1 completed. Success: 25, Errors: 0
   ...
   ```

## Important Notes

### Primary Key Assumption
The script assumes your table uses `deviceId` as the partition key. If your table uses a different key schema, update the `getTableKeySchema()` function:

```javascript
async function getTableKeySchema(tableName) {
  return { 
    partitionKey: 'your-partition-key', 
    sortKey: 'your-sort-key' // or null if no sort key
  };
}
```

### Throttling Handling
The script implements several strategies to handle DynamoDB throttling:
- Exponential backoff with jitter
- Configurable retry limits  
- Delays between batches and scans
- Graceful degradation on persistent errors

### Memory Usage
For very large tables (100k+ items), consider:
- Running on a machine with adequate memory
- Processing in smaller chunks by modifying the scan logic
- Using DynamoDB Streams for real-time processing instead

## Error Handling

- **Throttling**: Automatic retry with exponential backoff
- **Item-level errors**: Logged but don't stop the process
- **Network errors**: Retried according to `MAX_RETRIES`
- **Fatal errors**: Stop execution with detailed error message

## Monitoring and Verification

After running the script:

1. **Check CloudWatch metrics** for your DynamoDB table
2. **Verify a few items** manually to ensure attributes were removed
3. **Review the final summary** for success/error counts

## Safety Considerations

⚠️ **Important**: This script modifies your DynamoDB data. Always:

1. **Test on a non-production table first**
2. **Create a backup** of your table before running
3. **Verify the attributes list** is correct
4. **Run with a small batch size** initially to test

## Troubleshooting

### Common Issues

1. **"Table not found"**: Check table name and region
2. **"Access denied"**: Verify IAM permissions
3. **"Throttling"**: Reduce batch size or increase delays
4. **Memory issues**: Process smaller chunks or increase instance memory

### Performance Tuning

For optimal performance:
- Start with default settings
- Monitor CloudWatch metrics  
- Adjust `BATCH_SIZE` based on your table's capacity
- Increase `INITIAL_DELAY` if experiencing frequent throttling

## License

MIT License - feel free to modify and use as needed.
