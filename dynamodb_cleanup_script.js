require('dotenv').config();
const { DynamoDBClient, ScanCommand, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');
const { marshall, unmarshall } = require('@aws-sdk/util-dynamodb');

// Configuration from environment variables
const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;
const PARTITION_KEY = process.env.DYNAMODB_PARTITION_KEY;
const ATTRIBUTES_TO_REMOVE = process.env.ATTRIBUTES_TO_REMOVE?.split(',').map(attr => attr.trim()) || [];
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 25; // Max batch size for DynamoDB
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES) || 5;
const INITIAL_DELAY = parseInt(process.env.INITIAL_DELAY) || 100; // milliseconds

// Initialize DynamoDB client
const dynamoClient = new DynamoDBClient({
  region: AWS_REGION,
  maxAttempts: MAX_RETRIES
});

// Utility function for exponential backoff delay
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Function to calculate exponential backoff delay
const calculateBackoffDelay = (attempt, baseDelay = INITIAL_DELAY) => {
  return Math.min(baseDelay * Math.pow(2, attempt) + Math.random() * 1000, 30000);
};

// Function to scan all items from the table
async function scanAllItems(tableName) {
  const items = [];
  let lastEvaluatedKey = null;
  let scanCount = 0;

  console.log(`Starting scan of table: ${tableName}`);

  do {
    try {
      const params = {
        TableName: tableName,
        ...(lastEvaluatedKey && { ExclusiveStartKey: lastEvaluatedKey })
      };

      const command = new ScanCommand(params);
      const response = await dynamoClient.send(command);

      if (response.Items) {
        const unmarshalled = response.Items.map(item => unmarshall(item));
        items.push(...unmarshalled);
        scanCount++;
        console.log(`Scan ${scanCount}: Retrieved ${response.Items.length} items (Total: ${items.length})`);
      }

      lastEvaluatedKey = response.LastEvaluatedKey;

      // Add small delay between scans to avoid throttling
      if (lastEvaluatedKey) {
        await delay(50);
      }

    } catch (error) {
      if (error.name === 'ProvisionedThroughputExceededException' || 
          error.name === 'ThrottlingException') {
        console.log('Throttled during scan, applying backoff...');
        await delay(calculateBackoffDelay(1));
        continue;
      }
      throw error;
    }
  } while (lastEvaluatedKey);

  console.log(`Scan completed. Total items retrieved: ${items.length}`);
  return items;
}

// Function to get the primary key schema for the table
async function getTableKeySchema(tableName) {
  return { partitionKey: PARTITION_KEY, sortKey: null };
}

// Function to filter items that have at least one attribute to remove
function filterItemsWithAttributes(items, attributesToRemove) {
  return items.filter(item => {
    return attributesToRemove.some(attr => item.hasOwnProperty(attr));
  });
}

// Function to create update expression for removing attributes
function createUpdateExpression(attributesToRemove, item) {
  const existingAttributes = attributesToRemove.filter(attr => item.hasOwnProperty(attr));
  
  if (existingAttributes.length === 0) {
    return null; // No attributes to remove from this item
  }

  const removeExpression = `REMOVE ${existingAttributes.map((attr, index) => `#attr${index}`).join(', ')}`;
  const expressionAttributeNames = {};
  
  existingAttributes.forEach((attr, index) => {
    expressionAttributeNames[`#attr${index}`] = attr;
  });

  return {
    UpdateExpression: removeExpression,
    ExpressionAttributeNames: expressionAttributeNames
  };
}

// Function to update a single item with retry logic
async function updateItemWithRetry(tableName, key, updateExpression, maxRetries = MAX_RETRIES) {
  let attempt = 0;
  
  while (attempt < maxRetries) {
    try {
      const params = {
        TableName: tableName,
        Key: marshall(key),
        ...updateExpression,
        ReturnValues: 'NONE'
      };

      const command = new UpdateItemCommand(params);
      await dynamoClient.send(command);
      return true;

    } catch (error) {
      if ((error.name === 'ProvisionedThroughputExceededException' || 
           error.name === 'ThrottlingException') && attempt < maxRetries - 1) {
        
        const backoffDelay = calculateBackoffDelay(attempt);
        console.log(`Throttled on attempt ${attempt + 1}, retrying in ${backoffDelay}ms...`);
        await delay(backoffDelay);
        attempt++;
        continue;
      }
      
      // Log error but don't stop the entire process
      console.error(`Failed to update item after ${attempt + 1} attempts:`, error.message);
      return false;
    }
  }
  
  return false;
}

// Function to process items in batches
async function processItemsInBatches(items, tableName, attributesToRemove, keySchema) {
  const batches = [];
  
  // Group items into batches
  for (let i = 0; i < items.length; i += BATCH_SIZE) {
    batches.push(items.slice(i, i + BATCH_SIZE));
  }

  console.log(`Processing ${items.length} items in ${batches.length} batches of ${BATCH_SIZE}`);

  let successCount = 0;
  let errorCount = 0;

  for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
    const batch = batches[batchIndex];
    console.log(`Processing batch ${batchIndex + 1}/${batches.length} (${batch.length} items)`);

    // Process items in the current batch concurrently (but with limited concurrency)
    const batchPromises = batch.map(async (item) => {
      // Extract the primary key
      const key = { [keySchema.partitionKey]: item[keySchema.partitionKey] };
      if (keySchema.sortKey && item[keySchema.sortKey]) {
        key[keySchema.sortKey] = item[keySchema.sortKey];
      }

      // Create update expression (we know these items have attributes to remove)
      const updateExpression = createUpdateExpression(attributesToRemove, item);
      
      const success = await updateItemWithRetry(tableName, key, updateExpression);
      return { success };
    });

    // Wait for all items in the batch to complete
    const batchResults = await Promise.all(batchPromises);
    
    // Count results
    batchResults.forEach(result => {
      if (result.success) {
        successCount++;
      } else {
        errorCount++;
      }
    });

    // Add delay between batches to prevent overwhelming the service
    if (batchIndex < batches.length - 1) {
      await delay(200);
    }

    // Log progress
    console.log(`Batch ${batchIndex + 1} completed. Success: ${successCount}, Errors: ${errorCount}`);
  }

  return { successCount, errorCount };
}

// Main function
async function main() {
  try {
    // Validate environment variables
    if (!TABLE_NAME) {
      throw new Error('DYNAMODB_TABLE_NAME environment variable is required');
    }
    
    if (!PARTITION_KEY) {
      throw new Error('PARTITION_KEY environment variable is required');
    }
    if (!ATTRIBUTES_TO_REMOVE || ATTRIBUTES_TO_REMOVE.length === 0) {
      throw new Error('ATTRIBUTES_TO_REMOVE environment variable is required (comma-separated list)');
    }

    console.log('Starting DynamoDB attribute removal process...');
    console.log(`Table: ${TABLE_NAME}`);
    console.log(`Attributes to remove: ${ATTRIBUTES_TO_REMOVE.join(', ')}`);
    console.log(`Region: ${AWS_REGION}`);
    console.log(`Batch size: ${BATCH_SIZE}`);
    console.log('---');

    // Get table key schema
    const keySchema = await getTableKeySchema(TABLE_NAME);
    console.log(`Using key schema: ${JSON.stringify(keySchema)}`);

    // Scan all items
    const allItems = await scanAllItems(TABLE_NAME);
    
    if (allItems.length === 0) {
      console.log('No items found in the table.');
      return;
    }

    // Filter items that have at least one attribute to remove
    const itemsToUpdate = filterItemsWithAttributes(allItems, ATTRIBUTES_TO_REMOVE);
    
    console.log(`Total items in table: ${allItems.length}`);
    console.log(`Items that need updates: ${itemsToUpdate.length}`);
    console.log(`Items skipped (no target attributes): ${allItems.length - itemsToUpdate.length}`);
    
    if (itemsToUpdate.length === 0) {
      console.log('No items need to be updated. All items are already clean.');
      return;
    }

    // Process items in batches
    const results = await processItemsInBatches(itemsToUpdate, TABLE_NAME, ATTRIBUTES_TO_REMOVE, keySchema);

    // Final summary
    console.log('---');
    console.log('Process completed!');
    console.log(`Total items in table: ${allItems.length}`);
    console.log(`Items that needed updates: ${itemsToUpdate.length}`);
    console.log(`Successfully updated: ${results.successCount}`);
    console.log(`Errors: ${results.errorCount}`);
    console.log(`Items skipped (no target attributes): ${allItems.length - itemsToUpdate.length}`);

  } catch (error) {
    console.error('Fatal error:', error.message);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\nReceived SIGINT, shutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\nReceived SIGTERM, shutting down gracefully...');
  process.exit(0);
});

// Run the script
if (require.main === module) {
  main().catch(error => {
    console.error('Unhandled error:', error);
    process.exit(1);
  });
}

module.exports = {
  main,
  scanAllItems,
  filterItemsWithAttributes,
  processItemsInBatches,
  createUpdateExpression,
  updateItemWithRetry
};
