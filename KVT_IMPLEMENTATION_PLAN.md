# KVT Command Implementation Plan for EloqKV

## Overview
The KVT (Key-Value Transaction) command is a new Redis command that provides transaction-based key-value operations with table management capabilities. This command will integrate with the existing transaction service infrastructure while providing a Redis-compatible interface.

## Command Syntax
```
KVT cmd args ...
```

Where `cmd` and `args` can be:
- `create_table tablename partition_method` - Creates a new table
- `start_tx` - Returns a transaction ID
- `get tx_id tablename key` - Retrieves a value within a transaction
- `set tx_id tablename key value` - Sets a value within a transaction
- `scan tx_id tablename keystart keyend` - Scans keys within a range, returns cursor_id and results
- `read_cursor cursor_id` - Continues reading from a cursor if cursor_id != 0
- `commit_tx tx_id` - Commits a transaction
- `abort_tx tx_id` - Aborts a transaction

## Architecture Analysis

### Current System Components
1. **Redis Command Layer** (`src/redis_command.cpp`)
   - Command parsing and routing
   - RedisCommandType enum for command classification
   - Integration with transaction service

2. **Transaction Service** (`tx_service/`)
   - Transaction management (start, commit, abort)
   - Key-value operations within transactions
   - Table management and schema handling
   - Concurrency control and conflict resolution

3. **Store Handlers** (`store_handler/`)
   - Data persistence layer
   - Support for multiple storage backends (RocksDB, BigTable, DynamoDB)

### Integration Points
- Redis command parsing → Transaction service calls
- Transaction service → Store handler operations
- Redis response formatting → Client communication

## Implementation Plan

### Phase 1: Command Infrastructure Setup

#### 1.1 Add KVT Command Type
**File**: `include/redis_command.h`
- Add `KVT` to the `RedisCommandType` enum
- Position it appropriately in the command hierarchy

#### 1.2 Add KVT Command to Redis Command Table
**File**: `include/redis/commands.def`
- Add KVT command entry to `redisCommandTable[]`
- Define command flags, ACL categories, and argument specifications
- Set up subcommand structure for different KVT operations

#### 1.3 Create KVT Command Handler Stub
**File**: `src/redis_command.cpp`
- Add minimal `kvtCommand` function stub that delegates to KVT handler
- Add command routing logic in the main command handler
- **Minimal Integration**: Only add the command parsing and routing, no implementation logic

### Phase 2: Core KVT Implementation

#### 2.1 KVT Command Parser
**File**: `src/kvt_manager.cpp` (NEW FILE)
- Parse KVT command and subcommands
- Validate argument counts and types
- Route to appropriate subcommand handlers
- **All parsing logic goes in dedicated KVT manager files**

#### 2.2 Subcommand Handlers
**File**: `src/kvt_manager.cpp` (NEW FILE)
Implement individual handlers for each KVT subcommand:

1. **create_table**
   - Validate table name and partition method
   - Call transaction service to create table schema
   - Return success/failure response

2. **start_tx**
   - Generate new transaction ID
   - Initialize transaction context
   - Return transaction ID to client

3. **get**
   - Validate transaction ID and table name
   - Execute read operation within transaction
   - Return value or error

4. **set**
   - Validate transaction ID, table name, and key
   - Execute write operation within transaction
   - Return success/failure

5. **scan**
   - Initialize or continue key scanning
   - Return cursor ID and batch of results
   - Handle range-based scanning logic

6. **read_cursor**
   - Continue reading from existing cursor
   - Return next batch of results
   - Handle cursor completion

7. **commit_tx**
   - Validate transaction ID
   - Commit transaction through transaction service
   - Return success/failure

8. **abort_tx**
   - Validate transaction ID
   - Abort transaction through transaction service
   - Return success/failure

**All implementation logic goes in dedicated KVT manager files**

### Phase 3: Transaction Service Integration

#### 3.1 Transaction Context Management
**File**: `src/kvt_manager.cpp` (NEW FILE)
- Create `KvtTransactionContext` class
- **Centralized Transaction Management**: Store transaction contexts in a global hash map instead of per-connection storage
- **Single-Operation Transactions**: Handle transaction ID 0 as single-operation transactions (similar to Redis PUT/GET commands)
- Manage transaction state and metadata
- Handle transaction lifecycle

#### 3.2 Table Management Integration
**File**: `src/kvt_manager.cpp` (NEW FILE)
- Integrate with existing catalog factory
- Handle table creation and validation
- Support partition method configuration

#### 3.3 Key-Value Operations
**File**: `src/kvt_manager.cpp` (NEW FILE)
- Implement get/set operations within transactions
- Handle key formatting and validation
- Integrate with store handlers for data persistence

#### 3.4 Scanning Implementation
**File**: `src/kvt_manager.cpp` (NEW FILE)
- Implement cursor-based scanning
- Handle range queries efficiently
- Manage cursor lifecycle and cleanup

**All transaction service integration logic goes in dedicated KVT manager files**

### Phase 4: Error Handling and Response Formatting

#### 4.1 Error Handling
**File**: `src/kvt_manager.cpp` (NEW FILE)
- Define KVT-specific error codes
- Handle transaction validation errors
- Provide meaningful error messages

#### 4.2 Response Formatting
**File**: `src/kvt_manager.cpp` (NEW FILE)
- Format responses according to Redis protocol
- Handle different response types (simple strings, arrays, errors)
- Ensure compatibility with Redis clients

**All error handling and response formatting logic goes in dedicated KVT manager files**

### Phase 5: Testing and Validation

#### 5.1 Unit Tests
- Test individual subcommand handlers
- Validate error handling
- Test transaction lifecycle

#### 5.2 Integration Tests
- Test end-to-end KVT operations
- Validate transaction consistency
- Test concurrent transaction handling

#### 5.3 Performance Testing
- Measure transaction throughput
- Validate scanning performance
- Test with large datasets

## File Modifications Required

### 1. `include/redis_command.h`
```cpp
// Add to RedisCommandType enum
KVT,

// Add KVT command handler declaration
void kvtCommand(RedisCommandContext& ctx);
```

### 2. `include/redis/commands.def`
```cpp
// Add KVT command entry
{MAKE_CMD("kvt","Key-Value Transaction operations","Depends on subcommand","1.0.0",CMD_DOC_NONE,NULL,NULL,"generic",COMMAND_GROUP_GENERIC,KVT_History,0,KVT_Tips,0,kvtCommand,-2,CMD_WRITE,ACL_CATEGORY_KEYSPACE,KVT_Keyspecs,0,NULL,0),.subcommands=KVT_Subcommands},
```

### 3. `src/redis_command.cpp` - MINIMAL MODIFICATIONS ONLY
```cpp
// Add KVT command type handling
case RedisCommandType::KVT:
    kvtCommand(ctx);
    break;

// Implement minimal kvtCommand function stub
void kvtCommand(RedisCommandContext& ctx) {
    // Delegate to KVT handler - minimal stub only
    KvtHandler::handleCommand(ctx);
}
```

### 4. New Files to Create - ALL IMPLEMENTATION LOGIC GOES HERE
- `src/kvt_manager.cpp` - **Core KVT implementation (ALL LOGIC)**
- `include/kvt_manager.h` - **KVT manager declarations (ALL INTERFACES)**
- `src/kvt_transaction.cpp` - Transaction context management
- `src/kvt_transaction.h` - Transaction context declarations
- `src/kvt_transaction_manager.cpp` - Centralized transaction management
- `src/kvt_transaction_manager.h` - Centralized transaction manager declarations

**Note**: Existing files are modified ONLY for command parsing and routing stubs. All actual implementation logic goes in the dedicated KVT manager files.

## Implementation Details

### File Modification Strategy
**Existing Files - MINIMAL CHANGES ONLY:**
- `include/redis_command.h` - Add KVT to RedisCommandType enum
- `include/redis/commands.def` - Add KVT command table entry
- `src/redis_command.cpp` - Add minimal command routing stub

**New Files - ALL IMPLEMENTATION LOGIC:**
- `include/kvt_manager.h` - Complete KVT manager interface
- `src/kvt_manager.cpp` - Complete KVT implementation

### Command Parsing Strategy
1. Parse `KVT` as main command
2. Extract subcommand (create_table, start_tx, etc.)
3. Validate argument count based on subcommand
4. **Transaction ID Handling**: Special handling for transaction ID 0 (single-operation mode)
5. Route to appropriate handler

**All parsing and implementation logic goes in `kvt_manager.cpp`**

### Transaction Management
**File**: `src/kvt_manager.cpp` (NEW FILE)
1. Use existing transaction service infrastructure
2. **Centralized Storage**: Maintain transaction contexts in a global hash map indexed by transaction ID
3. **Single-Operation Mode**: Handle transaction ID 0 as immediate single operations (no transaction context needed)
4. **Global Cleanup**: Handle transaction cleanup through centralized management
5. Support nested transactions if needed

**All transaction management logic goes in dedicated KVT manager files**

### Centralized Transaction Architecture
**File**: `src/kvt_manager.cpp` (NEW FILE)
1. **Global Transaction Manager**: Single instance managing all active transactions
2. **Transaction ID Mapping**: Hash map from transaction ID to transaction context
3. **Single-Operation Transactions**: Transaction ID 0 bypasses transaction context creation
4. **Thread Safety**: Concurrent access protection for global transaction map
5. **Resource Management**: Automatic cleanup of expired/abandoned transactions

**All centralized transaction architecture logic goes in dedicated KVT manager files**

### Data Model
**File**: `src/kvt_manager.cpp` (NEW FILE)
1. Tables as logical containers for key-value pairs
2. Keys scoped to tables within transactions
3. Support for different partition methods
4. Efficient scanning with cursor-based pagination

**All data model logic goes in dedicated KVT manager files**

### Error Handling
**File**: `src/kvt_manager.cpp` (NEW FILE)
1. Redis-compatible error responses
2. Transaction-specific error codes
3. Graceful degradation for invalid operations
4. Clear error messages for debugging

**All error handling logic goes in dedicated KVT manager files**

## Security Considerations

### Access Control
1. Integrate with Redis ACL system
2. Validate table access permissions
3. Transaction isolation levels
4. Resource usage limits

### Input Validation
1. Validate table names and key formats
2. Sanitize partition method parameters
3. Prevent injection attacks
4. Resource exhaustion protection

## Performance Optimizations

### Transaction Batching
1. Support for batch operations
2. Efficient cursor management
3. Optimized scanning algorithms
4. Connection pooling for high throughput

### Memory Management
1. Efficient transaction context storage
2. Cursor memory cleanup
3. Connection-level resource management
4. Garbage collection for abandoned transactions

## Testing Strategy

### Unit Tests
1. Individual subcommand functionality
2. Error handling scenarios
3. Transaction lifecycle management
4. Input validation

### Integration Tests
1. End-to-end transaction flows
2. Concurrent transaction handling
3. Table creation and management
4. Scanning and cursor operations

### Performance Tests
1. Transaction throughput
2. Memory usage patterns
3. Scanning performance
4. Concurrent access patterns

## Deployment Considerations

### Configuration
1. Transaction timeout settings
2. Memory limits for transactions
3. Table creation policies
4. Scanning batch sizes

### Monitoring
1. Transaction metrics
2. Error rate tracking
3. Performance monitoring
4. Resource usage alerts

### Migration
1. Backward compatibility
2. Data migration tools
3. Rollback procedures
4. Version compatibility matrix

## Timeline Estimate

- **Phase 1**: 1-2 weeks (Command infrastructure)
- **Phase 2**: 2-3 weeks (Core implementation)
- **Phase 3**: 2-3 weeks (Transaction integration)
- **Phase 4**: 1-2 weeks (Error handling)
- **Phase 5**: 2-3 weeks (Testing and validation)

**Total Estimated Time**: 8-13 weeks

## Risk Assessment

### Technical Risks
1. Transaction service integration complexity
2. Performance impact on existing operations
3. Memory management challenges
4. Concurrency control issues

### Mitigation Strategies
1. Incremental implementation approach
2. Comprehensive testing at each phase
3. Performance benchmarking
4. Code review and peer validation

## Success Criteria

1. All KVT subcommands function correctly
2. Transaction consistency maintained
3. Performance meets requirements
4. Error handling is robust
5. Integration with existing systems is seamless
6. Comprehensive test coverage achieved
7. Documentation is complete and accurate

## File Modification Summary

### **Existing Files - MINIMAL CHANGES ONLY**
These files will be modified with minimal additions for command routing:

1. **`include/redis_command.h`**
   - Add `KVT` to `RedisCommandType` enum
   - **No implementation logic added**

2. **`include/redis/commands.def`**
   - Add KVT command entry to `redisCommandTable[]`
   - **No implementation logic added**

3. **`src/redis_command.cpp`**
   - Add KVT case to command routing switch
   - Add minimal `kvtCommand` stub function
   - **No implementation logic added**

### **New Files - ALL IMPLEMENTATION LOGIC**
All KVT functionality will be implemented in these dedicated files:

1. **`src/kvt_handler.h`** - Complete KVT handler interface
2. **`src/kvt_handler.cpp`** - Complete KVT implementation
3. **`src/kvt_transaction.h`** - Transaction context declarations
4. **`src/kvt_transaction.cpp`** - Transaction context implementation
5. **`src/kvt_transaction_manager.h`** - Centralized transaction manager
6. **`src/kvt_transaction_manager.cpp`** - Centralized transaction manager implementation

### **Key Principle**
- **Existing files**: Only command parsing and routing stubs
- **New files**: All business logic, transaction management, and implementation details
- **Clean separation**: Maintain existing Redis architecture while adding KVT functionality

## Next Steps

1. Review and approve implementation plan
2. Set up development environment
3. Begin Phase 1 implementation (minimal file modifications)
4. Establish testing framework
5. Create development milestones
6. Assign development resources
7. Begin implementation execution
