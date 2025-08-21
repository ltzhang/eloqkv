#include "guest_common.h"
#include <string.h>
#include <stdio.h>

// Import host function for testing host-guest communication
__attribute__((import_module("env"), import_name("f1"))) void f1(int32_t value);

// Static Result struct for returning data
static Result result_ = {0, 0, 0};

// Main run function - echo functionality that returns Result struct pointer
// return -1 if error, or pointer to Result struct in WASM memory
int32_t run(char* input, int input_len) {
    printf("=== RUN FUNCTION: Input length = %d ===\n", input_len);
    
    // Clear previous result
    if (result_.ptr != 0) {
        free((void*)(uintptr_t)result_.ptr);
        result_.ptr = 0;
        result_.len = 0;
    }
    
    if (input_len == 0) {
        // Empty input test
        printf("Empty input received\n");
        result_.status = 0;
        result_.ptr = 0;
        result_.len = 0;
    } else {
        // Echo input back
        printf("Input: '%.*s'\n", input_len, input);
        
        // Allocate memory for output
        char* output = (char*)malloc(input_len + 1);
        if (output) {
            memcpy(output, input, input_len);
            output[input_len] = '\0';
            result_.status = 0;
            result_.ptr = (int32_t)(uintptr_t)output;
            result_.len = input_len;
            printf("Output allocated: %s\n", output);
        } else {
            result_.status = -1;
            result_.ptr = 0;
            result_.len = 0;
            printf("Memory allocation failed\n");
        }
    }
    
    printf("Run function completed with status: %d\n", result_.status);
    return (int32_t)(uintptr_t)&result_;
}

// Start function - basic initialization, return -1 if error, or 0 if success
int32_t start(char* input, int input_len) {
    printf("=== START FUNCTION: Input length = %d ===\n", input_len);
    
    if (input_len > 0) {
        printf("Initialization input: '%.*s'\n", input_len, input);
    } else {
        printf("No initialization input\n");
    }
    
    // Test host function call
    printf("Calling host function f1 with value 42...\n");
    f1(42);
    printf("Host function call completed\n");
    
    // Test basic memory allocation
    printf("Testing basic memory allocation...\n");
    char* test_mem = (char*)malloc(64);
    if (test_mem) {
        sprintf(test_mem, "Memory test successful, input was '%.*s'", input_len, input);
        printf("Memory allocation: %s\n", test_mem);
        free(test_mem);
    } else {
        printf("Memory allocation failed\n");
        return -1;
    }
    
    printf("Start function completed successfully\n");
    return 0;
}
