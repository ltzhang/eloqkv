#ifndef GUEST_COMMON_H
#define GUEST_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

// Result struct for returning data from guest functions
struct Result {
    int32_t status;  // 0 for success, -1 for error
    int32_t ptr;     // Pointer to output data in WASM memory
    int32_t len;     // Length of output data
};

extern "C" {
/**
 * @brief Run the guest code
 * @param input Input string
 * @param input_len Length of input string
 * @return Pointer to Result struct in WASM memory, or -1 if error
 */
int32_t run(char * input, int input_len); 

/**
 * @brief Start the guest code
 * @param input Input string
 * @param input_len Length of input string
 * @return 0 if success, -1 if error
 */
int32_t start(char * input, int input_len); 

/**
 * @brief Allocate memory in WASM instance
 * @param size Size of memory to allocate in bytes
 * @return Pointer to allocated memory (32-bit offset in WASM memory)
 */
int32_t guest_alloc(uint32_t size);

/**
 * @brief Deallocate memory in WASM instance
 * @param ptr Pointer to memory to deallocate (32-bit offset in WASM memory)
 */
void guest_dealloc(uint32_t ptr); 
}

#endif // GUEST_COMMON_H
