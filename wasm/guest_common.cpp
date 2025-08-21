#include "guest_common.h"

int32_t guest_alloc(uint32_t size) { 
    void *p = malloc(size);
    return (int32_t)(uintptr_t)p; // cast void* to uintptr_t first, then to int32_t
}

void guest_dealloc(uint32_t ptr) { 
    free((void*)(uintptr_t)ptr); // cast uint32_t to uintptr_t first, then to void*
}
