# Compiler and flags
EMCC = emcc
EMCC_FLAGS = -s EXPORTED_FUNCTIONS='["_start", "_run", "_guest_alloc", "_guest_dealloc"]' \
             -s STANDALONE_WASM=1 \
             -s PURE_WASI=1 \
             -s ERROR_ON_UNDEFINED_SYMBOLS=0 \
             -Wl,--no-entry

# Source files
GUEST_SOURCES = guest_echo.cpp guest_common.cpp

# Object files
WASM_MODULE = guest_echo.wasm

# Default target
all: $(WASM_MODULE)

# Build WASM module
$(WASM_MODULE): $(GUEST_SOURCES)
	$(EMCC) $(EMCC_FLAGS) guest_echo.cpp guest_common.cpp -o $@

# Clean build artifacts
clean:
	rm -f $(WASM_MODULE)

# Phony targets
.PHONY: all clean 
