#include "wasm_host.h"
#include <iostream>
#include <fstream>
#include <cstring>
#include <stdexcept>

struct Result {
    int32_t status;  // 0 for success, -1 for error
    int32_t ptr;     // Pointer to output data in WASM memory
    int32_t len;     // Length of output data
};

// WasmInstance method implementations
enum WasmError WasmInstance::setup(std::string & input) {
    try {
        //1. Get memory from instance
        try {
            std::optional<wasmtime::Extern> memory_export = instance_.get(store_context_, "memory");
            if (!memory_export) {
                throw std::runtime_error("Failed to get memory export");
            }
            
            wasmtime::Memory* memory_ptr = std::get_if<wasmtime::Memory>(&(*memory_export));
            if (!memory_ptr) {
                throw std::runtime_error("Export is not a memory");
            }
            
            memory_ = *memory_ptr;
           
        } catch (const std::exception& e) {
            std::cerr << "Failed to setup memory" << e.what() << std::endl;
            return WASM_MEMORY_SETUP_FAILED;
        }

        //2. Setup guest functions
        std::optional<wasmtime::Func> start_func;

        try {
            // Import guest_alloc function
            guest_alloc_func_ = import_function("guest_alloc");
            // Import guest_dealloc function
            guest_dealloc_func_ = import_function("guest_dealloc");
            // Import run function
            guest_run_func_ = import_function("run");
            std::cerr << "DEBUG: Imported run function, has_value: " << guest_run_func_.has_value() << std::endl;
            // Import start function
            start_func = import_function("start");
            
        } catch (const std::exception& e) {
            std::cerr << "Failed to setup guest functions: " << e.what() << std::endl;
            guest_alloc_func_.reset();
            guest_dealloc_func_.reset();
            guest_run_func_.reset();
            return WASM_GUEST_FUNCTION_SETUP_FAILED;
        }

        //3. call start function with the parameter
        try {
            // Allocate memory for input in guest
            uint32_t input_ptr = string_to_guest(input);
            if (input_ptr == 0) {
                return WASM_MEMORY_ALLOCATION_FAILED;
            }
            
            // Call the start function with input pointer and length
            std::vector<wasmtime::Val> params;
            params.push_back(wasmtime::Val((int32_t)input_ptr));
            params.push_back(wasmtime::Val((int32_t)input.length()));
            
            wasmtime::TrapResult<std::vector<wasmtime::Val>> call_result = 
                start_func->call(store_context_, params);
            
            // Clean up input memory
            guest_dealloc(input_ptr + get_memory_base());
            if (!call_result) {
                return WASM_START_FUNCTION_FAILED;
            }
        } catch (const std::exception& e) {
            std::cerr << "Exception calling start function: " << e.what() << std::endl;
            return WASM_START_FUNCTION_FAILED;
        }
        return WASM_SUCCESS;
    } catch (...) {
        throw;
    }
}

wasmtime::Func WasmInstance::import_function(const std::string& name) {
    std::optional<wasmtime::Extern> export_opt = instance_.get(store_context_, name);
    if (!export_opt) {
        throw std::runtime_error("Failed to get function: " + name);
    }
    wasmtime::Func* func_ptr = std::get_if<wasmtime::Func>(&(*export_opt));
    if (!func_ptr) {
        throw std::runtime_error("Export is not a function: " + name);
    }
    return *func_ptr;
}

enum WasmError WasmInstance::call_function(const std::string& input, std::string& output) {
    try {
        // Allocate memory for input
        int32_t input_ptr = string_to_guest(input);
        if (input_ptr < 0) {
            return WASM_INVALID_INPUT_POINTER;
        }
        // Call the function with input pointer and length
        std::vector<wasmtime::Val> params;
        params.push_back(wasmtime::Val((int32_t)input_ptr));
        params.push_back(wasmtime::Val((int32_t)input.length()));
        
        wasmtime::TrapResult<std::vector<wasmtime::Val>> call_result = 
            guest_run_func_->call(store_context_, params);
        
        char * memory_base = get_memory_base();
        if (!call_result) {
            std::cerr << "DEBUG: Function call failed" << std::endl;
            guest_dealloc(input_ptr + memory_base);
            return WASM_FUNCTION_CALL_FAILED;
        }
        
        // The run function returns a Result struct pointer
        if (call_result.ok().size() < 1) {
            std::cerr << "DEBUG: Expected 1 return value, got " << call_result.ok().size() << std::endl;
            guest_dealloc(input_ptr + memory_base);
            return WASM_INVALID_RETURN_VALUE_COUNT;
        }
        
        // Get the Result struct pointer from the return value
        wasmtime::Val result_ptr_val = call_result.ok()[0];
        
        if (result_ptr_val.kind() != wasmtime::ValKind::I32) {
            guest_dealloc(input_ptr + memory_base);
            return WASM_INVALID_RETURN_VALUE_TYPE;
        }
        
        int32_t result_ptr = result_ptr_val.i32();
        
        // Check if the call was successful (negative value indicates error)
        if (result_ptr < 0) {
            guest_dealloc(input_ptr + memory_base);
            return WASM_GUEST_ERROR; // Return the error status from guest
        }
        
        // Read the Result struct from WASM memory
        if (result_ptr > 0) {
            Result* result_struct = reinterpret_cast<Result*>(memory_base + result_ptr);
            
            // Check if the result struct indicates success
            if (result_struct->status != 0) {
                guest_dealloc(input_ptr + memory_base);
                return WASM_GUEST_ERROR; // Return the error status from guest
            }
            
            // Read output from the result struct
            if (result_struct->ptr > 0 && result_struct->len > 0) {
                output = std::string(memory_base + result_struct->ptr, result_struct->len);
            } else {
                output = ""; // No output data
            }
        } else {
            output = ""; // No result struct
        }
        
        // Clean up input memory
        guest_dealloc(input_ptr + memory_base);
        
        return WASM_SUCCESS;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception in call_function: " << e.what() << std::endl;
        return WASM_GUEST_ERROR;
    }
}

char* WasmInstance::guest_alloc(size_t size) {
    assert(size < MEMORY_ALLOC_LIMIT);
    
    try {
        std::vector<wasmtime::Val> params;
        params.push_back(wasmtime::Val((int32_t)size));
        
        wasmtime::TrapResult<std::vector<wasmtime::Val>> result = 
            guest_alloc_func_->call(store_context_, params);
        
        if (!result) {
            return nullptr;
        }
        
        wasmtime::Val return_val = result.ok()[0];
        if (return_val.kind() == wasmtime::ValKind::I32) {
            uint32_t ptr_offset = (uint32_t)return_val.i32();
            if (ptr_offset == 0) {
                return nullptr;
            } else {
                return get_memory_base() + ptr_offset;
            }
        }
        
        return nullptr;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception in guest_alloc: " << e.what() << std::endl;
        return nullptr;
    }
}

void WasmInstance::guest_dealloc(char* ptr) {
    char * memory_base = get_memory_base(); 
    if (ptr < memory_base || ptr > memory_base + MEMORY_ALLOC_LIMIT) {
        std::cerr << "DEBUG: Invalid pointer for deallocation" << std::endl;
        return;
    }
    guest_dealloc((int32_t)(ptr - memory_base));
}

void WasmInstance::guest_dealloc(int32_t ptr_offset) {
    try {
        std::vector<wasmtime::Val> params;
        params.push_back(wasmtime::Val((int32_t)ptr_offset));
        
        auto result = guest_dealloc_func_->call(store_context_, params);
        if (!result) {
            std::cerr << "Failed to call guest_dealloc: " << result.err().message() << std::endl;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Exception in guest_dealloc: " << e.what() << std::endl;
    }
}

int32_t WasmInstance::string_to_guest(const std::string& data) {
    try {
        // Use guest_alloc function to allocate memory
        char* ptr = guest_alloc(data.length() + 1); // +1 for null terminator
        if (!ptr) {
            return WASM_MEMORY_ALLOCATION_FAILED;
        }
        
        // Copy data directly to the allocated pointer
        std::memcpy(ptr, data.data(), data.length());
        ptr[data.length()] = '\0'; // null terminator
        
        // Return the offset from base for consistency with the rest of the API
        uint32_t offset = (uint32_t)(ptr - get_memory_base());
        return offset;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception allocating memory: " << e.what() << std::endl;
        return WASM_MEMORY_ALLOCATION_FAILED;
    }
}

// WasmHost method implementations
bool WasmHost::load_module_internal(const std::string& module_file_name) {
    try {
        // Read WASM file
        std::vector<uint8_t> wasm_bytes;
        std::ifstream file(module_file_name, std::ios::binary | std::ios::ate);
        if (!file) {
            log_error("Failed to open file: " + module_file_name);
            return false;
        }
        
        std::streamsize size = file.tellg();
        file.seekg(0, std::ios::beg);
        wasm_bytes.resize(size);
        if (!file.read(reinterpret_cast<char*>(wasm_bytes.data()), size)) {
            log_error("Failed to read file: " + module_file_name);
            return false;
        }

        // Compile module
        wasmtime::Result<wasmtime::Module> module_result = wasmtime::Module::compile(engine_, wasm_bytes);
        if (!module_result) {
            log_error("Failed to compile module: " + module_result.err().message());
            return false;
        }
        
        wasmtime::Module module = module_result.ok();
        
        // Create linker for this module
        wasmtime::Linker linker(engine_);
        
        // Store module context (host functions will be set up per instance)
        modules_[module_file_name] = std::make_unique<ModuleContext>(
            module_file_name, std::move(module), std::move(linker));
        
        return true;
        
    } catch (const std::exception& e) {
        log_error("Exception loading module: " + std::string(e.what()));
        return false;
    }
}

WasmInstanceHandle WasmHost::create_instance(const std::string& module_file_name, std::string & input) {
    // Check if module is loaded, load it if not
    if (modules_.find(module_file_name) == modules_.end()) {
        if (!load_module_internal(module_file_name)) {
            log_error("Failed to load module: " + module_file_name);
            return WasmInstanceHandle::INVALID();
        }
    }
    
    // Check if we have free slots
    if (free_list_.empty()) {
        log_error("No free instance slots available");
        return WasmInstanceHandle::INVALID();
    }
    
    try {
        ModuleContext& module_ctx = *modules_[module_file_name];
        
        // Create store for this instance
        wasmtime::Store store(engine_);
        
        // Setup host functions in this store
        if (!setup_host_functions(module_ctx.linker, store)) {
            log_error("Failed to setup host functions for instance");
            return WasmInstanceHandle::INVALID();
        }
        
        // Enable WASI support
        wasmtime::Result<std::monostate> wasi_result = module_ctx.linker.define_wasi();
        if (!wasi_result) {
            log_error("Failed to enable WASI support");
            return WasmInstanceHandle::INVALID();
        }
        
        // Configure WASI context
        wasmtime::WasiConfig wasi_config;
        wasi_config.inherit_stdin();
        wasi_config.inherit_stdout();
        wasi_config.inherit_stderr();
        
        wasmtime::Result<std::monostate> context_result = store.context().set_wasi(std::move(wasi_config));
        if (!context_result) {
            log_error("Failed to set WASI context");
            return WasmInstanceHandle::INVALID();
        }
        
        // Instantiate module
        wasmtime::TrapResult<wasmtime::Instance> instance_result = 
            module_ctx.linker.instantiate(store.context(), module_ctx.module);
        if (!instance_result) {
            log_error("Failed to instantiate module: " + instance_result.err().message());
            return WasmInstanceHandle::INVALID();
        }
        
        wasmtime::Instance instance = instance_result.ok();
        
        // Get free slot index
        uint32_t index = free_list_.back();
        free_list_.pop_back();
        
        // Generate magic number
        uint32_t magic = next_magic_++;
        
        // Create instance context
        instances_[index] = std::make_unique<WasmInstance>(
            index, magic, module_file_name, std::move(instance), std::move(store));
        bool ok = instances_[index]->setup(input);
        if (!ok) {
            log_error("Failed to setup instance");
            return WasmInstanceHandle::INVALID();
        }
        return WasmInstanceHandle::make(index, magic);
    } catch (const std::exception& e) {
        log_error("Exception creating instance: " + std::string(e.what()));
        return WasmInstanceHandle::INVALID();
    }
}

bool WasmHost::is_instance_valid(WasmInstanceHandle handle) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    uint32_t index = handle.get_index();
    uint32_t magic = handle.get_magic();
    
    if (index >= instances_.size() || !instances_[index]) {
        return false;
    }
    
    return instances_[index]->get_handle().get_magic() == magic;
}

bool WasmHost::delete_instance(WasmInstanceHandle handle) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    uint32_t index = handle.get_index();
    uint32_t magic = handle.get_magic();
    
    if (index >= instances_.size() || !instances_[index] || 
        instances_[index]->get_handle().get_magic() != magic) {
        log_error("Invalid instance handle for deletion");
        return false;
    }
    
    if (instances_[index]->assigned_thread_id_ != std::thread::id(0)) {
        log_warning("Instance is assigned to a thread, cannot delete");
        return false;
    }

    // Clear the instance
    instances_[index].reset();
    
    // Add index back to free list
    free_list_.push_back(index);
    
    return true;
}

WasmInstanceContext WasmHost::get_instance_context(WasmInstanceHandle handle) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    uint32_t index = handle.get_index();
    uint32_t magic = handle.get_magic();
    
    if (index >= instances_.size() || !instances_[index] || 
        instances_[index]->get_handle().get_magic() != magic) {
        log_error("Invalid instance handle");
        return WasmInstanceContext(nullptr, this);
    }
    
    WasmInstance* instance = instances_[index].get();
    
    // Check if instance is already assigned, if so, return invalid context
    if (instance->assigned_thread_id_ != std::thread::id(0)) {
        log_error("Instance is already assigned to a thread");
        return WasmInstanceContext(nullptr, this); // Instance is assigned to another thread
    }
    else {
        instance->assigned_thread_id_ = std::this_thread::get_id();
        return WasmInstanceContext(instance, this);
    }
}

bool WasmHost::release_instance_context(WasmInstanceContext& ctx) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!ctx.is_valid()) {
        log_error("Invalid instance context");
        return false;
    }
    ctx.get_instance()->assigned_thread_id_ = std::thread::id(0);
    return true;
}

bool WasmHost::setup_host_functions(wasmtime::Linker& linker, wasmtime::Store& store) {
    try {
        // Define the f1 function that the guest will call
        export_function(linker, store, "f1", [](wasmtime::Caller caller, int32_t struct_ptr) {
            (void)caller; // Suppress unused parameter warning
            std::cout << "Host f1 called with struct pointer: " << struct_ptr << std::endl;
            
            // For now, just print that we received the call
            // TODO: Implement full struct processing when needed
            std::cout << "f1 function called successfully from guest" << std::endl;
        });
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to setup host functions: " << e.what() << std::endl;
        return false;
    }
}

