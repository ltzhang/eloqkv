#pragma once
#include <wasmtime.hh>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <optional>
#include <cstdint>
#include <atomic>
#include <mutex>
#include <thread>

// Forward declaration
class WasmInstanceContext;

// Error codes for WASM operations
enum WasmError {
    WASM_SUCCESS = 0,
    WASM_MEMORY_ALLOCATION_FAILED = -1,
    WASM_INVALID_INPUT_POINTER = -2,
    WASM_FUNCTION_CALL_FAILED = -3,
    WASM_INVALID_RETURN_VALUE_COUNT = -4,
    WASM_INVALID_RETURN_VALUE_TYPE = -5,
    WASM_GUEST_ERROR = -6,  // Error returned from guest code
    WASM_INSTANCE_LOCKED = -10,  // Instance is locked by another thread
    WASM_SETUP_FAILED = -20,  // General setup failure
    WASM_MEMORY_SETUP_FAILED = -21,  // Memory setup specific failure
    WASM_GUEST_FUNCTION_SETUP_FAILED = -22,  // Guest function setup failure
    WASM_START_FUNCTION_FAILED = -23,  // Start function execution failure
    WASM_INSTANCE_ALREADY_ASSIGNED = -30,  // Instance already assigned to another thread
    WASM_INSTANCE_NOT_ASSIGNED = -31,  // Instance not assigned to current thread
    WASM_THREAD_ASSIGNMENT_FAILED = -32  // Failed to assign instance to thread
};

// Helper function to convert error enum to string
inline const char* wasm_error_to_string(enum WasmError error) {
    switch (error) {
        case WASM_SUCCESS: return "SUCCESS";
        case WASM_MEMORY_ALLOCATION_FAILED: return "MEMORY_ALLOCATION_FAILED";
        case WASM_INVALID_INPUT_POINTER: return "INVALID_INPUT_POINTER";
        case WASM_FUNCTION_CALL_FAILED: return "FUNCTION_CALL_FAILED";
        case WASM_INVALID_RETURN_VALUE_COUNT: return "INVALID_RETURN_VALUE_COUNT";
        case WASM_INVALID_RETURN_VALUE_TYPE: return "INVALID_RETURN_VALUE_TYPE";
        case WASM_GUEST_ERROR: return "GUEST_ERROR";
        case WASM_INSTANCE_LOCKED: return "INSTANCE_LOCKED";
        case WASM_SETUP_FAILED: return "SETUP_FAILED";
        case WASM_MEMORY_SETUP_FAILED: return "MEMORY_SETUP_FAILED";
        case WASM_GUEST_FUNCTION_SETUP_FAILED: return "GUEST_FUNCTION_SETUP_FAILED";
        case WASM_START_FUNCTION_FAILED: return "START_FUNCTION_FAILED";
        case WASM_INSTANCE_ALREADY_ASSIGNED: return "INSTANCE_ALREADY_ASSIGNED";
        case WASM_INSTANCE_NOT_ASSIGNED: return "INSTANCE_NOT_ASSIGNED";
        case WASM_THREAD_ASSIGNMENT_FAILED: return "THREAD_ASSIGNMENT_FAILED";
        default: return "UNKNOWN_ERROR";
    }
}

class WasmInstanceHandle 
{
public:
    uint32_t index_ = 0;
    uint32_t magic_ = 0;

    static WasmInstanceHandle INVALID() {
        return WasmInstanceHandle{0, 0};
    }

    static WasmInstanceHandle make(uint32_t index, uint32_t magic) {
        WasmInstanceHandle handle;
        handle.index_ = index;
        handle.magic_ = magic;
        return handle;
    }

    uint32_t get_index() const {
        return index_;
    }

    uint32_t get_magic() const {
        return magic_;
    }

    bool operator==(const WasmInstanceHandle& other) const {
        return index_ == other.index_ && magic_ == other.magic_;
    }

    bool operator!=(const WasmInstanceHandle& other) const {
        return !(*this == other);
    }
};

class WasmInstance {
    private:
        static constexpr size_t MEMORY_ALLOC_LIMIT = 1024 * 1024;

    public:
        WasmInstance(uint32_t idx, uint32_t mag, const std::string& mod_name, 
                    wasmtime::Instance inst, wasmtime::Store s)
            : handle_(WasmInstanceHandle::make(idx, mag)), module_name_(mod_name), instance_(std::move(inst)), 
            store_(std::move(s)), store_context_(store_.context()){
        }

        WasmInstanceHandle get_handle() const {
            return handle_;
        }

        // Instance-specific methods moved from WasmHost
        enum WasmError setup(std::string & input);
        enum WasmError call_function(const std::string& input, std::string& output);
        char* guest_alloc(size_t size);
        void guest_dealloc(char* ptr);
        void guest_dealloc(int32_t ptr_offset);

        int32_t string_to_guest(const std::string& data);

        char * host_ptr(int32_t guest_ptr_offset) {
            return get_memory_base() + guest_ptr_offset;
        }

        int32_t guest_ptr_offset(char * ptr) {
            char * memory_base = get_memory_base();
            if (ptr < memory_base || ptr > memory_base + MEMORY_ALLOC_LIMIT) {
                std::cerr << "DEBUG: Invalid pointer for deallocation" << std::endl;
                return -1;
            }
            return (int32_t)(ptr - memory_base);
        }
        
        // Getters for WasmHost to access
        // wasmtime::Instance& get_instance() { return instance_; }
        // wasmtime::Store& get_store() { return store_; }
        //const std::string& get_module_name() const { return module_name_; }
        // wasmtime::Store::Context& get_store_context() { return store_context_; }
        
    private:
        WasmInstanceHandle handle_;
        std::string module_name_;
        wasmtime::Instance instance_;
        wasmtime::Store store_;
        std::optional<wasmtime::Memory> memory_;
        wasmtime::Store::Context store_context_;

        std::thread::id assigned_thread_id_ = std::thread::id(0);
        friend class WasmHost;
        
        // Guest function pointers
        std::optional<wasmtime::Func> guest_alloc_func_;
        std::optional<wasmtime::Func> guest_dealloc_func_;
        std::optional<wasmtime::Func> guest_run_func_;
        // int32_t input_ptr_reserved_ = -1;
        // int32_t input_ptr_ = -1;
        // static constexpr int32_t INPUT_MAX_LEN = 16 * 1024;
        // int32_t set_input(const std::string& input);
        // Private helper methods
        wasmtime::Func import_function(const std::string& name);
        char* get_memory_base() {
            wasmtime::Span<uint8_t> mem_data = memory_->data(store_context_);
            return reinterpret_cast<char*>(mem_data.data());
        }
};


/**
 * @brief Encapsulates the wasmtime runtime and manages multiple WASM instances
 * 
 * This class provides a high-level interface for:
 * - Loading WASM modules
 * - Creating multiple instances of modules
 * - Managing instance lifecycle
 */
class WasmHost {
public:
    WasmHost() : next_magic_(1) {
        // Engine is default-constructed
        // Initialize instances array and free list
        instances_.resize(MAX_INSTANCES);
        for (size_t i = 0; i < MAX_INSTANCES; ++i) {
            free_list_.push_back(i);
        }
    }

    ~WasmHost() = default; 

    // Disable copy constructor and assignment operator
    WasmHost(const WasmHost&) = delete;
    WasmHost& operator=(const WasmHost&) = delete;

    /**
     * @brief Create a new instance of a module (automatically loads if not cached)
     * @param module_file_name Name of the module to instantiate
     * @return Context handle on success, INVALID on failure
     */
    WasmInstanceHandle create_instance(const std::string& module_file_name, std::string & input_param);

    /**
     * @brief Delete an instance and free its resources
     * @param handle Context handle returned by create_instance
     * @return true if instance was deleted successfully, false otherwise
     */
    bool delete_instance(WasmInstanceHandle handle);

    /**
     * @brief Check if an instance context is valid
     * @param handle Context handle
     * @return true if context is valid, false otherwise
     */
    bool is_instance_valid(WasmInstanceHandle handle) const;

    /**
     * @brief Get an instance context by handle and assign it to the current thread
     * @param handle Context handle
     * @return WasmInstanceContext object that manages the instance lifecycle, or invalid context if failed
     */
    WasmInstanceContext get_instance_context(WasmInstanceHandle handle);

    /**
     * @brief Release an instance from the current thread
     * @param handle Context handle
     * @return true if instance was released successfully, false otherwise
     */
    bool release_instance_context(WasmInstanceContext& ctx);

private:
    // Internal structures
    struct ModuleContext {
        std::string file_name;
        wasmtime::Module module;
        wasmtime::Linker linker;
        
        ModuleContext(const std::string& name, wasmtime::Module mod, wasmtime::Linker link)
            : file_name(name), module(std::move(mod)), linker(std::move(link)) {}
    };

    // Core wasmtime components
    wasmtime::Engine engine_;
    
    // Module management
    std::unordered_map<std::string, std::unique_ptr<ModuleContext>> modules_;
    
    // Instance management using arrays instead of maps
    static constexpr size_t MAX_INSTANCES = 1024;
    std::vector<std::unique_ptr<WasmInstance>> instances_;
    std::vector<uint32_t> free_list_;
    
    // Magic number generator
    uint32_t next_magic_;

    mutable std::mutex mutex_;
    
    // Helper methods
    bool load_module_internal(const std::string& module_file_name);
    bool setup_host_functions(wasmtime::Linker& linker, wasmtime::Store& store);
    
    template<typename F>
    void export_function(wasmtime::Linker& linker, wasmtime::Store& store, const std::string& name, F func) {
        wasmtime::Func func_wrap = wasmtime::Func::wrap(store.context(), func);
        wasmtime::Result<std::monostate> result = linker.define(store.context(), "env", name, func_wrap);   
        if (!result) {
            throw std::runtime_error("Failed to define function: " + name);
        }
    }
    
    // Error handling
    void log_error(const std::string& message) const {
        std::cerr << "[WasmHost] Error: " << message << std::endl;
    }
    void log_warning(const std::string& message) const {
        std::cerr << "[WasmHost] Warning: " << message << std::endl;
    }
};

/**
 * @brief RAII wrapper for WasmInstance that ensures proper thread assignment and release
 *
 * This class provides safe access to WasmInstance objects with automatic cleanup.
 * It ensures that instances are properly released when the context goes out of scope.
 */
class WasmInstanceContext
{
    public:
        //disable copy
        WasmInstanceContext(const WasmInstanceContext&) = delete;
        WasmInstanceContext& operator=(const WasmInstanceContext&) = delete;

        //enable move
        WasmInstanceContext(WasmInstanceContext&& other) noexcept : instance_(other.instance_), host_(other.host_) {
            other.instance_ = nullptr;
        }

        WasmInstanceContext& operator=(WasmInstanceContext&& other) noexcept {
            if (this != &other) {
                release();
                instance_ = other.instance_;
                host_ = other.host_;
                other.instance_ = nullptr;
            }
            return *this;
        }

        WasmInstanceContext(WasmInstance* instance, WasmHost* host) : instance_(instance), host_(host) {}

        ~WasmInstanceContext() { release(); }

        WasmInstance* get_instance() const { return instance_; }

        WasmInstance* operator->() const { return get_instance(); }

        bool is_valid() const { return instance_ != nullptr; }

        void release() {
            if (instance_ != nullptr) {
                host_->release_instance_context(*this);
                instance_ = nullptr;
            }
        }

    private:
        WasmInstance* instance_;
        WasmHost* host_;
};

