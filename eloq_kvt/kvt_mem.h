#ifndef KVT_MEM_H
#define KVT_MEM_H
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <string>
#include <vector>
#include <memory>
#include <cstdint>
#include <cassert>
#include <cstring>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include "kvt_inc.h"

extern int g_verbosity;
extern int g_sanity_check_level;

#if 1
    #define VERBOSE(x) {if (g_verbosity > 0) {x;}}
    #define VERBOSE1(x) {if (g_verbosity > 1) {x;}}
    #define VERBOSE2(x) {if (g_verbosity > 2) {x;}}
#else 
    #define VERBOSE(x) {}
    #define VERBOSE1(x) {}
    #define VERBOSE2(x) {}
#endif

#if 1
    #define CHECK(x) {if (g_sanity_check_level > 0) {x;}}
    #define CHECK1(x) {if (g_sanity_check_level > 1) {x;}}
    #define CHECK2(x) {if (g_sanity_check_level > 2) {x;}}
#else 
    #define CHECK(x) {}
    #define CHECK1(x) {}
    #define CHECK2(x) {}
#endif

extern int g_verbosity;
extern int g_sanity_check_level;

class KVTLogger 
{
    private:
        bool log_as_text_;
        bool write_to_file_;  // Whether to actually write to file
        bool do_fsync_;       // Whether to fsync after each write
        std::ofstream ofs_;
        uint64_t next_log_id_;
        std::stringstream current_entry_buffer_;
        size_t total_size_;

        // Simple checksum calculation
        uint32_t calculate_checksum(const char* data, size_t length) {
            uint32_t checksum = 0;
            for (size_t i = 0; i < length; ++i) {
                checksum = checksum * 31 + static_cast<unsigned char>(data[i]);
            }
            return checksum;
        }
        
        // Convert binary data to hex representation for text mode
        std::string to_hex_string(const char* data, size_t length) {
            std::stringstream ss;
            for (size_t i = 0; i < length; ++i) {
                unsigned char c = static_cast<unsigned char>(data[i]);
                if (c < 32 || c > 126) {
                    ss << "\\" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
                } else {
                    ss << static_cast<char>(c);
                }
            }
            return ss.str();
        }
        
        // Parse hex string back to binary data
        std::string from_hex_string(const std::string& hex_str) {
            std::string result;
            for (size_t i = 0; i < hex_str.length(); ) {
                if (hex_str[i] == '\\' && i + 2 < hex_str.length()) {
                    // Parse hex escape sequence
                    std::string hex_part = hex_str.substr(i + 1, 2);
                    result += static_cast<char>(std::stoi(hex_part, nullptr, 16));
                    i += 3;
                } else {
                    result += hex_str[i];
                    i++;
                }
            }
            return result;
        }

    public:
        KVTLogger(const std::string& file_name, bool text_mode = false, bool write_to_file = true, bool do_fsync = false, 
                  size_t log_size_limit = 0, size_t keep_history = 0) 
            : log_as_text_(text_mode), write_to_file_(write_to_file), do_fsync_(do_fsync), next_log_id_(1) {
            total_size_ = 0;
            // Note: log_size_limit and keep_history are handled by KVTWrapper, not KVTLogger
            if (write_to_file_) {
                ofs_.open(file_name, std::ios::out | std::ios::app | std::ios::binary);
                if (!ofs_.is_open()) {
                    throw std::runtime_error("Failed to open log file: " + file_name);
                }
            }
        }
        
        ~KVTLogger() {
            if (ofs_.is_open()) {
                ofs_.close();
            }
        }
        
        // Start a new log entry
        KVTLogger& start_entry() {
            current_entry_buffer_.str("");
            current_entry_buffer_.clear();
            return *this;
        }
        
        // Add data to current entry
        template<typename T>
        KVTLogger& operator<<(const T& data) {
            current_entry_buffer_ << data;
            return *this;
        }
        
        // Flush the current entry to file
        void flush_log() {
            std::string payload = current_entry_buffer_.str();
            uint64_t log_id = next_log_id_++;
            uint32_t payload_length = static_cast<uint32_t>(payload.length());
            uint32_t checksum = calculate_checksum(payload.c_str(), payload.length());
            
            if (write_to_file_) {
                if (log_as_text_) {
                    // Text format: ID LENGTH CHECKSUM PAYLOAD
                    ofs_ << log_id << " " << payload_length << " " << checksum << " ";
                    ofs_ << to_hex_string(payload.c_str(), payload.length());
                    ofs_ << "\n";
                } else {
                    // Binary format: ID(8) LENGTH(4) CHECKSUM(4) PAYLOAD(variable)
                    ofs_.write(reinterpret_cast<const char*>(&log_id), sizeof(log_id));
                    ofs_.write(reinterpret_cast<const char*>(&payload_length), sizeof(payload_length));
                    ofs_.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
                    ofs_.write(payload.c_str(), payload.length());
                }
                ofs_.flush();
                
                if (do_fsync_) {
                    // Force data to disk
                    // Note: In production, you'd want to use fsync on the file descriptor
                    // For now, just flush the stream which is better than nothing
                    ofs_.flush();
                }
            }
            total_size_ += payload.length();
            current_entry_buffer_.str("");
            current_entry_buffer_.clear();
        }
        
        // Static method to read entries from a log file
        static bool read_entry_from_file(const std::string& file_name, bool text_mode, 
                                       uint64_t& log_id, std::string& payload) {
            std::ifstream ifs(file_name, std::ios::in | std::ios::binary);
            if (!ifs.is_open()) {
                return false;
            }
            
            if (text_mode) {
                // Text format parsing
                std::string line;
                if (!std::getline(ifs, line)) {
                    return false;
                }
                
                std::istringstream iss(line);
                uint32_t payload_length, checksum;
                if (!(iss >> log_id >> payload_length >> checksum)) {
                    return false;
                }
                
                // Read the hex-encoded payload
                std::string hex_payload;
                std::getline(iss, hex_payload);
                // Remove leading space
                if (!hex_payload.empty() && hex_payload[0] == ' ') {
                    hex_payload = hex_payload.substr(1);
                }
                
                payload = from_hex_string_static(hex_payload);
                
                // Verify checksum
                uint32_t calculated_checksum = calculate_checksum_static(payload.c_str(), payload.length());
                if (calculated_checksum != checksum) {
                    return false; // Checksum mismatch
                }
                
                return true;
            } else {
                // Binary format parsing
                uint32_t payload_length, checksum;
                
                if (!ifs.read(reinterpret_cast<char*>(&log_id), sizeof(log_id))) {
                    return false;
                }
                if (!ifs.read(reinterpret_cast<char*>(&payload_length), sizeof(payload_length))) {
                    return false;
                }
                if (!ifs.read(reinterpret_cast<char*>(&checksum), sizeof(checksum))) {
                    return false;
                }
                
                payload.resize(payload_length);
                if (!ifs.read(payload.data(), payload_length)) {
                    return false;
                }
                
                // Verify checksum
                uint32_t calculated_checksum = calculate_checksum_static(payload.c_str(), payload.length());
                if (calculated_checksum != checksum) {
                    return false; // Checksum mismatch
                }
                
                return true;
            }
        }
        
        // Static helper methods
        static uint32_t calculate_checksum_static(const char* data, size_t length) {
            uint32_t checksum = 0;
            for (size_t i = 0; i < length; ++i) {
                checksum = checksum * 31 + static_cast<unsigned char>(data[i]);
            }
            return checksum;
        }
        
        static std::string from_hex_string_static(const std::string& hex_str) {
            std::string result;
            for (size_t i = 0; i < hex_str.length(); ) {
                if (hex_str[i] == '\\' && i + 2 < hex_str.length()) {
                    // Parse hex escape sequence
                    std::string hex_part = hex_str.substr(i + 1, 2);
                    result += static_cast<char>(std::stoi(hex_part, nullptr, 16));
                    i += 3;
                } else {
                    result += hex_str[i];
                    i++;
                }
            }
            return result;
        }

        size_t get_total_payload_size() const {
            return total_size_;
        }

        // Close the log file
        void close() {
            if (write_to_file_ && ofs_.is_open()) {
                ofs_.close();
            }
        }
};


class KVTWrapper
{
    private:
        size_t check_point_id_;  // Current checkpoint/log ID (checkpoint N uses log N-1, starts at 1)
        std::unique_ptr<KVTLogger> logger_;

        // Configurable checkpoint parameters
        bool persist_;                // Whether to persist to disk
        bool do_fsync_;               // Whether to fsync after writes
        size_t log_size_limit_;       // Maximum log size before checkpoint (in bytes)
        size_t keep_history_;         // Number of old checkpoints to keep
        bool text_log_;               // Whether to use text log format

        std::string data_path_;

        std::string check_point_name_base_ = "/kvt_checkpoint";
        std::string log_file_name_base_ = "/kvt_log";

        std::string get_checkpoint_name(size_t check_point_id) {
            return data_path_ + check_point_name_base_ + "_"  + std::to_string(check_point_id);
        }

        std::string get_logfile_name(size_t log_id) {
            return data_path_ + log_file_name_base_ + "_" + std::to_string(log_id);
        }

        size_t get_checkpoint_id_from_file_name(std::string file_name) {
            return std::stoull(file_name.substr(file_name.find_last_of('_') + 1));
        }

    protected:
        //helper
        KVTKey make_table_key(uint64_t table_id, const KVTKey& key) {
            KVTKey result;
            if (key.empty()) {
                // Special case: empty key (maximum key) is encoded as table_id + 1
                // This ensures empty keys are treated as largest in standard string comparison
                result.resize(8);
                uint64_t encoded_table_id = table_id + 1;
                for (int i = 0; i < 8; ++i) {
                    result[i] = static_cast<char>((encoded_table_id >> (i * 8)) & 0xFF);
                }
            } else {
                result.resize(8 + key.size());
                // Write table_id as 8 bytes (little-endian)
                for (int i = 0; i < 8; ++i) {
                    result[i] = static_cast<char>((table_id >> (i * 8)) & 0xFF);
                }
                // Copy key after the table_id
                std::copy(key.begin(), key.end(), result.begin() + 8);
            }
            return result;
        }
        
        std::pair<uint64_t, KVTKey> parse_table_key(const KVTKey& table_key) {
            if (table_key.size() < 8) {
                return std::make_pair(0, KVTKey());
            }
            // Read table_id from first 8 bytes (little-endian)
            uint64_t encoded_table_id = 0;
            for (int i = 0; i < 8; ++i) {
                encoded_table_id |= static_cast<uint64_t>(static_cast<unsigned char>(table_key[i])) << (i * 8);
            }
            
            if (table_key.size() == 8) {
                // Special case: 8-byte key indicates empty key (maximum key)
                // Decode by subtracting 1 from the encoded table_id
                uint64_t table_id = encoded_table_id - 1;
                return std::make_pair(table_id, KVTKey());
            } else {
                // Normal case: extract key (everything after the 8-byte table_id)
                KVTKey key(table_key.begin() + 8, table_key.end());
                return std::make_pair(encoded_table_id, key);
            }
        }



    public:
        KVTWrapper(std::string data_path = "./") : data_path_(data_path) {
            check_point_id_ = 1;
            
            // Default checkpoint parameters (suitable for production)
            persist_ = true;                 // Persist to disk by default
            do_fsync_ = false;               // Don't fsync by default (for performance)
            log_size_limit_ = 16 * 1024 * 1024;  // 10MB log size limit
            keep_history_ = 5;               // Keep 5 old checkpoints
            text_log_ = false;               // Use binary log format by default
            // Note: startup() must be called after the derived class is fully constructed
        }
        // Virtual destructor to ensure proper cleanup of derived classes
        virtual ~KVTWrapper() = default;
        
        // Methods to configure checkpoint parameters (must be called before startup)
        void set_persist_params(bool persist, bool do_fsync, size_t log_size_limit, size_t keep_history, bool text_log) { 
            persist_ = persist; 
            do_fsync_ = do_fsync;
            log_size_limit_ = log_size_limit;
            keep_history_ = keep_history;
            text_log_ = text_log;
        }

        void startup() {
            int64_t current_check_point_id = -1;
            std::filesystem::path check_point_dir(data_path_);
            
            // Create data directory if it doesn't exist
            if (!std::filesystem::exists(check_point_dir)) {
                std::filesystem::create_directories(check_point_dir);
            }
            
            // Look for checkpoint files
            std::cout << "Looking for checkpoints in: " << data_path_ << std::endl;
            for (const auto& entry : std::filesystem::directory_iterator(data_path_)) {
                std::string filename = entry.path().filename().string();
                if (filename.find("kvt_checkpoint_") != std::string::npos) {
                    std::cout << "Found checkpoint: " << filename << std::endl;
                    size_t id = get_checkpoint_id_from_file_name(entry.path().string());
                    std::cout << "Checkpoint ID: " << id << std::endl;
                    if(static_cast<int64_t>(id) > current_check_point_id) {
                        current_check_point_id = id;
                    }
                }
            }
            std::cout << "Final checkpoint ID selected: " << current_check_point_id << std::endl;
            if (current_check_point_id == -1) {
                std::cout << "No checkpoint found, Trying to Replay Log" << std::endl;
                check_point_id_ = 1;
            } else {
                std::cout << "KVT Store continue from checkpoint " << current_check_point_id << std::endl;
                if (!load_checkpoint(get_checkpoint_name(current_check_point_id))){
                    std::cout << "KVT Failed to load checkpoint " << current_check_point_id << std::endl;
                    exit(1);
                    return;
                }
            }
            int64_t current_log_id = -1;
            
            // Look for log files
            for (const auto& entry : std::filesystem::directory_iterator(data_path_)) {
                std::string filename = entry.path().filename().string();
                if (filename.find("kvt_log_") != std::string::npos) {
                    size_t id = get_checkpoint_id_from_file_name(entry.path().string());
                    if(static_cast<int64_t>(id) > current_log_id) {
                        current_log_id = id;
                    }
                }
            }
            // Validate log and checkpoint consistency
            if (current_log_id != -1 && current_check_point_id != -1 && current_log_id > current_check_point_id + 1) {
                std::cout << "Log file id is greater than checkpoint id + 1, Corrupted Data" << std::endl;
                exit(1);
                return;
            }
            
            // Determine the checkpoint to use
            if (current_check_point_id == -1) {
                // No checkpoint found
                check_point_id_ = 1;
                if (current_log_id != -1) {
                    // But we have a log file, replay it (log 0 for checkpoint 1)
                    if (std::filesystem::exists(get_logfile_name(0))) {
                        std::cout << "Replaying log file: " << get_logfile_name(0) << std::endl;
                        if (!replay_log(get_logfile_name(0))) {
                            std::cout << "Failed to replay log" << std::endl;
                            exit(1);
                        }
                    }
                } else {
                    std::cout << "No checkpoint or log found, starting from scratch" << std::endl;
                }
            } else {
                // Checkpoint found, use it
                // Checkpoint N was created from log N-1, so we replay log N-1
                check_point_id_ = current_check_point_id;
                
                // Replay the log file for this checkpoint (log N-1 for checkpoint N)
                size_t log_to_replay = check_point_id_ - 1;
                if (std::filesystem::exists(get_logfile_name(log_to_replay))) {
                    std::cout << "Replaying log " << log_to_replay << " (for checkpoint " << check_point_id_ << ")" << std::endl;
                    if (!replay_log(get_logfile_name(log_to_replay))) {
                        std::cout << "Failed to replay log " << log_to_replay << std::endl;
                        exit(1);
                    }
                }
                
                // After replay, we're ready for next checkpoint
                check_point_id_ = current_check_point_id + 1;
            }
            
            // Always open log file for writing (log N-1 for checkpoint N)
            std::string log_file_name_str = get_logfile_name(check_point_id_ - 1);
            std::cout << "DEBUG: Opening log file: " << log_file_name_str << std::endl;
            
            try {
                logger_ = std::make_unique<KVTLogger>(log_file_name_str, text_log_, persist_, do_fsync_, log_size_limit_, keep_history_);
                std::cout << "DEBUG: Log file opened successfully" << std::endl;
            } catch (const std::exception& e) {
                std::cout << "Failed to open log file: " << log_file_name_str << " - " << e.what() << std::endl;
                exit(1);
            }
        }

        bool replay_log_text(std::string log_file_name)
        {
            std::ifstream ifs(log_file_name);
            if (!ifs.is_open()) {
                std::cout << "Failed to open text log file: " << log_file_name << std::endl;
                return false;
            }
            
            std::string line;
            while (std::getline(ifs, line)) {
                if (line.empty()) continue;
                
                // Parse text format: ID LENGTH CHECKSUM PAYLOAD
                std::istringstream iss(line);
                uint64_t log_id;
                uint32_t payload_length, checksum;
                
                if (!(iss >> log_id >> payload_length >> checksum)) {
                    std::cout << "Failed to parse log entry header" << std::endl;
                    return false;
                }
                
                // Read the rest of the line as hex-encoded payload
                std::string hex_payload;
                std::getline(iss, hex_payload);
                // Remove leading space
                if (!hex_payload.empty() && hex_payload[0] == ' ') {
                    hex_payload = hex_payload.substr(1);
                }
                
                // Convert from hex string
                std::string payload = KVTLogger::from_hex_string_static(hex_payload);
                
                // Verify checksum
                uint32_t calculated_checksum = KVTLogger::calculate_checksum_static(payload.c_str(), payload.length());
                if (calculated_checksum != checksum) {
                    std::cout << "Checksum mismatch in log entry " << log_id << std::endl;
                    return false;
                }
                
                // Process the payload (same as binary format)
                if (!process_log_entry(payload)) {
                    return false;
                }
            }
            
            ifs.close();
            return true;
        }
        
        bool process_log_entry(const std::string& payload)
        {
            std::istringstream iss(payload);
            std::string operation;
            iss >> operation;
            
            if (operation == "CREATE_TABLE") {
                std::string table_name, partition_method;
                uint64_t logged_table_id;
                iss >> table_name >> partition_method >> logged_table_id;
                std::string error_msg;
                uint64_t new_table_id;
                KVTError result = create_table(table_name, partition_method, new_table_id, error_msg);
                if (result != KVTError::SUCCESS) {
                    std::cout << "Replay CREATE_TABLE failed: " << error_msg << std::endl;
                    return false;
                }
            }
            else if (operation == "DROP_TABLE") {
                uint64_t table_id;
                iss >> table_id;
                std::string error_msg;
                KVTError result = drop_table(table_id, error_msg);
                if (result != KVTError::SUCCESS) {
                    std::cout << "Replay DROP_TABLE failed: " << error_msg << std::endl;
                    return false;
                }
            }
            else if (operation == "START_TRANSACTION") {
                uint64_t tx_id;
                iss >> tx_id;
                std::string error_msg;
                KVTError result = start_transaction(tx_id, error_msg);
                if (result != KVTError::SUCCESS) {
                    std::cout << "Replay START_TRANSACTION failed: " << error_msg << std::endl;
                    return false;
                }
            }
            else if (operation == "COMMIT_TRANSACTION") {
                uint64_t tx_id;
                iss >> tx_id;
                std::string error_msg;
                KVTError result = commit_transaction(tx_id, error_msg);
                if (result != KVTError::SUCCESS) {
                    std::cout << "Replay COMMIT_TRANSACTION failed: " << error_msg << std::endl;
                    return false;
                }
            }
            else if (operation == "ROLLBACK_TRANSACTION") {
                uint64_t tx_id;
                iss >> tx_id;
                std::string error_msg;
                KVTError result = rollback_transaction(tx_id, error_msg);
                if (result != KVTError::SUCCESS) {
                    std::cout << "Replay ROLLBACK_TRANSACTION failed: " << error_msg << std::endl;
                    return false;
                }
            }
            else if (operation == "GET") {
                // GET operations don't modify state, skip during replay
                return true;
            }
            else if (operation == "SET") {
                uint64_t tx_id, table_id;
                std::string key, value;
                iss >> tx_id >> table_id >> key >> value;
                std::string error_msg;
                KVTError result = set(tx_id, table_id, key, value, error_msg);
                if (result != KVTError::SUCCESS) {
                    std::cout << "Replay SET failed: " << error_msg << std::endl;
                    return false;
                }
            }
            else if (operation == "DEL") {
                uint64_t tx_id, table_id;
                std::string key;
                iss >> tx_id >> table_id >> key;
                std::string error_msg;
                KVTError result = del(tx_id, table_id, key, error_msg);
                if (result != KVTError::SUCCESS) {
                    std::cout << "Replay DEL failed: " << error_msg << std::endl;
                    return false;
                }
            }
            else if (operation == "SCAN" || operation == "PROCESS" || 
                     operation == "RANGE_PROCESS" || operation == "BATCH_EXECUTE") {
                // Skip compound operations during replay
                return true;
            }
            else {
                std::cout << "Unknown operation in log: " << operation << std::endl;
                return false;
            }
            
            return true;
        }
        
        bool replay_log(std::string log_file_name)
        {
            // Try to detect format by attempting to read as binary first
            std::ifstream ifs(log_file_name, std::ios::binary);
            if (!ifs.is_open()) {
                std::cout << "Failed to open log file: " << log_file_name << std::endl;
                return false;
            }
            
            // Check if it's a text file by looking at first few bytes
            char first_bytes[8];
            ifs.read(first_bytes, 8);
            ifs.seekg(0);  // Reset to beginning
            
            bool is_text_format = false;
            // If first 8 bytes contain non-binary characters, it's likely text
            for (int i = 0; i < 8 && i < ifs.gcount(); i++) {
                if (first_bytes[i] >= '0' && first_bytes[i] <= '9') {
                    is_text_format = true;
                    break;
                }
            }
            
            if (is_text_format) {
                ifs.close();
                return replay_log_text(log_file_name);
            }
            
            while (ifs.good()) {
                // Read log entry header
                uint64_t log_id;
                uint32_t payload_length, checksum;
                
                if (!ifs.read(reinterpret_cast<char*>(&log_id), sizeof(log_id))) {
                    break; // End of file
                }
                if (!ifs.read(reinterpret_cast<char*>(&payload_length), sizeof(payload_length))) {
                    std::cout << "Corrupt log entry: could not read payload length" << std::endl;
                    return false;
                }
                if (!ifs.read(reinterpret_cast<char*>(&checksum), sizeof(checksum))) {
                    std::cout << "Corrupt log entry: could not read checksum" << std::endl;
                    return false;
                }
                
                // Read payload
                std::string payload(payload_length, '\0');
                if (!ifs.read(payload.data(), payload_length)) {
                    std::cout << "Corrupt log entry: could not read payload" << std::endl;
                    return false;
                }
                
                // Verify checksum
                uint32_t calculated_checksum = KVTLogger::calculate_checksum_static(payload.c_str(), payload.length());
                if (calculated_checksum != checksum) {
                    std::cout << "Checksum mismatch in log entry " << log_id << std::endl;
                    return false;
                }
                
                // Process the payload using common function
                if (!process_log_entry(payload)) {
                    return false;
                }
            }
            
            ifs.close();
            return true;
        }

        void try_check_point()
        {
            if (!persist_ || logger_->get_total_payload_size() <= log_size_limit_)
                return; 

            save_checkpoint(get_checkpoint_name(check_point_id_));
            logger_->close();
            
            // Move to next checkpoint
            check_point_id_++;
            
            // Open new log file (log N-1 for checkpoint N)
            try {
                logger_ = std::make_unique<KVTLogger>(get_logfile_name(check_point_id_ - 1), text_log_, persist_, do_fsync_, log_size_limit_, keep_history_);
            } catch (const std::exception& e) {
                std::cout << "Failed to open log file: " << get_logfile_name(check_point_id_ - 1) << " - " << e.what() << std::endl;
                exit(1);
            }
            for (size_t i = 0; i< 10; ++i){
                int64_t id = check_point_id_ - i - keep_history_;
                if (id < 0)
                    continue;
                if (std::filesystem::exists(get_checkpoint_name(id))) {
                    std::filesystem::remove(get_checkpoint_name(id));
                }   
                if (std::filesystem::exists(get_logfile_name(id))) {
                    std::filesystem::remove(get_logfile_name(id));
                }
            }
        }
            
        KVTError do_create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) 
        {
            // Log the operation
            std::cout << "DEBUG: do_create_table called, logging to file" << std::endl;
            logger_->start_entry() << "CREATE_TABLE " << table_name << " " << partition_method << " " << table_id;
            logger_->flush_log();
            std::cout << "DEBUG: Logged CREATE_TABLE operation" << std::endl;
            
            // Call the actual implementation
            return create_table(table_name, partition_method, table_id, error_msg);
        }
        KVTError do_drop_table(uint64_t table_id, std::string& error_msg)
        {
            // Log the operation
            logger_->start_entry() << "DROP_TABLE " << table_id;
            logger_->flush_log();
            // Call the actual implementation
            return drop_table(table_id, error_msg);
        }
        KVTError do_start_transaction(uint64_t& tx_id, std::string& error_msg)
        {
            // Log the operation
            logger_->start_entry() << "START_TRANSACTION " << tx_id;
            logger_->flush_log();
            // Call the actual implementation
            return start_transaction(tx_id, error_msg);
        }
        KVTError do_commit_transaction(uint64_t tx_id, std::string& error_msg) 
        {
            // Log the operation
            logger_->start_entry() << "COMMIT_TRANSACTION " << tx_id;
            logger_->flush_log();
            KVTError result = commit_transaction(tx_id, error_msg);
            try_check_point();
            return result;
        }

        KVTError do_rollback_transaction(uint64_t tx_id, std::string& error_msg)
        {
            // Log the operation
            logger_->start_entry() << "ROLLBACK_TRANSACTION " << tx_id;
            logger_->flush_log();
            KVTError result = rollback_transaction(tx_id, error_msg);
            try_check_point();
            return result;
        }

        KVTError do_scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
            const KVTKey& key_end, size_t num_item_limit, 
            std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg)
        {
            // Log the operation
            logger_->start_entry() << "SCAN " << tx_id << " " << table_id << " " << key_start << " " << key_end << " " << num_item_limit;
            logger_->flush_log();
            // Call the actual implementation
            return scan(tx_id, table_id, key_start, key_end, num_item_limit, results, error_msg);
        }

        KVTError do_get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                std::string& value, std::string& error_msg)
        {
            // Log the operation
            logger_->start_entry() << "GET " << tx_id << " " << table_id << " " << key;
            logger_->flush_log();
            // Call the actual implementation
            return get(tx_id, table_id, key, value, error_msg);
        }

        
        KVTError do_set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const std::string& value, std::string& error_msg) 
        {
            // Log the operation
            logger_->start_entry() << "SET " << tx_id << " " << table_id << " " << key << " " << value;
            logger_->flush_log();
            // Call the actual implementation
            KVTError result = set(tx_id, table_id, key, value, error_msg);
            if (tx_id == 0)
                try_check_point();
            return result;
        }

        KVTError do_del(uint64_t tx_id, uint64_t table_id, 
                const KVTKey& key, std::string& error_msg)
        {
            // Log the operation
            logger_->start_entry() << "DEL " << tx_id << " " << table_id << " " << key;
            logger_->flush_log();
            KVTError result = del(tx_id, table_id, key, error_msg);
            if (tx_id == 0)
                try_check_point();
            return result;
        }

        KVTError do_process(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const KVTProcessFunc& func, const std::string& parameter, std::string& result_value, std::string& error_msg)
        {
            // Log the operation (note: we don't log the function itself, just "FUNC")
            logger_->start_entry() << "PROCESS " << tx_id << " " << table_id << " " << key << " FUNC " << parameter;
            logger_->flush_log();
            // Call the actual implementation
            return process(tx_id, table_id, key, func, parameter, result_value, error_msg);
        }

        KVTError do_range_process(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                  const KVTKey& key_end, size_t num_item_limit, 
                  const KVTProcessFunc& func, const std::string& parameter, std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg)
        {
            // Log the operation (note: we don't log the function itself, just "FUNC")
            logger_->start_entry() << "RANGE_PROCESS " << tx_id << " " << table_id << " " << key_start << " " 
                                    << key_end << " " << num_item_limit << " FUNC " << parameter;
            logger_->flush_log();
            // Call the actual implementation
            return range_process(tx_id, table_id, key_start, key_end, num_item_limit, func, parameter, results, error_msg);
        }

        KVTError do_batch_execute(uint64_t tx_id, const KVTBatchOps& batch_ops, 
                 KVTBatchResults& batch_results, std::string& error_msg)
        {
            // Log the operation
            logger_->start_entry() << "BATCH_EXECUTE " << tx_id << " " << batch_ops.size();
            logger_->flush_log();
            // Call the actual implementation
            return batch_execute(tx_id, batch_ops, batch_results, error_msg);
        }
        

        virtual bool save_checkpoint(std::string checkpoint_name) = 0;
        virtual bool load_checkpoint(std::string checkpoint_name) = 0;
        // Table management
        virtual KVTError create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) = 0;
        virtual KVTError drop_table(uint64_t table_id, std::string& error_msg) = 0;
        virtual KVTError get_table_name(uint64_t table_id, std::string& table_name, std::string& error_msg) = 0;
        virtual KVTError get_table_id(const std::string& table_name, uint64_t& table_id, std::string& error_msg) = 0;
        virtual KVTError list_tables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg) = 0;
        virtual KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) = 0;
        virtual KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) = 0;
        virtual KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) = 0;
        // Data operations  
        virtual KVTError get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 std::string& value, std::string& error_msg) = 0;
        virtual KVTError set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const std::string& value, std::string& error_msg) = 0;
        virtual KVTError del(uint64_t tx_id, uint64_t table_id, 
                const KVTKey& key, std::string& error_msg) = 0;
        // Scan keys in range [key_start, key_end) - key_start inclusive, key_end exclusive
        virtual KVTError scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                  const KVTKey& key_end, size_t num_item_limit, 
                  std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg) = 0;

        virtual KVTError process(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const KVTProcessFunc& func, const std::string& parameter, std::string& result_value, std::string& error_msg)
                {
                    std::string orig_value;
                    KVTError r_get = do_get(tx_id, table_id, key, orig_value, error_msg);
                    if (r_get != KVTError::SUCCESS)
                        return r_get;
                    
                    KVTProcessInput input(&key, &orig_value, &parameter);
                    KVTProcessOutput output;
                    bool success = func(input, output);
                    
                    if (!success) {
                        if (output.return_value.has_value()) {
                            error_msg = output.return_value.value();
                        } else {
                            error_msg = "Process function failed";
                        }
                        return KVTError::EXT_FUNC_ERROR;
                    }
                    
                    if (output.update_value.has_value()) {
                        KVTError r_set = do_set(tx_id, table_id, key, output.update_value.value(), error_msg);
                        if (r_set != KVTError::SUCCESS) {
                            result_value.clear();
                            return r_set;
                        }
                    }
                    
                    if (output.delete_key) {
                        KVTError r_del = do_del(tx_id, table_id, key, error_msg);
                        if (r_del != KVTError::SUCCESS) {
                            result_value.clear();
                            return r_del;
                        }
                    }
                    
                    if (output.return_value.has_value()) {
                        result_value = output.return_value.value();
                    } else {
                        result_value.clear();
                    }
                    
                    return KVTError::SUCCESS;
                }
                
        virtual KVTError range_process(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                  const KVTKey& key_end, size_t num_item_limit, 
                  const KVTProcessFunc& func, const std::string& parameter, std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg)
                {
                    std::vector<std::pair<KVTKey, std::string>> temp_results;
                    KVTKey new_start_key = key_start;
                    KVTError r_scan = KVTError::UNKNOWN_ERROR;
                    bool first_item = true;
                    while (results.size() < num_item_limit) {
                        temp_results.clear();
                        r_scan = do_scan(tx_id, table_id, new_start_key, key_end, num_item_limit, temp_results, error_msg);
                        if (r_scan != KVTError::SUCCESS && r_scan != KVTError::SCAN_LIMIT_REACHED) {
                            results.clear();
                            return r_scan;
                        }
                        
                        for (auto& [key, orig_value] : temp_results) {
                            KVTProcessInput input(&key, &orig_value, &parameter);
                            input.range_first = first_item;
                            first_item = false;
                            KVTProcessOutput output;
                            bool success = func(input, output);
                            
                            if (!success) {
                                if (output.return_value.has_value()) {
                                    error_msg = output.return_value.value();
                                } else {
                                    error_msg = "Process function failed";
                                }
                                results.clear();
                                return KVTError::EXT_FUNC_ERROR;
                            }
                            
                            if (output.update_value.has_value()) {
                                KVTError r_set = do_set(tx_id, table_id, key, output.update_value.value(), error_msg);
                                if (r_set != KVTError::SUCCESS) {
                                    results.clear();
                                    return r_set;
                                }
                            }
                            
                            if (output.delete_key) {
                                KVTError r_del = do_del(tx_id, table_id, key, error_msg);
                                if (r_del != KVTError::SUCCESS) {
                                    results.clear();
                                    return r_del;
                                }
                            }
                            
                            if (output.return_value.has_value()) {
                                results.emplace_back(key, output.return_value.value());
                            }
                        }
                        
                        if (temp_results.empty()) break;
                        new_start_key = temp_results.back().first;
                        new_start_key += '\0'; // Move to next key
                    } 
                    std::string dummy; 
                    KVTProcessInput input(nullptr, nullptr, nullptr, false, true);
                    KVTProcessOutput output;
                    bool success = func(input, output);
                    if (!success) {
                        results.clear();
                        return KVTError::EXT_FUNC_ERROR;
                    }
                    if (output.return_value.has_value()) {
                        error_msg = output.return_value.value();
                    }
                    return r_scan;
                }
 
        // Default batch execute implementation - executes operations individually
        virtual KVTError batch_execute(uint64_t tx_id, const KVTBatchOps& batch_ops, 
                  KVTBatchResults& batch_results, std::string& error_msg) {
            batch_results.clear();
            batch_results.reserve(batch_ops.size());
            
            bool all_success = true;
            std::string concatenated_errors;
            
            for (size_t i = 0; i < batch_ops.size(); ++i) {
                const KVTOp& op = batch_ops[i];
                KVTOpResult result;
                std::string op_error;
                
                switch (op.op) {
                    case OP_GET:
                        result.error = do_get(tx_id, op.table_id, op.key, result.value, op_error);
                        break;
                    case OP_SET:
                        result.error = do_set(tx_id, op.table_id, op.key, op.value, op_error);
                        break;
                    case OP_DEL:
                        result.error = do_del(tx_id, op.table_id, op.key, op_error);
                        break;
                    default:
                        result.error = KVTError::UNKNOWN_ERROR;
                        op_error = "Unknown operation type";
                        break;
                }
                
                if (result.error != KVTError::SUCCESS) {
                    all_success = false;
                    if (!op_error.empty()) {
                        concatenated_errors += "op[" + std::to_string(i) + "]: " + op_error + "; ";
                    }
                }
                
                batch_results.push_back(result);
            }
            
            if (all_success) {
                return KVTError::SUCCESS;
            } else {
                error_msg = concatenated_errors;
                return KVTError::BATCH_NOT_FULLY_SUCCESS;
            }
        }
    
};

class KVTMemManagerBase : public KVTWrapper
{
    protected:
        struct Entry {
            std::string data;
            int32_t metadata; //for 2PL, it is the lock flag, for OCC, it is the version number. -1 means deleted. 
            
            Entry() : data(""), metadata(0) {}
            Entry(const std::string& d, int32_t m) : data(d), metadata(m) {}
        };

        struct Table {
            uint64_t id;
            std::string name;
            std::string partition_method;  // "hash" or "range"
            std::map<KVTKey, Entry> data;
            Table(const std::string& n, const std::string& pm, uint64_t i) : id(i), name(n), partition_method(pm) {}
        };

        struct Transaction {
            uint64_t tx_id;
            std::map<KVTKey, Entry> read_set;    // table_key -> Value (for reads)
            std::map<KVTKey, Entry> write_set;   // table_key -> Value (for writes)
            std::unordered_set<KVTKey> delete_set; // table_key -> deleted
            
            Transaction(uint64_t id) : tx_id(id) {}
        };

        std::unordered_map<std::string, std::unique_ptr<Table>> tables;
        std::unordered_map<std::string, uint64_t> tablename_to_id;
        std::mutex global_mutex;
        uint64_t next_table_id;
        uint64_t next_tx_id;
        
        // Helper function to get table by ID
        Table* get_table_by_id(uint64_t table_id) {
            for (auto& pair : tables) {
                if (pair.second->id == table_id) {
                    return pair.second.get();
                }
            }
            return nullptr;
        }

    public:
        KVTMemManagerBase() : KVTWrapper("./")
        {
            next_table_id = 1;
            next_tx_id = 1;
            // Note: startup() must be called by the final derived class
        }
        ~KVTMemManagerBase()
        {
        }

        bool save_checkpoint(std::string checkpoint_name) override
        {
            std::ofstream ofs(checkpoint_name, std::ios::binary);
            if (!ofs.is_open()) {
                std::cerr << "Failed to open checkpoint file for writing: " << checkpoint_name << std::endl;
                return false;
            }
            
            // Write metadata
            uint64_t num_tables = tables.size();
            ofs.write(reinterpret_cast<const char*>(&num_tables), sizeof(num_tables));
            ofs.write(reinterpret_cast<const char*>(&next_table_id), sizeof(next_table_id));
            ofs.write(reinterpret_cast<const char*>(&next_tx_id), sizeof(next_tx_id));
            
            // Write each table
            for (const auto& [table_name, table_ptr] : tables) {
                // Write table name
                uint64_t name_len = table_name.length();
                ofs.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
                ofs.write(table_name.data(), name_len);
                
                // Write table metadata
                ofs.write(reinterpret_cast<const char*>(&table_ptr->id), sizeof(table_ptr->id));
                uint64_t pm_len = table_ptr->partition_method.length();
                ofs.write(reinterpret_cast<const char*>(&pm_len), sizeof(pm_len));
                ofs.write(table_ptr->partition_method.data(), pm_len);
                
                // Write table data
                uint64_t num_entries = table_ptr->data.size();
                ofs.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
                
                for (const auto& [key, entry] : table_ptr->data) {
                    // Write key
                    uint64_t key_len = key.length();
                    ofs.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
                    ofs.write(key.data(), key_len);
                    
                    // Write entry data
                    uint64_t data_len = entry.data.length();
                    ofs.write(reinterpret_cast<const char*>(&data_len), sizeof(data_len));
                    ofs.write(entry.data.data(), data_len);
                    
                    // Write entry metadata
                    ofs.write(reinterpret_cast<const char*>(&entry.metadata), sizeof(entry.metadata));
                }
            }
            
            ofs.close();
            VERBOSE(std::cout << "Checkpoint saved to " << checkpoint_name << " with " << num_tables << " tables" << std::endl);
            return true;
        }
        
        bool load_checkpoint(std::string checkpoint_name) override
        {
            std::ifstream ifs(checkpoint_name, std::ios::binary);
            if (!ifs.is_open()) {
                std::cerr << "Failed to open checkpoint file for reading: " << checkpoint_name << std::endl;
                return false;
            }
            
            // Clear existing data
            tables.clear();
            tablename_to_id.clear();
            
            // Read metadata
            uint64_t num_tables;
            ifs.read(reinterpret_cast<char*>(&num_tables), sizeof(num_tables));
            ifs.read(reinterpret_cast<char*>(&next_table_id), sizeof(next_table_id));
            ifs.read(reinterpret_cast<char*>(&next_tx_id), sizeof(next_tx_id));
            
            // Read each table
            for (uint64_t i = 0; i < num_tables; ++i) {
                // Read table name
                uint64_t name_len;
                ifs.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
                std::string table_name(name_len, '\0');
                ifs.read(table_name.data(), name_len);
                
                // Read table metadata
                uint64_t table_id;
                ifs.read(reinterpret_cast<char*>(&table_id), sizeof(table_id));
                uint64_t pm_len;
                ifs.read(reinterpret_cast<char*>(&pm_len), sizeof(pm_len));
                std::string partition_method(pm_len, '\0');
                ifs.read(partition_method.data(), pm_len);
                
                // Create table
                auto table = std::make_unique<Table>(table_name, partition_method, table_id);
                
                // Read table data
                uint64_t num_entries;
                ifs.read(reinterpret_cast<char*>(&num_entries), sizeof(num_entries));
                
                for (uint64_t j = 0; j < num_entries; ++j) {
                    // Read key
                    uint64_t key_len;
                    ifs.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
                    std::string key(key_len, '\0');
                    ifs.read(key.data(), key_len);
                    
                    // Read entry data
                    uint64_t data_len;
                    ifs.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
                    std::string data(data_len, '\0');
                    ifs.read(data.data(), data_len);
                    
                    // Read entry metadata
                    int32_t metadata;
                    ifs.read(reinterpret_cast<char*>(&metadata), sizeof(metadata));
                    
                    table->data[key] = Entry(data, metadata);
                }
                
                tables[table_name] = std::move(table);
                tablename_to_id[table_name] = table_id;
            }
            
            ifs.close();
            VERBOSE(std::cout << "Checkpoint loaded from " << checkpoint_name << " with " << num_tables << " tables" << std::endl);
            return true;
        }
        // Table management
        KVTError create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) override
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (tables.find(table_name) != tables.end()) {
                error_msg = "Table '" + table_name + "' already exists";
                return KVTError::TABLE_ALREADY_EXISTS;
            }
            if (partition_method != "hash" && partition_method != "range") {
                error_msg = "Invalid partition method. Must be 'hash' or 'range'";
                return KVTError::INVALID_PARTITION_METHOD;
            }
            uint64_t table_id_val = next_table_id ++;
            tables[table_name] = std::make_unique<Table>(table_name, partition_method, table_id_val);
            tablename_to_id[table_name] = table_id_val;
            table_id = table_id_val;
            return KVTError::SUCCESS;
            
        }

        KVTError drop_table(uint64_t table_id, std::string& error_msg) override
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            std::string table_name;
            for (const auto& pair : tablename_to_id) {
                if (pair.second == table_id) {
                    table_name = pair.first;
                    break;
                }
            }
            if (table_name.empty()) {
                error_msg = "Table with ID " + std::to_string(table_id) + " not found";
                return KVTError::TABLE_NOT_FOUND;
            }
            tables.erase(table_name);
            tablename_to_id.erase(table_name);
            return KVTError::SUCCESS;
        }

        KVTError get_table_name(uint64_t table_id, std::string& table_name, std::string& error_msg) override
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            for (const auto& pair : tablename_to_id) {
                if (pair.second == table_id) {
                    table_name = pair.first;
                    return KVTError::SUCCESS;
                }
            }
            error_msg = "Table with ID " + std::to_string(table_id) + " not found";
            return KVTError::TABLE_NOT_FOUND;
        }

        KVTError get_table_id(const std::string& table_name, uint64_t& table_id, std::string& error_msg) override
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            auto it = tablename_to_id.find(table_name);
            if (it == tablename_to_id.end()) {
                error_msg = "Table '" + table_name + "' not found";
                return KVTError::TABLE_NOT_FOUND;
            }
            table_id = it->second;
            return KVTError::SUCCESS;
        }

        KVTError list_tables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg) override
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            results.clear();
            for (const auto& pair : tablename_to_id) {
                results.emplace_back(pair.first, pair.second);
            }
            return KVTError::SUCCESS;
        }
};

class KVTMemManagerNoCC : public KVTMemManagerBase
{
    private:
        std::map<KVTKey, std::string> table_data;  // Combined table data storage
    public:
        KVTMemManagerNoCC() : KVTMemManagerBase()
        {
            startup();  // Call startup after fully constructed
        }
        ~KVTMemManagerNoCC()
        {
        }

        KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) override;
        KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
        KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
        // Data operations  
        KVTError get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 std::string& value, std::string& error_msg) override;
        KVTError set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const std::string& value, std::string& error_msg) override;
        KVTError del(uint64_t tx_id, uint64_t table_id, 
                const KVTKey& key, std::string& error_msg) override;
        KVTError scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                  const KVTKey& key_end, size_t num_item_limit, 
                  std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg) override;
    }                  
;

class KVTMemManagerSimple : public KVTMemManagerBase
{
    private:
        //Only process one at a time, and transaction that can be rolled back.
        uint64_t current_tx_id;
        std::map<KVTKey, std::string> table_data;  // Combined table data storage
        std::map<KVTKey, std::string> write_set;   // Transaction write set
        std::unordered_set<KVTKey> delete_set;     // Transaction delete set

    public:
        KVTMemManagerSimple() : KVTMemManagerBase()
        {
            current_tx_id = 0;
            startup();  // Call startup after fully constructed
        }
        ~KVTMemManagerSimple()
        {
        }

        KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) override;
        KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
        KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
        // Data operations  
        KVTError get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 std::string& value, std::string& error_msg) override;
        KVTError set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const std::string& value, std::string& error_msg) override;
        KVTError del(uint64_t tx_id, uint64_t table_id, 
                const KVTKey& key, std::string& error_msg) override;
        KVTError scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                  const KVTKey& key_end, size_t num_item_limit, 
                  std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg) override;
};


class KVTMemManager : public KVTMemManagerBase
{
    protected:

        std::unordered_map<uint64_t, std::unique_ptr<Transaction>> transactions;

        Transaction* get_transaction(uint64_t tx_id) {
            auto it = transactions.find(tx_id);
            if (it == transactions.end()) {
                return nullptr;
            }
            return it->second.get();
        }

    public:
        KVTMemManager(std::string data_path = "./") : KVTMemManagerBase()
        {
            next_table_id = 1;
            next_tx_id = 1;
        }

        ~KVTMemManager()
        {
        }
    
        KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) override {
            std::lock_guard<std::mutex> lock(global_mutex);
            uint64_t tx_id_val = next_tx_id ++;
            transactions[tx_id_val] = std::make_unique<Transaction>(tx_id_val);
            tx_id = tx_id_val;
            return KVTError::SUCCESS;
        }
    };

class KVTMemManager2PL: public KVTMemManager
{
    // For table entries: metadata field stores the locking transaction ID (0 = unlocked)
    // When a transaction acquires a lock, it sets metadata to its tx_id
public:
    // Constructor
    KVTMemManager2PL(std::string data_path = "./") : KVTMemManager(data_path) {
        startup();  // Call startup after fully constructed
    }
    // Transaction management
    KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
    KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
    // Data operations  
    KVTError get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                std::string& value, std::string& error_msg) override;
    KVTError set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                const std::string& value, std::string& error_msg) override;
    KVTError del(uint64_t tx_id, uint64_t table_id, 
            const KVTKey& key, std::string& error_msg) override;
    KVTError scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                const KVTKey& key_end, size_t num_item_limit, 
                std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg) override;
};


class KVTMemManagerOCC: public KVTMemManager
{
    //for OCC, the metadata in an entry is the version number.
    //delete must be put into the read set so that it can keep version.
    //in this case, when a delete happens, it needs to be removed from write

    //Important Invariance: 
    //1. a key cannot appear in both write set and delete set. 
    //2. a deleted key must be in read set, if it does not already in write set (and then need to be removed)

public:
    // Constructor
    KVTMemManagerOCC(std::string data_path = "./") : KVTMemManager(data_path) {
        startup();  // Call startup after fully constructed
    }
    
  // Transaction management
    KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
    KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
    // Data operations  
    KVTError get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                std::string& value, std::string& error_msg) override;
    KVTError set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                const std::string& value, std::string& error_msg) override;
    KVTError del(uint64_t tx_id, uint64_t table_id, 
            const KVTKey& key, std::string& error_msg) override;
    KVTError scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                const KVTKey& key_end, size_t num_item_limit, 
                std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg) override;
  };

#endif // KVT_MEM_H
