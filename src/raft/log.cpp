#include "config.hpp"
#include "raft/log.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace raft {

// =============================================================================
// CONSTRUCTOR / DESTRUCTOR
// =============================================================================
RaftLog::RaftLog(int node_id)
    : node_id_(node_id),
      commit_index_(0),
      last_applied_(0),
      last_included_index_(0),
      last_included_term_(0) {

    filename_ = "node_" + std::to_string(node_id_) + ".wal";
    entries_.push_back(LogEntry{0, ""});   // dummy offset entry

    load();                                 // parse existing WAL into entries_
    openWALForAppend();                     // persistent fd, O_APPEND

    fsync_running_ = true;
    fsync_thread_ = std::thread(&RaftLog::fsyncLoop, this);
}

RaftLog::~RaftLog() {
    if (fsync_running_) {
        fsync_running_ = false;
        fsync_cv_.notify_all();
        if (fsync_thread_.joinable()) fsync_thread_.join();
    }
    closeWAL();
}

// =============================================================================
// APPEND — fast path, no fsync
// =============================================================================
uint64_t RaftLog::append(uint64_t term, const std::string& command) {
    std::unique_lock<std::mutex> lock(wal_mutex_);
    entries_.push_back(LogEntry{term, command});
    uint64_t idx = lastIndex();
    writeEntryRaw(term, command);
    unsynced_index_ = idx;
    fsync_cv_.notify_one();   // wake the fsync thread
    return idx;
}

void RaftLog::appendEntries(uint64_t prev_log_index,
                            const std::vector<LogEntry>& new_entries) {
    std::unique_lock<std::mutex> lock(wal_mutex_);

    bool changed = false;
    for (size_t i = 0; i < new_entries.size(); ++i) {
        uint64_t target_index = prev_log_index + 1 + i;
        if (target_index <= last_included_index_) continue;

        uint64_t vec_index = target_index - last_included_index_;
        if (vec_index < entries_.size()) {
            if (entries_[vec_index].term != new_entries[i].term) {
                // Conflict: truncate from here on, then append the new entry.
                entries_.erase(entries_.begin() + vec_index, entries_.end());
                entries_.push_back(new_entries[i]);
                changed = true;
            }
            // else: matching entry already present. Do NOT push_back again.
        } else {
            entries_.push_back(new_entries[i]);
            changed = true;
        }
    }

    if (changed) {
        // Full rewrite: correct and matches the previous behavior for the
        // follower path. The leader's client-hot path (append()) stays O(1).
        rewriteWALRaw();
        durable_index_  = lastIndex();
        unsynced_index_ = lastIndex();
    }
}

// =============================================================================
// READ PATH
// =============================================================================
std::optional<LogEntry> RaftLog::getEntry(uint64_t index) const {
    if (index <= last_included_index_ || index > lastIndex()) {
        return std::nullopt;
    }
    return entries_[index - last_included_index_];
}

uint64_t RaftLog::getTerm(uint64_t index) const {
    if (index == last_included_index_) return last_included_term_;
    if (index < last_included_index_ || index > lastIndex()) return 0;
    return entries_[index - last_included_index_].term;
}

uint64_t RaftLog::lastIndex() const {
    return last_included_index_ + entries_.size() - 1;
}

uint64_t RaftLog::lastTerm() const {
    if (entries_.size() == 1) return last_included_term_;
    return entries_.back().term;
}

size_t RaftLog::size() const {
    return (size_t)lastIndex();
}

// =============================================================================
// TRUNCATE / COMPACT — rare, full rewrite
// =============================================================================
void RaftLog::truncate(uint64_t index) {
    if (index <= last_included_index_) return;
    uint64_t vec_index = index - last_included_index_;
    if (vec_index < entries_.size()) {
        std::unique_lock<std::mutex> lock(wal_mutex_);
        entries_.erase(entries_.begin() + vec_index, entries_.end());
        rewriteWALRaw();
        durable_index_  = lastIndex();
        unsynced_index_ = lastIndex();
    }
}

void RaftLog::compact(uint64_t snapshot_index, uint64_t snapshot_term) {
    if (snapshot_index <= last_included_index_) return;

    uint64_t vec_index = snapshot_index - last_included_index_;
    std::unique_lock<std::mutex> lock(wal_mutex_);

    std::vector<LogEntry> new_entries;
    new_entries.push_back(LogEntry{snapshot_term, ""});
    if (vec_index < entries_.size()) {
        new_entries.insert(new_entries.end(),
                           entries_.begin() + vec_index + 1,
                           entries_.end());
    }
    entries_ = std::move(new_entries);
    last_included_index_ = snapshot_index;
    last_included_term_  = snapshot_term;

    rewriteWALRaw();
    durable_index_  = lastIndex();
    unsynced_index_ = lastIndex();
}

// =============================================================================
// RAW I/O
// =============================================================================
void RaftLog::openWALForAppend() {
    wal_fd_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (wal_fd_ < 0) {
        std::cerr << "[RaftLog] open(" << filename_ << ") for append failed: "
                  << std::strerror(errno) << "\n" << std::flush;
    }
}

void RaftLog::closeWAL() {
    if (wal_fd_ >= 0) {
        ::fsync(wal_fd_);
        ::close(wal_fd_);
        wal_fd_ = -1;
    }
}

void RaftLog::writeEntryRaw(uint64_t term, const std::string& command) {
    if (wal_fd_ < 0) return;
    // Format matches the loader: "<term> <len> <command>\n"
    char header[64];
    int n = std::snprintf(header, sizeof(header), "%lu %zu ",
                          (unsigned long)term, command.size());
    if (n <= 0) return;

    // Best-effort full write. On a healthy local fd this cannot short-write
    // for <1KB payloads, but we retry anyway for correctness.
    auto write_all = [this](const char* buf, size_t len) {
        size_t off = 0;
        while (off < len) {
            ssize_t w = ::write(wal_fd_, buf + off, len - off);
            if (w <= 0) return;
            off += (size_t)w;
        }
    };
    write_all(header, (size_t)n);
    write_all(command.data(), command.size());
    write_all("\n", 1);
    // NOTE: no fsync here — the fsync thread batches it.
}

void RaftLog::rewriteWALRaw() {
    // Close and reopen with truncate.
    if (wal_fd_ >= 0) {
        ::close(wal_fd_);
        wal_fd_ = -1;
    }
    int fd = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        std::cerr << "[RaftLog] rewrite open(" << filename_
                  << ") failed: " << std::strerror(errno) << "\n" << std::flush;
        openWALForAppend();
        return;
    }

    auto write_all = [fd](const char* buf, size_t len) {
        size_t off = 0;
        while (off < len) {
            ssize_t w = ::write(fd, buf + off, len - off);
            if (w <= 0) return;
            off += (size_t)w;
        }
    };

    char header[128];
    int n = std::snprintf(header, sizeof(header), "SNAP %lu %lu\n",
                          (unsigned long)last_included_index_,
                          (unsigned long)last_included_term_);
    if (n > 0) write_all(header, (size_t)n);

    for (size_t i = 1; i < entries_.size(); ++i) {
        char hdr[64];
        int m = std::snprintf(hdr, sizeof(hdr), "%lu %zu ",
                              (unsigned long)entries_[i].term,
                              entries_[i].command.size());
        if (m <= 0) continue;
        write_all(hdr, (size_t)m);
        write_all(entries_[i].command.data(), entries_[i].command.size());
        write_all("\n", 1);
    }

    ::fsync(fd);
    ::close(fd);
    openWALForAppend();
}

// =============================================================================
// FSYNC THREAD — group commit
// =============================================================================
void RaftLog::fsyncLoop() {
    while (fsync_running_) {
        uint64_t target = 0;
        {
            std::unique_lock<std::mutex> lock(wal_mutex_);
            fsync_cv_.wait_for(lock, std::chrono::milliseconds(5), [this]() {
                return !fsync_running_
                    || unsynced_index_ > durable_index_;
            });
            if (!fsync_running_) break;
            target = unsynced_index_;
            if (target <= durable_index_) continue;
        }

        // ---- Fault injection: simulate slow disk on the WAL fsync path ----
        // This is the fault that matters for the paper's claim: if fsync is
        // slow, waitForDurable() blocks, the Raft tick thread blocks on
        // applyLogsToStore(), and heartbeats get delayed. That is the
        // mechanism by which storage degradation threatens consensus.
        if (config::GlobalConfig::instance().fault_inject_active.load(
                std::memory_order_relaxed)) {
            int ms = config::GlobalConfig::instance().fault_inject_flush_latency_ms;
            if (ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            }
        }

        // fsync outside the lock. All appends that arrived while we were
        // sleeping are covered by this single syscall — that is the
        // "group commit" batching.
        if (wal_fd_ >= 0) {
            if (::fsync(wal_fd_) != 0) {
                std::cerr << "[RaftLog] fsync failed: "
                          << std::strerror(errno) << "\n" << std::flush;
            }
        }

        {
            std::unique_lock<std::mutex> lock(wal_mutex_);
            if (target > durable_index_) durable_index_ = target;
        }
        durable_cv_.notify_all();
    }
}

bool RaftLog::waitForDurable(uint64_t index, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(wal_mutex_);
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (durable_index_ < index) {
        if (durable_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
            return durable_index_ >= index;
        }
    }
    return true;
}

uint64_t RaftLog::getDurableIndex() const {
    std::unique_lock<std::mutex> lock(wal_mutex_);
    return durable_index_;
}

// =============================================================================
// LOAD — parse existing WAL into memory
// =============================================================================
void RaftLog::load() {
    std::ifstream infile(filename_);
    if (!infile.is_open()) return;

    std::string header;
    if (infile >> header) {
        if (header == "SNAP") {
            infile >> last_included_index_ >> last_included_term_;
            entries_[0].term = last_included_term_;
        } else {
            // Pre-snapshot format: first line is an entry, not a header.
            uint64_t term = std::stoull(header);
            size_t length;
            infile >> length;
            infile.ignore();
            std::string cmd(length, '\0');
            infile.read(&cmd[0], (std::streamsize)length);
            infile.ignore();
            entries_.push_back(LogEntry{term, cmd});
        }
    }

    uint64_t term;
    size_t length;
    while (infile >> term >> length) {
        infile.ignore();
        std::string cmd(length, '\0');
        infile.read(&cmd[0], (std::streamsize)length);
        infile.ignore();
        entries_.push_back(LogEntry{term, cmd});
    }

    durable_index_  = lastIndex();
    unsynced_index_ = lastIndex();

    std::cout << "[RaftLog Node " << node_id_ << "] Recovered WAL up to index "
              << lastIndex() << " (Snapshot index offset: "
              << last_included_index_ << ").\n" << std::flush;
}

} // namespace raft