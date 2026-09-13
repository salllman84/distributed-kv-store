#include "raft/log.hpp"
#include <iostream>
#include <fstream>
#include <sstream>

namespace raft {

RaftLog::RaftLog(int node_id) : node_id_(node_id), commit_index_(0), last_applied_(0) {
    filename_ = "node_" + std::to_string(node_id_) + ".wal";
    // Push dummy entry at index 0
    entries_.push_back(LogEntry{0, ""});
    load(); // Recover from disk if WAL exists
}

uint64_t RaftLog::append(uint64_t term, const std::string& command) {
    entries_.push_back(LogEntry{term, command});
    persist();
    return lastIndex();
}

void RaftLog::appendEntries(uint64_t prev_log_index, const std::vector<LogEntry>& new_entries) {
    for (size_t i = 0; i < new_entries.size(); ++i) {
        uint64_t target_index = prev_log_index + 1 + i;
        if (target_index < entries_.size()) {
            if (entries_[target_index].term != new_entries[i].term) {
                truncate(target_index);
                entries_.push_back(new_entries[i]);
            }
        } else {
            entries_.push_back(new_entries[i]);
        }
    }
    persist();
}

std::optional<LogEntry> RaftLog::getEntry(uint64_t index) const {
    if (index == 0 || index >= entries_.size()) {
        return std::nullopt;
    }
    return entries_[index];
}

uint64_t RaftLog::getTerm(uint64_t index) const {
    if (index == 0 || index >= entries_.size()) {
        return 0;
    }
    return entries_[index].term;
}

uint64_t RaftLog::lastIndex() const {
    return entries_.size() - 1;
}

uint64_t RaftLog::lastTerm() const {
    if (entries_.empty()) {
        return 0;
    }
    return entries_.back().term;
}

size_t RaftLog::size() const {
    return entries_.size() - 1; 
}

void RaftLog::truncate(uint64_t index) {
    if (index < entries_.size()) {
        entries_.erase(entries_.begin() + index, entries_.end());
        persist();
    }
}

void RaftLog::persist() const {
    std::ofstream outfile(filename_, std::ios::trunc);
    if (!outfile.is_open()) return;

    // Save all entries starting from index 1
    for (size_t i = 1; i < entries_.size(); ++i) {
        outfile << entries_[i].term << " " << entries_[i].command.length() << " " << entries_[i].command << "\n";
    }
}

void RaftLog::load() {
    std::ifstream infile(filename_);
    if (!infile.is_open()) return;

    uint64_t term;
    size_t length;
    while (infile >> term >> length) {
        infile.ignore(); // skip space
        std::string cmd(length, '\0');
        infile.read(&cmd[0], length);
        infile.ignore(); // skip newline
        entries_.push_back(LogEntry{term, cmd});
    }
    std::cout << "[RaftLog Node " << node_id_ << "] Recovered " << (entries_.size() - 1) << " entries from disk WAL.\n";
}

} // namespace raft