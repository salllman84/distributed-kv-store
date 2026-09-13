#include "raft/log.hpp"

namespace raft {

RaftLog::RaftLog() : commit_index_(0), last_applied_(0) {
    // Push a dummy entry at index 0 to simplify 1-based log indexing logic
    entries_.push_back(LogEntry{0, ""});
}

uint64_t RaftLog::append(uint64_t term, const std::string& command) {
    entries_.push_back(LogEntry{term, command});
    return lastIndex();
}

void RaftLog::appendEntries(uint64_t prev_log_index, const std::vector<LogEntry>& new_entries) {
    // Truncate any conflicting entries if necessary, then append new ones
    // (Detailed conflict resolution handled inside consensus logic)
    for (size_t i = 0; i < new_entries.size(); ++i) {
        uint64_t target_index = prev_log_index + 1 + i;
        if (target_index < entries_.size()) {
            if (entries_[target_index].term != new_entries[i].term) {
                truncate(target_index);
                entries_.push_back(new_entries[i]);
            }
            // If term matches, already exists, skip
        } else {
            entries_.push_back(new_entries[i]);
        }
    }
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
    return entries_.size() - 1; // Exclude dummy entry
}

void RaftLog::truncate(uint64_t index) {
    if (index < entries_.size()) {
        entries_.erase(entries_.begin() + index, entries_.end());
    }
}

} // namespace raft