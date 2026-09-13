#include "raft/log.hpp"
#include <iostream>
#include <fstream>
#include <sstream>

namespace raft {

RaftLog::RaftLog(int node_id) 
    : node_id_(node_id), 
      commit_index_(0), 
      last_applied_(0),
      last_included_index_(0),
      last_included_term_(0) {
    
    filename_ = "node_" + std::to_string(node_id_) + ".wal";
    entries_.push_back(LogEntry{0, ""}); // Dummy entry
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
        
        // Skip entries that are already part of our snapshot
        if (target_index <= last_included_index_) continue;

        uint64_t vec_index = target_index - last_included_index_;
        if (vec_index < entries_.size()) {
            if (entries_[vec_index].term != new_entries[i].term) {
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
    if (index <= last_included_index_ || index > lastIndex()) {
        return std::nullopt; // Discarded by snapshot or out of bounds
    }
    return entries_[index - last_included_index_];
}

uint64_t RaftLog::getTerm(uint64_t index) const {
    if (index == last_included_index_) {
        return last_included_term_; // Handled by dummy offset entry
    }
    if (index < last_included_index_ || index > lastIndex()) {
        return 0;
    }
    return entries_[index - last_included_index_].term;
}

uint64_t RaftLog::lastIndex() const {
    return last_included_index_ + entries_.size() - 1;
}

uint64_t RaftLog::lastTerm() const {
    if (entries_.size() == 1) { // Only the offset dummy entry exists
        return last_included_term_;
    }
    return entries_.back().term;
}

size_t RaftLog::size() const {
    return lastIndex(); 
}

void RaftLog::truncate(uint64_t index) {
    if (index <= last_included_index_) return; // Cannot truncate compacted logs
    uint64_t vec_index = index - last_included_index_;
    if (vec_index < entries_.size()) {
        entries_.erase(entries_.begin() + vec_index, entries_.end());
        persist();
    }
}

void RaftLog::compact(uint64_t snapshot_index, uint64_t snapshot_term) {
    if (snapshot_index <= last_included_index_) return; // Already compacted past this point

    uint64_t vec_index = snapshot_index - last_included_index_;
    std::vector<LogEntry> new_entries;
    
    // Create new dummy entry holding the offset state
    new_entries.push_back(LogEntry{snapshot_term, ""});

    // Copy over any logs that occurred *after* the snapshot index
    if (vec_index < entries_.size()) {
        new_entries.insert(new_entries.end(), entries_.begin() + vec_index + 1, entries_.end());
    }

    entries_ = std::move(new_entries);
    last_included_index_ = snapshot_index;
    last_included_term_ = snapshot_term;
    
    persist();
}

void RaftLog::persist() const {
    std::ofstream outfile(filename_, std::ios::trunc);
    if (!outfile.is_open()) return;

    // Write metadata header to support loading compacted offsets
    outfile << "SNAP " << last_included_index_ << " " << last_included_term_ << "\n";

    for (size_t i = 1; i < entries_.size(); ++i) {
        outfile << entries_[i].term << " " << entries_[i].command.length() << " " << entries_[i].command << "\n";
    }
}

void RaftLog::load() {
    std::ifstream infile(filename_);
    if (!infile.is_open()) return;

    std::string header;
    if (infile >> header) {
        if (header == "SNAP") {
            // New snapshot-aware format
            infile >> last_included_index_ >> last_included_term_;
            entries_[0].term = last_included_term_;
        } else {
            // Backward compatibility for old WAL files before compaction existed
            uint64_t term = std::stoull(header);
            size_t length;
            infile >> length;
            infile.ignore(); // skip space
            std::string cmd(length, '\0');
            infile.read(&cmd[0], length);
            infile.ignore(); // skip newline
            entries_.push_back(LogEntry{term, cmd});
        }
    }

    uint64_t term;
    size_t length;
    while (infile >> term >> length) {
        infile.ignore();
        std::string cmd(length, '\0');
        infile.read(&cmd[0], length);
        infile.ignore();
        entries_.push_back(LogEntry{term, cmd});
    }
    std::cout << "[RaftLog Node " << node_id_ << "] Recovered WAL up to index " << lastIndex() 
              << " (Snapshot index offset: " << last_included_index_ << ").\n" << std::flush;
}

} // namespace raft