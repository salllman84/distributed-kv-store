#ifndef PROTOCOL_HPP
#define PROTOCOL_HPP

#include "raft/state.hpp"
#include <string>
#include <sstream>
#include <vector>

namespace common {

class Protocol {
public:
    // --- RequestVote Serialization ---
    static std::string serializeRequestVote(const raft::RequestVoteArgs& args) {
        std::ostringstream oss;
        oss << "REQ_VOTE " 
            << args.term << " " 
            << args.candidate_id << " " 
            << args.last_log_index << " " 
            << args.last_log_term << "\n";
        return oss.str();
    }

    static raft::RequestVoteArgs deserializeRequestVote(std::istringstream& iss) {
        raft::RequestVoteArgs args;
        iss >> args.term >> args.candidate_id >> args.last_log_index >> args.last_log_term;
        return args;
    }

    static std::string serializeRequestVoteReply(const raft::RequestVoteReply& reply) {
        std::ostringstream oss;
        oss << "VOTE_REPLY " << reply.term << " " << (reply.vote_granted ? 1 : 0) << "\n";
        return oss.str();
    }

    // --- AppendEntries Serialization ---
    static std::string serializeAppendEntries(const raft::AppendEntriesArgs& args) {
        std::ostringstream oss;
        oss << "APPEND_ENTRIES " 
            << args.term << " " 
            << args.leader_id << " " 
            << args.prev_log_index << " " 
            << args.prev_log_term << " " 
            << args.leader_commit << " "
            << args.entries.size();
        
        for (const auto& entry : args.entries) {
            oss << " " << entry.term << " " << entry.command.length() << " " << entry.command;
        }
        oss << "\n";
        return oss.str();
    }

    static raft::AppendEntriesArgs deserializeAppendEntries(std::istringstream& iss) {
        raft::AppendEntriesArgs args;
        size_t entry_count = 0;
        iss >> args.term >> args.leader_id >> args.prev_log_index >> args.prev_log_term >> args.leader_commit >> entry_count;
        
        for (size_t i = 0; i < entry_count; ++i) {
            uint64_t term;
            size_t len;
            std::string cmd;
            iss >> term >> len;
            iss.ignore(); // skip space
            char buf[256] = {0};
            iss.read(buf, len);
            args.entries.push_back(raft::LogEntry{term, std::string(buf, len)});
        }
        return args;
    }

    static std::string serializeAppendEntriesReply(const raft::AppendEntriesReply& reply) {
        std::ostringstream oss;
        oss << "APPEND_REPLY " << reply.term << " " << (reply.success ? 1 : 0) << "\n";
        return oss.str();
    }
};

} // namespace common

#endif // PROTOCOL_HPP