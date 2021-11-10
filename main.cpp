#include <iostream>
#include <string>
#include <vector>
#include <array>
#include <deque>
#include <thread>
#include <mutex>
#include <chrono>
#include <cstdlib>
#include <unistd.h>
#include "common/json.hpp"
#include "common/fmt.hpp"
#include "common/base64.hpp"

// for convenience
using json = nlohmann::json;
using namespace std;

void DEBUG_INFO(string info){
    std::cout << "\x1b[34m" << info << "\x1b[0m" << std::endl;
}

// interaction with the frame work, a base for message sending/receiving
namespace framework{
    const int TIME_BOUND = 1000; // ms

    void send(int to_node_id, json& j){
        // json to string
        string message = j.dump();
        fmt::print("SEND {} {}\n", to_node_id, message); cout << flush;
    }

    void state(json j){
        for (auto it = j.begin(); it != j.end(); ++it)
        {
            if(it.key()=="leader"){
//                auto val_str = std::to_string(it.value().get<int>());
//                auto val_uvec = std::vector<unsigned char>(val_str.data(), val_str.data() + val_str.length() + 1);
//                unsigned char* val_uchar = &val_uvec[0]; //base64_encode(val_uchar,val_str.size())
                fmt::print("STATE {}=\"{}\"\n", it.key(), it.value()); cout << flush;
            }
            else{
                fmt::print("STATE {}={}\n", it.key(), it.value()); cout << flush;
            }
        }
    }

    void commit(int index, string& s){
        fmt::print("COMMITTED {} {}\n", s, index); cout << flush;
    }

}

namespace raft{

    // Server states
    const string LEADER = "LEADER";
    const string CANDIDATE = "CANDIDATE";
    const string FOLLOWER = "FOLLOWER";

    // RPC type
    const string APPENDENTIES = "AppendEntries";
    const string REQUESTVOTE = "RequestVotes";

    const string APPENDENTIES_RPL = "AppendEntriesResponse";
    const string REQUESTVOTE_RPL = "RequestVotesResponse";


    class Log_Entry{
            // The term and contents of the log entry i, including the term number and the contents.
        public:
            int index = 1;
            int term = 1;
            int node_num = 1;
            string command;

            Log_Entry() = default;
            Log_Entry(int index_, int term_, string command_) : index(index_), term(term_) , command(command_) {}

            Log_Entry(string json_str){
                json j = json::parse(json_str);
                index = stoi(j.begin().key().substr(j.begin().key().find_first_of('['),(j.begin().key().find_last_of(']')-j.begin().key().find_first_of('[')-1)));
                term = j.begin().value()[0];
                command = j.begin().value()[1];
            }

            string to_str(){
                json j;
                j["log["+to_string(index)+"]"] = {term, command};
                return j.dump();
            }

            json to_json(){
                json j;
                j["log["+to_string(index)+"]"] = {term, command};
                return j;
            }

    };

    struct Raft_Node_Info{
        int node_id = 0;
        bool alive = true;
        bool voted_for_me = false;
        unsigned long int next_idx = 0;
    };

    class Raft{
        private:
            int this_node_id = 0;
            int group_node_number = 1;
            vector<Raft_Node_Info> nodes_info; // for other nodes
            bool voted = false;
            unsigned int timeout = framework::TIME_BOUND;
            chrono::time_point<chrono::high_resolution_clock> timeout_start{ chrono::high_resolution_clock::now() };

            // (Log entries are numbered starting at 1.)
            unsigned long int term = 1;// The current term according to the node
            string state = FOLLOWER; // One of LEADER, FOLLOWER, or CANDIDATE (please use all caps)
            int leader = -1; // The identity of the the leader of the current term, if known
            vector<Log_Entry> log_t;
            unsigned long int commitIndex = 0; // The index of the last committed entry.
            unsigned long int matchIndex = 0;

            json latest_state = {{"term",this->term},
                                 {"state",this->state},
                                 {"leader",this->leader},
                                 {"commitIndex",this->commitIndex},};
            json history_state = {{"term",this->term},
                                  {"state",this->state},
                                  {"leader",this->leader},
                                  {"commitIndex",this->commitIndex},};

            //reset start time
            inline void reset_start_time()
            {
                timeout_start = chrono::high_resolution_clock::now();
            }
            //reset timeout
            inline void reset_time_out(unsigned int time_out)
            {
                timeout = time_out;
            }



        public:


            Raft(int group_node_number_, int this_node_id_) : group_node_number(group_node_number_), this_node_id(this_node_id_) {
                this->nodes_info = vector<Raft_Node_Info>(group_node_number_-1);

                vector<Raft_Node_Info>::iterator node_iter = this->nodes_info.begin();
                for(int node_id=0; node_id<group_node_number_; node_id++){
                    if(node_id!=this_node_id_ && node_iter!=this->nodes_info.end()) {
                        node_iter->node_id = node_id;
                        node_iter++;
                    }
                }
                this->reset_time_out(framework::TIME_BOUND + rand()%framework::TIME_BOUND);
            }

            void append_log(string log_str){
                Log_Entry log_entry;
                log_entry.index = log_t.size()+1;
                log_entry.term = term;
                log_entry.command = log_str;
                log_t.push_back(log_entry);
            }

            //is timeout
            bool is_time_out()
            {
                chrono::time_point<chrono::high_resolution_clock> now = chrono::high_resolution_clock::now();
                return chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch() - this->timeout_start.time_since_epoch()).count() >= chrono::milliseconds(this->timeout).count();
            }

            void heartbeat(){
                chrono::time_point<chrono::high_resolution_clock> now = chrono::high_resolution_clock::now();
                if (chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch() - this->timeout_start.time_since_epoch()).count() >= framework::TIME_BOUND) {
                    for(auto node: this->nodes_info){
                        this->send_append_entry(node.node_id);
                        //DEBUG_INFO("heartbeat");
                    }
                    this->reset_start_time();
                }
            }

            void become_leader(){
                //DEBUG_INFO("become leader");
                this->leader = this_node_id;
                this->state = LEADER;
                this->voted = true;
                for(auto& node: this->nodes_info){
                    node.next_idx = this->log_t.size() + 1;
                    this->send_append_entry(node.node_id);
                }
            }
            void become_candidate(){
                this->state = CANDIDATE;
                this->term++;

                this->reset_start_time();
                this->reset_time_out(framework::TIME_BOUND + rand()%framework::TIME_BOUND);
                this->voted = true; // voted for self

                this->clear_votes();
                this->send_request_vote(); // request vote
            }
            void become_follower(){
                this->state = FOLLOWER;
                this->voted = false;
            }

            int count_votes() {
                int votes = 1;
                for (auto node: this->nodes_info) if (node.voted_for_me)  votes += 1;
                return votes;
            }

            void clear_votes(){
                for (auto& node: this->nodes_info) node.voted_for_me = 0;
            }

            [[nodiscard]] bool is_follower() const { return this->state == FOLLOWER; }

            [[nodiscard]] bool is_leader() const { return this->state == LEADER; }

            [[nodiscard]] bool is_candidate() const { return this->state == CANDIDATE; }

            void set(string state_) { this->state = move(state_); }

            [[nodiscard]] string get() const { return this->state; }

            /**
             * Send Messages to STDOUT
             */
            void send_request_vote(){
                for(auto node: this->nodes_info)
                {
                    json j = json{{"msg_type", REQUESTVOTE},
                                  {"candidateId",this->this_node_id}, // candidate requesting vote
                                  {"term",this->term}, // candidate's term
                                  {"lastLogIndex", (this->log_t.empty() ? 0 : log_t.back().index)}, // index of candidate's last log entry
                                  {"lastLogTerm", (this->log_t.empty() ? 0 : log_t.back().term)}, // term of candidate's last log entry
                                  };
                    framework::send(node.node_id,j);
                }
            }

            void reply_vote(int candiate){
                json j;
                if(!voted){
                    j = json{{"msg_type", REQUESTVOTE_RPL},
                             {"voterId",this->this_node_id},
                              {"term",this->term}, // currentTerm, for candidate to update itself
                              {"granted", true}, // true means candidate received vote
                    };

                    this->voted = true;
                }else{
                    j = json{{"msg_type", REQUESTVOTE_RPL},
                             {"voterId",this_node_id},
                             {"term",this->term}, // currentTerm, for candidate to update itself
                             {"granted", false},
                    };
                }
                framework::send(candiate,j);
            }

            void send_append_entry(int nid){
                vector<string> entries_to_append;
                for(auto log:this->log_t){
                    entries_to_append.push_back(log.to_str());
                }
                json j = json{{"msg_type", APPENDENTIES},
                              {"leaderId",this->this_node_id}, // candidate requesting vote
                              {"term",this->term}, // leader's term
                              {"prevLogIndex", (this->log_t.empty() ? 0 : log_t.back().index)}, // index of log entry immediately preceding new ones
                              {"prevLogTerm", (this->log_t.empty() ? 0 : log_t.back().term)}, // term of prevLogIndex entry
                              {"entries",entries_to_append}, // log entries to store (empty for heartbeat) --> json of vector of json string
                              {"commitIndex", this->commitIndex} // last entry known to be committed
                };
                framework::send(nid,j);
            }

            void reply_append_entry(int nid, bool success){
                json j = json{{"msg_type", APPENDENTIES_RPL},
                             {"followerId",this->this_node_id},
                             {"term",this->term}, // currentTerm, for leader to update itself
                             {"success", success}, // true if follower contained entry matching prevLogIndex and prevLogTerm
                              {"matchIndex", this->matchIndex}
                };
                framework::send(nid,j);
            }

            /**
             * Parse Recv Messages from STDIN
             * 1. LOG
             * 2. RECEIVE
             */
             void parse_recv_message(string recv_str){
                vector<string> str_list{};
                char delimiter = ' ';
                size_t pos = 0;
                int str_cnt = 0; // RECEIVE {} {}
                while ((pos = recv_str.find(delimiter)) != string::npos && (str_cnt++)<1) {
                    if(pos!=0){
                        str_list.push_back(recv_str.substr(0, pos));
                    }
                    recv_str.erase(0, pos + 1);
                }
                str_list.push_back(recv_str.substr(0, recv_str.length()));

                 json j = json::parse(str_list[1]);
                 json j_new;
                 if(j["msg_type"]==REQUESTVOTE){
                     //sleep(0.001);
                     //DEBUG_INFO("Receive REQUESTVOTE");
                     if(j["lastLogIndex"].get<int>()>=this->log_t.size() && !this->voted && this->term<=j["term"].get<int>()){
                         this->reply_vote(j["candidateId"]);
                     }
                 }
                 else if(j["msg_type"]==REQUESTVOTE_RPL){
                    if(j["granted"]){
                        this->nodes_info[j["voterId"]].voted_for_me = true;
                    }
                    if(this->count_votes()>=(this->nodes_info.size()/2+1) && !this->is_leader()){ // majority
                        this->become_leader();
                    }
                 }
                 else if(j["msg_type"]==APPENDENTIES){
                     if(this->term>j["term"]){
                        this->reply_append_entry(j["leaderId"], false);
                     }
                     else{ // see a larger term :  this->term <= j["term"])
                         this->voted = false;
                         Log_Entry log_entry;
                         if (this->is_leader() || this->is_candidate())
                             become_follower();
                         if(j["prevLogIndex"]!=0){ // starting from 1
                             log_entry = this->log_t.at(j["prevLogIndex"].get<int>()-1);
                             if(log_entry.term!=j["prevLogTerm"].get<int>()){
                                 this->reply_append_entry(j["leaderId"], false);
                                 return;
                             }
                             log_entry = log_t.at(j["prevLogIndex"].get<int>());
                             this->log_t.erase(this->log_t.begin() + j["prevLogIndex"].get<int>() - 1, this->log_t.end());
                         }

                         while(this->commitIndex<j["commitIndex"]){
                             log_entry = this->log_t.back();
                             this->commitIndex = (log_entry.index<j["commitIndex"].get<int>() ? log_entry.index:j["commitIndex"].get<int>());
                             this->matchIndex = (log_entry.index<j["commitIndex"].get<int>() ? log_entry.index:j["commitIndex"].get<int>());

                             this->commit_entry();
                         }

                         this->term = j["term"];
                         this->leader = j["leaderId"];

                         // append all entries to log
                         for(string entry_j_str : j["entries"]){
                             log_entry = Log_Entry(entry_j_str);
                             log_entry.term = this->term;
                             this->log_t.push_back(log_entry);
                         }

                         this->reply_append_entry(j["leaderId"], true);
                         this->reset_start_time();
                     }
                 }
                 else if(j["msg_type"]==APPENDENTIES_RPL){
                     if(j["success"]){
                         this->nodes_info[j["followerId"]].next_idx = j["matchIndex"];

                         for(int i=commitIndex+1;i<j["matchIndex"];i++){
                             this->log_t[i].node_num++;
                         }

                         while(this->commitIndex<j["matchIndex"]){
                             if(this->log_t[this->commitIndex+1].node_num>=(this->nodes_info.size()/2+1)) { // majority
                                 this->commit_entry();
                             }
                             else
                                 break;
                         }
                     }
                     else{
                         this->nodes_info[j["followerId"]].next_idx--;
                         this->send_append_entry(j["followerId"]);
                     }
                 }
             }

            /**
            * Print States to STDIN
            */
            void print_states(){

                this->latest_state = json{{"term",this->term},
                              {"state",this->state},
                              {"leader",this->leader},
                              {"commitIndex",this->commitIndex},};
                for(auto log_entry: this->log_t){
                    if(log_entry.index<=this->commitIndex){
                        this->latest_state.update(log_entry.to_json());
                    }
                }
                json j_diff = json::diff(this->history_state,this->latest_state);
                json j_add = {};
                for(auto ja:j_diff){
                    if(ja["op"]=="replace" || ja["op"]=="add"){
//                        DEBUG_INFO(to_string(ja["op"]));
//                        DEBUG_INFO(to_string(ja["path"]));
//                        DEBUG_INFO(to_string(ja["value"]));
                        j_add[ja["path"].get<string>().substr(1,ja["path"].get<string>().size())]=ja["value"];
                    }
                }

                this->history_state = this->latest_state;
                framework::state(j_add);
            }

            /**
            * Commit entry to "State Machine" (STDIN)
            */
            void commit_entry(){
                Log_Entry log_entry = this->log_t[this->commitIndex];
                this->commitIndex++; // committed index
                framework::commit(log_entry.index,log_entry.command);
            }

    };

}

deque<pair<string,string>> in_queue;
mutex in_queue_mtx;
void parse_lines(){
    // string to string pair
    string message;
    while(getline(cin, message))
    {
        vector<string> str_list{};
        char delimiter = ' ';
        string json_str;
        size_t pos = 0;

        int str_cnt = 0; // RECEIVE {} {}
        while ((pos = message.find(delimiter)) != string::npos && (str_cnt++)<1) {
            if(pos!=0){
                str_list.push_back(message.substr(0, pos));
            }
            message.erase(0, pos + 1);
        }
        str_list.push_back(message.substr(0, message.length()));

        if(str_list[0]=="RECEIVE" || str_list[0]=="LOG")
        {
            in_queue_mtx.lock();
            in_queue.emplace_back(str_list[0],str_list[1]);
            in_queue_mtx.unlock();
        }

        sleep(0.0001);
    }
}

// .\Raft <the identity of the current Raft node> < the number of nodes >
int main(int argc, char const *argv[]) {

    int node_id;
    int num_nodes;
    if(argc==3){
        node_id = stoi(argv[1]);
        num_nodes = stoi(argv[2]);
    }
    else{
        return -1;
    }
    sleep(0.001*node_id); // prevent different processes starting together
    srand(chrono::duration_cast<chrono::nanoseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count());

    raft::Raft raft(num_nodes, node_id);
    thread recv_thread(parse_lines);
    recv_thread.detach();

    // timeout
    while(true)
    {
        while(!in_queue.empty()){
            in_queue_mtx.lock();
            pair<string,string> recv_queue_front = in_queue.front();
            in_queue_mtx.unlock();


            if(recv_queue_front.first=="RECEIVE"){
                raft.parse_recv_message(recv_queue_front.second);
            }
            else if(recv_queue_front.first=="LOG"){
                raft.append_log(recv_queue_front.second);
                //DEBUG_INFO("LOG!");
            }
            else{
                cerr << "message parsing error" << ", get:\n " << endl;
                //DEBUG_INFO("message parsing error");
            }

            in_queue_mtx.lock();
            in_queue.pop_front();
            in_queue_mtx.unlock();
            raft.print_states();
        }

        if(raft.is_follower() && raft.is_time_out()){
            // DEBUG_INFO("TIMEOUT!!!!");
            raft.become_candidate();
        }

        if(raft.is_leader()){
            raft.heartbeat();
        }
    }
}

//int main(){
//    json latest_state = json{{"term", 0},
//                              {"state", "fasfs"},
//                              {"leader", 0},
//                              {"commitIndex",0},};
//    json history_state ={{"term", 0},};
//    json j_diff = json::diff(history_state,latest_state);
//    json j_add = {};
//    for(auto ja:j_diff){
//        if(ja["op"]=="replace" || ja["op"]=="add"){
//            DEBUG_INFO(to_string(ja["op"]));
//            DEBUG_INFO(to_string(ja["path"]));
//            DEBUG_INFO(to_string(ja["value"]));
//            // j_add.update(ja["value"]);
//        }
//    }
//}