/*
 * Copyright (c) 2012 Bernhard Firner and Rutgers University
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

/*******************************************************************************
 * Owl World Model
 * Listens for incoming connections from solvers and clients.
 * Stores data in a sqlite3 database (world_model.db) by default and
 * supports all of the world model messages from world_protocol.hpp
 * if USE_MYSQL is defined then msyql/mariadb is used rather than sqlite3.
 *
 * @author Bernhard Firner
 ******************************************************************************/

//For access control
#include <mutex>

#include <owl/netbuffer.hpp>
#include <owl/simple_sockets.hpp>
#include <owl/temporarily_unavailable.hpp>

//For a multithreaded network server
#include <thread>
#include <sys/types.h>
#include <sys/socket.h>

#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <stdio.h>
#include <sstream>
#include <string>
#include <set>
#include <utility>

//Handle interrupt signals to exit cleanly.
#include <signal.h>

//Sleep commands
#include <time.h>
#include <unistd.h>

//World model behavior
#include <world_model.hpp>

//For database access
#ifndef USE_MYSQL
#include <sqlite3.h>
#include <sqlite3_world_model.hpp>
#else
#include <mysql_world_model.hpp>
//For config file reading:
#include <fstream>
#include <limits>
#endif

#include <owl/world_model_protocol.hpp>
using namespace world_model;

#include "request_state.hpp"
#include "thread_connection.hpp"
#include <owl/message_receiver.hpp>

#include "repository_version.h"

using std::map;
using std::pair;
using std::string;
using std::u16string;
using std::vector;

//Global variable for the signal handler.
bool killed = false;
//Signal handler.
void handler(int signal) {
  psignal( signal, "Received signal ");
  if (killed) {
    std::cerr<<"Aborting.\n";
    // This is the second time we've received the interrupt, so just exit.
    exit(-1);
  }
  std::cerr<<"Shutting down...\n";
  killed = true;
}

#define DEBUG

struct Debug {
};
template<typename T>
Debug& operator<<(Debug& dbg, T arg) {
  //Only print if DEBUG was defined during compilation
#ifdef DEBUG
  std::cerr<<arg;
#else
  //Stop unused argument warnings
  T silence = arg;
  silence = T();
#endif
  return dbg;
}

//Store the URIs of requests for on demand data.
//The solvers will poll this information to see if they should turn
//their on demand data on or off and to check which URIs on demand
//data should be generated for.
//The variable @od_req_counts is a map from currently available
//on demand data types to requests for that data.
//The variable @on_demand_lock should be locked before modifying
//@od_req_counts.
std::map<std::u16string, std::multiset<u16string>> od_req_counts;
std::mutex on_demand_lock;

/**
 * Clients connected to the world model can make requests for data.
 * Before data is sent to clients the names of origins and attributes
 * are first aliased in a message that is sent to the client.
 * If clients make requests for on demand data then that on demand
 * data is turned on from whatever solvers provide it.
 */
class ClientConnection : public ThreadConnection {
  private:

    Debug debug;
    bool interrupted;
    MessageReceiver client_server;

    WorldModel& wm;

    //Alias mappings for this client connection
    std::map<uint32_t, std::u16string> solution_types;
    //Alias from name to number for this client.
    std::map<std::u16string, uint32_t> solution_aliases;
    std::map<std::u16string, uint32_t> origin_aliases;
    /**
     * Remember which on demand types were requested so that
     * the requests can be removed when this connection closes.
     * A map from attributes to URIs
     */
    std::map<u16string, std::set<u16string>> requested_on_demands;
    //Remember the state of streaming requests
    vector<RequestState> streaming_requests;
    //Lock the stremaing_requests vector so that we can use a separate
    //thread to handle streaming requests
    std::mutex stream_request_mutex;
    std::thread streaming_thread;
    bool stream_thread_started;
    //Lock the tx_mutex before calling send()
    std::mutex tx_mutex;
    //Preference levels for different solutions and the highest scores
    //for different URI/Attribute pairs
    std::map<u16string, int32_t> preference_levels;
    std::map<std::pair<URI, URI>, uint32_t> highest_score;

    void streaming_function() {
      stream_thread_started = true;
      grail_time next_service = getGRAILTime();
      while (not interrupted) {
        try {
          //Remember the time we started servicing this loop to save function calls
          grail_time cur_time = getGRAILTime();
          {
            std::unique_lock<std::mutex> stream_lock(stream_request_mutex);
            //Handle streaming data - see if any stream needs new data.
            for (auto sr = streaming_requests.begin(); sr != streaming_requests.end(); ++sr) {
              //Update this streaming request if it is time to update it
              if (sr->last_serviced + sr->interval < cur_time) {
                //Enable any newly matching on demand attributes
                //TODO FIXME This is inefficient -- should be checked only when
                //new on demand data appears.
                for (auto attr = sr->desired_attributes.begin(); attr != sr->desired_attributes.end(); ++attr) {
                  std::unique_lock<std::mutex> lck(on_demand_lock);
                  //If this has not been requested yet and the attribute name now
                  //appears in the od_req_counts map then make a new request.
                  if ((requested_on_demands.end() == requested_on_demands.find(*attr) or
                        0 == requested_on_demands[*attr].count(sr->search_uri)) and
                      od_req_counts.end() != od_req_counts.find(*attr)) {
                    debug<<"Adding on_demand request for attribute "<<std::string(attr->begin(), attr->end())<<
                      " with expression "<<std::string(sr->search_uri.begin(), sr->search_uri.end())<<"\n";
                    od_req_counts[*attr].insert(sr->search_uri);
                    requested_on_demands[*attr].insert(sr->search_uri);
                  }
                }
                vector<AliasedWorldData> aws = updateStreamRequest(*sr);
                for (auto aw = aws.begin(); aw != aws.end(); ++aw) {
                  //Don't bother sending a message if there aren't any updated
                  //attributes.
                  if (not aw->attributes.empty()) {
                    try {
                      std::unique_lock<std::mutex> tx_lock(tx_mutex);
                      send(client::makeDataMessage(*aw, sr->ticket_number));
                    } catch (temporarily_unavailable& err) {
                      //If this is temporary then just wait a small amount (100 milliseconds)
                      std::cerr<<"Socket temporarily not available handling stream request, 100 microseconds seconds.\n";
                      usleep(100);
                    }
                    //Delay a small amount between messages to avoid filling the network buffer.
                    usleep(10);
                  }
                }
              }
              //Remember when this should be serviced
              //Set next_service to the nearest service time
              else {
                if (sr->last_serviced + sr->interval - cur_time < next_service) {
                  next_service = sr->last_serviced + sr->interval - cur_time;
                }
              }
            }
            //Release the lock on the streaming requests (stream_lock) through RIAA
          }
          //Wait for the next service time, or a minimum of 10 microseconds and a maximum
          //of 10000 microseconds (10 milliseconds)
          if (next_service == 0) {
            usleep(10);
          }
          else if (next_service > 10) {
            usleep(10000);
          }
          else {
            usleep(next_service*1000);
          }
        } catch (std::exception& err) {
          std::cerr<<"Solver thread error in streaming thread: "<<err.what()<<'\n';
          interrupted = true;
          return;
        }
      }
    }

  public:
    static int total_connections;

    ClientConnection (ClientSocket&& csock, WorldModel& wm) :
      ThreadConnection(std::forward<ClientSocket>(csock), 60), client_server(sockRef()), wm(wm) {
      ++total_connections;
      interrupted = false;
      stream_thread_started = false;

      std::cerr<<"Opening a new client->world model connection. There are "<<
        total_connections<<" open client connections.\n";
      std::cerr<<"Client connection is from IP "<<sockRef().ip_address()<<'\n';
    }

    ~ClientConnection() {
      std::cerr<<"Client connection closing.\n";
      interrupted = true;
      //Turn off streaming requests for on demand types
      for (auto rt = requested_on_demands.begin(); rt != requested_on_demands.end(); ++rt) {
        for (auto uri = rt->second.begin(); uri != rt->second.end(); ++uri) {
          //Lock the on demand request mutex and remove these requests
          std::unique_lock<std::mutex> lck(on_demand_lock);
          if (od_req_counts.end() != od_req_counts.find(rt->first)) {
            std::multiset<u16string>& trc = od_req_counts[rt->first];
            auto req_iterator = trc.find(*uri);
            if (req_iterator != trc.end()) {
              debug<<"Decrementing on demand request count for URI "<<std::string(uri->begin(), uri->end())<<", attribute "<<std::string(rt->first.begin(), rt->first.end())<<"\n";
              trc.erase(req_iterator);
            }
          }
        }
      }
      if (stream_thread_started) {
        std::cerr<<"Waiting for streaming thread to finish...\n";
        streaming_thread.join();
      }
      --total_connections;
      std::cerr<<"Client connection closed. ("<<total_connections<<" connections remaining)\n";
    }

    //Interrupt this thread and cause it to stop.
    void interrupt() {
      interrupted = true;
      std::cerr<<"Interrupting client thread.\n";
    }

    vector<AliasedWorldData> worldStateToAliasedData(WorldModel::world_state& ws) {
      vector<AliasedWorldData> awds;
      vector<client::AliasType> new_names;
      vector<client::AliasType> new_origins;
      for (auto W = ws.begin(); W != ws.end(); ++W) {
        AliasedWorldData awd;
        awd.object_uri = W->first;
        for (auto attr = W->second.begin(); attr != W->second.end(); ++attr) {
          if (solution_aliases.find(attr->name) == solution_aliases.end()) {
            uint32_t next = solution_aliases.size()+1;
            solution_aliases[attr->name] = next;
            new_names.push_back(client::AliasType{next, attr->name});
          }
          if (origin_aliases.find(attr->origin) == origin_aliases.end()) {
            uint32_t next = origin_aliases.size()+1;
            origin_aliases[attr->origin] = next;
            new_origins.push_back(client::AliasType{next, attr->origin});
          }
          awd.attributes.push_back(
              AliasedAttribute{solution_aliases[attr->name], attr->creation_date,
                               attr->expiration_date, origin_aliases[attr->origin], attr->data});
        }
        awds.push_back(awd);
      }
      //Before returning send a message to the client with the aliases of any
      //new attribute names or origins
      if (not new_names.empty()) {
        size_t tries = 0;
        bool success = false;
        while (not success and tries < 10) {
          try {
            ++tries;
            std::unique_lock<std::mutex> tx_lock(tx_mutex);
            send(makeAttrAliasMsg(new_names));
            success = true;
          } catch (temporarily_unavailable& err) {
            //If this is temporary then just wait a small amount (100 milliseconds)
            usleep(100);
          }
        }
        if (not success) {
          std::cerr<<"Error sending new type messages to client (socket unavailable).\n";
          //TODO FIXME Disconnect
        }
      }
      if (not new_origins.empty()) {
        size_t tries = 0;
        bool success = false;
        while (not success and tries < 10) {
          try {
            ++tries;
            std::unique_lock<std::mutex> tx_lock(tx_mutex);
            send(makeOriginAliasMsg(new_origins));
            success = true;
          } catch (temporarily_unavailable& err) {
            //If this is temporary then just wait a small amount (100 milliseconds)
            usleep(100);
          }
        }
        if (not success) {
          std::cerr<<"Error sending new orign alias messages to client (socket unavailable).\n";
          //TODO FIXME Disconnect
        }
      }
      return awds;
    }

    void applyOriginPreferences(WorldModel::world_state& ws) {
      //If the user has no preferences just return
      if (preference_levels.empty()) {
        return;
      }
      std::map<u16string, int32_t> preference_levels;
      //Make one pass through the current state to record the highest
      //preference level of each unique attribute (name, origin) pair
      //TODO FIXME Update the highest_score values when something is expired or deleted
      for (auto I = ws.begin(); I != ws.end(); ++I) {
        std::vector<world_model::Attribute>& attributes = I->second;
        for (auto attr = attributes.begin(); attr != attributes.end(); ++attr) {
          int32_t preference = 1;
          if (preference_levels.find(attr->origin) != preference_levels.end()) {
            preference = preference_levels[attr->origin];
          }
          else {
            preference_levels[attr->origin] = 1;
          }
          auto uri_attr = make_pair(I->first, attr->name);
          auto J = highest_score.find(uri_attr);
          if (J == highest_score.end()) {
            highest_score.insert(make_pair(uri_attr, preference));
          }
          //Replace if higher than the previous best
          else if (J->second < preference) {
            highest_score.insert(make_pair(uri_attr, preference));
          }
        }
        //Now remove any items that are less than the desired level of preference
        attributes.erase(std::remove_if(attributes.begin(), attributes.end(),
              [&](world_model::Attribute& attr) {
              return preference_levels[attr.origin] < 0 or
              preference_levels[attr.origin] < highest_score[make_pair(I->first, attr.name)];}),
            attributes.end());
      }
    }

    //Update a stream request with the data from the world model and return
    //the aliased world data that should be sent to the client to represent
    //the changes in the world model.
    vector<AliasedWorldData> updateStreamRequest(RequestState& rs) {
      WorldModel::world_state changed = rs.sq.getData();
      //Apply user-supplied preference levels here
      applyOriginPreferences(changed);
      rs.last_serviced = getGRAILTime();
      return worldStateToAliasedData(changed);
    }

    //Go through the world model/client protocol and send data to the client.
    void run() {

      try {
        //Try to get the handshake message from the client
        {
          std::vector<unsigned char> handshake = client::makeHandshakeMsg();

          //Send the handshake message
          send(handshake);
          std::vector<unsigned char> raw_message(handshake.size());
          ssize_t length = receive(raw_message);
          //While this thread is not interrupted, no data has been received
          //keep trying to get the handshake.
          //If this is a nonblocking socket then wait for it to find some data.
          while (not interrupted and
                 -1 == length and
                 (EAGAIN == errno or
                  EWOULDBLOCK == errno)) {
            usleep(1000);
            length = receive(raw_message);
          }

          //Check if the handshake message failed
          if (not (length == handshake.size() and
                std::equal(handshake.begin(), handshake.end(), raw_message.begin()) )) {
            std::cerr<<"Failure during client handshake. Received bytes were:\n";
            std::for_each(raw_message.begin(), raw_message.end(), [&](unsigned char c){ std::cerr<<'\t'<<(uint32_t)c;});
            return;
          }
        }

        //The world model connections will run as single threads even though
        //communication is bidirectional. Splitting into two threads does not
        //increase throughput greatly since any messages sent to the client
        //(the attribute alias, origin alias, request complete, and data response)
        //are sent in response to a client message.
        //Keeping the up and down sides of the connection separated complicates
        //data management so a single thread is best.

        //Listen for type request messages. Set an internal variable, and
        //then call the streamData function to keep solutions streaming.
        while (not interrupted) {

          if (client_server.messageAvailable(interrupted)) {
            std::vector<unsigned char> raw_message = client_server.getNextMessage(interrupted);

            setActive();

            //Handle the message according to its message type.
            client::MessageID message_type = (client::MessageID)raw_message[4];

            if ( client::MessageID::keep_alive == message_type ) {
              setActive();
            }
            else if ( client::MessageID::snapshot_request == message_type ) {
              client::Request request;
              uint32_t ticket;
              std::tie(request, ticket) = client::decodeSnapshotRequest(raw_message);
              //TODO FIXME The protocol needs to allow for requests with and without data.
              debug<<"Received a snapshot request message for URI "<<
                std::string(request.object_uri.begin(), request.object_uri.end())<<
                " with "<<request.attributes.size()<< " attributes.\n";
              WorldModel::world_state ws;
              //If the begin and end time are both zero then this is for a current snapshot.
              if (request.start == 0 and request.stop_period == 0) {
                debug<<"Snapshot is for the current state.\n";
                ws = wm.currentSnapshot(request.object_uri, request.attributes, true);
              }
              else {
                debug<<"Snapshot is historic for the time range "<<
                  request.start<<" to "<<request.stop_period<<".\n";
                ws = wm.historicSnapshot(request.object_uri, request.attributes, request.start, request.stop_period);
              }
              vector<AliasedWorldData> aws = worldStateToAliasedData(ws);
              for (auto aw = aws.begin(); aw != aws.end(); ++aw) {
                debug<<"Returning URI "<<std::string(aw->object_uri.begin(), aw->object_uri.end())<<
                  " with "<<aw->attributes.size()<<" attributes\n";
                try {
                  Buffer buff = client::makeDataMessage(*aw, ticket);
                  if (buff.size() > 0) {
                    std::unique_lock<std::mutex> tx_lock(tx_mutex);
                    send(buff);
                  }
                  else {
                    std::cerr<<"Error creating data message! Not sending to the client.\n";
                  }
                  //Delay a small amount between messages to avoid filling the network buffer.
                  usleep(1500);
                } catch (temporarily_unavailable& err) {
                  //If this is temporary then just wait a small amount (100 milliseconds)
                  std::cerr<<"Socket temporarily not available during snapshot request, waiting 0.1 seconds.\n";
                  usleep(100);
                }
              }
              //Send the request complete message after all objects are sent
              std::unique_lock<std::mutex> tx_lock(tx_mutex);
              send(client::makeRequestComplete(ticket));
            }
            else if ( client::MessageID::range_request == message_type ) {
              debug<<"Received a range request message.\n";
              client::Request request;
              uint32_t ticket;
              std::tie(request, ticket) = client::decodeRangeRequest(raw_message);
              WorldModel::world_state ws = wm.historicDataInRange(request.object_uri, request.attributes, request.start, request.stop_period);
              vector<AliasedWorldData> aws = worldStateToAliasedData(ws);
              for (auto aw = aws.begin(); aw != aws.end(); ++aw) {
                try {
                  std::unique_lock<std::mutex> tx_lock(tx_mutex);
                  send(client::makeDataMessage(*aw, ticket));
                } catch (temporarily_unavailable& err) {
                  //If this is temporary then just wait a small amount (100 milliseconds)
                  std::cerr<<"Socket temporarily not available during range request, waiting 100 microseconds.\n";
                  usleep(100);
                }
                //Delay a small amount between messages to avoid filling the network buffer.
                usleep(10);
              }
              //Send the request complete message after all objects are sent
              std::unique_lock<std::mutex> tx_lock(tx_mutex);
              send(client::makeRequestComplete(ticket));
            }
            else if ( client::MessageID::stream_request == message_type ) {
              client::Request request;
              uint32_t ticket;
              std::tie(request, ticket) = client::decodeStreamRequest(raw_message);
              //Remove any existing requests with this ticket number
              {
                std::unique_lock<std::mutex> stream_lock(stream_request_mutex);
                //TODO FIXME This does not properly update request counts for different attributes.
                streaming_requests.erase(std::remove_if(streaming_requests.begin(), streaming_requests.end(),
                      [&](RequestState& rs) {return rs.ticket_number == ticket;}), streaming_requests.end());
              }
              //Create a new request state to handle this new stream request.
              std::cerr<<"In world model server period is "<<request.stop_period<<'\n';
              RequestState rs(request.stop_period, request.object_uri,
                  request.attributes, ticket, wm.requestStandingQuery(request.object_uri, request.attributes));
              //TODO FIXME Either a bug in this code or a bug in gcc corrupts the
              //value of rs.interval so we reassign it here.
              rs.interval = request.stop_period;
              debug<<"Received a stream request message with expression "<<std::string(rs.search_uri.begin(), rs.search_uri.end())<<
                " and "<<request.attributes.size()<<" attributes with interval "<<rs.interval<<".\n";
              //Drop connections that request negative times as they are invalid.
              if (rs.interval < 0) {
                throw std::runtime_error("Subscription received with negative interval.");
              }
              //Check the attributes to see if any are on demand types.
              for (auto attr = request.attributes.begin(); attr != request.attributes.end(); ++attr) {
                debug<<"Checking if "<<std::string(attr->begin(), attr->end())<<" is a on_demand type.\n";
                std::unique_lock<std::mutex> lck(on_demand_lock);
                if (od_req_counts.end() != od_req_counts.find(*attr)) {
                  debug<<"Adding on demand request count for attribute "<<
                    std::string(attr->begin(), attr->end())<<" with URI expression "<<
                    std::string(rs.search_uri.begin(), rs.search_uri.end())<<"\n";
                  od_req_counts[*attr].insert(rs.search_uri);
                  requested_on_demands[*attr].insert(rs.search_uri);
                }
              }

              vector<AliasedWorldData> aws = updateStreamRequest(rs);
              for (auto aw = aws.begin(); aw != aws.end(); ++aw) {
                //Too noisy to print this out.
                //std::cerr<<"Sending updated stream data\n";
                try {
                  std::unique_lock<std::mutex> tx_lock(tx_mutex);
                  send(client::makeDataMessage(*aw, ticket));
                } catch (temporarily_unavailable& err) {
                  //If this is temporary then just wait a small amount (100 milliseconds)
                  std::cerr<<"Socket temporarily not available handling stream request, waiting 100 microseconds.\n";
                  usleep(100);
                }
                //Delay a small amount between messages to avoid filling the network buffer.
                usleep(10);
              }
              //Add this to the list of streaming requests
              {
                std::unique_lock<std::mutex> stream_lock(stream_request_mutex);
                streaming_requests.push_back(std::move(rs));
                if (not stream_thread_started) {
                  stream_thread_started = true;
                  streaming_thread = std::thread(std::mem_fun(&ClientConnection::streaming_function), this);
                }
              }
            }
            else if ( client::MessageID::cancel_request == message_type ) {
              uint32_t ticket = client::decodeCancelRequest(raw_message);
              debug<<"Received a cancel request\n";
              //Cancel the stream corresponding to this request number.
              //Lock the stream request list first.
              std::unique_lock<std::mutex> stream_lock(stream_request_mutex);
              for (auto I = streaming_requests.begin(); I != streaming_requests.end(); ++I) {
                if (I->ticket_number == ticket) {
                  auto sr = std::find_if(streaming_requests.begin(), streaming_requests.end(),
                      [&](RequestState& rs) {return rs.ticket_number == ticket;});
                  if (sr != streaming_requests.end()) {
                    //Need to cancel on demand count from this request
                    //The request state requested a URI matching sr->search_uri
                    //and attributes matching sr->desired_attributes
                    for (auto attr = sr->desired_attributes.begin(); attr != sr->desired_attributes.end(); ++attr) {
                      //If this was requested cancel the request from
                      //the od_req_counts map.
                      if (requested_on_demands.end() != requested_on_demands.find(*attr) and
                        0 != requested_on_demands[*attr].count(sr->search_uri)) {
                        std::set<u16string>& rod = requested_on_demands[*attr];

                        //Verify this attribute was indeed requested in the od_req_counts map
                        {
                          std::unique_lock<std::mutex> lck(on_demand_lock);
                          auto req_iter = od_req_counts.find(*attr);
                          if ( od_req_counts.end() != req_iter) {
                            //And remove it
                            std::multiset<u16string>& req = req_iter->second;
                            req.erase(req.find(sr->search_uri));
                          }
                        }

                        //Mark this as not requested by erasing one entry of
                        //the search URI from the requested on demands map
                        rod.erase(rod.find(sr->search_uri));
                      }
                    }
                  }
                  //Remove any request with this ticket number
                  streaming_requests.erase(std::remove_if(streaming_requests.begin(), streaming_requests.end(),
                        [&](RequestState& rs) {return rs.ticket_number == ticket;}), streaming_requests.end());
                  std::unique_lock<std::mutex> tx_lock(tx_mutex);
                  //Send the request complete message after canceling
                  send(client::makeRequestComplete(ticket));
                  break;
                }
              }
            }
            else if ( client::MessageID::uri_search == message_type ) {
              URI search_uri = client::decodeURISearch(raw_message);
              debug<<"Received a uri search message for string: '"<<std::string(search_uri.begin(), search_uri.end())<<"'.\n";
              std::vector<world_model::URI> uris = wm.searchURI(search_uri);
              std::unique_lock<std::mutex> tx_lock(tx_mutex);
              send(client::makeURISearchResponse(uris));
            }
            else if ( client::MessageID::origin_preference == message_type ) {
              debug<<"Received an origin preference message\n";
              std::vector<std::pair<std::u16string, int32_t>> preferences = client::decodeOriginPreference(raw_message);
              for (auto I = preferences.begin(); I != preferences.end(); ++I) {
                preference_levels.insert(*I);
              }
            }
          }
          //Send a keep alive message if the connection has been idle
          //for half of the time out time.
          if (time(NULL) - lastSentTo() > timeout / 2.0) {
            std::unique_lock<std::mutex> tx_lock(tx_mutex);
            send(client::makeKeepAlive());
          }
        }
      } catch (std::exception& err) {
        std::cerr<<"Solver thread error: "<<err.what()<<'\n';
        interrupted = true;
        return;
      }
    }
};

//A class to handle connections from solvers to the world model
class SolverConnection : public ThreadConnection {
  private:
    Debug debug;
    bool interrupted;
    WorldModel& wm;
    //Origin string for this solver (provided in the type alias message)
    u16string origin;

  public:
    static int total_connections;
    std::map<uint32_t, std::u16string> solution_types;
    std::map<std::u16string, uint32_t> solution_aliases;
    //The on demand types (specified by name and origin) of this connection
    //and their streaming / not streaming status. Default is off.
    //The key value contains the on demand attribute name and the value
    //is a set that indicates which URI expressions are being sent.
    std::map<std::u16string, std::set<std::u16string>> on_demand_status;
    MessageReceiver solver_server;
    SolverConnection (ClientSocket&& csock, WorldModel& wm) : ThreadConnection(std::forward<ClientSocket>(csock)), wm(wm), solver_server(sockRef()) {
      std::cerr<<"Opening a new solver->world model connection. There are "<<
        SolverConnection::total_connections<<" solver connections.\n";
      std::cerr<<"Solver connection is from IP "<<sockRef().ip_address()<<'\n';
      ++total_connections;
      interrupted = false;
    }

    ~SolverConnection() {
      std::cerr<<"Solver connection closing.\n";
      --total_connections;
      std::cerr<<"Solver connection closed. ("<<SolverConnection::total_connections<<" connections remaining)\n";
    }

    //Interrupt this thread and cause it to stop.
    void interrupt() {
      std::cerr<<"Interrupting solver thread.\n";
      interrupted = true;
    }

    //A function to handle the connection in a thread.
    void run() {

      try {
        //Try to get the handshake message from the solver
        {
          std::vector<unsigned char> handshake = solver::makeHandshakeMsg();

          //Send the handshake message
          send(handshake);
          std::vector<unsigned char> raw_message(handshake.size());
          ssize_t length = receive(raw_message);
          //While this thread is not interrupted, no data has been received
          //keep trying to get the handshake.
          //If this is a nonblocking socket then wait for it to find some data.
          while (not interrupted and
                 -1 == length and
                 (EAGAIN == errno or
                  EWOULDBLOCK == errno)) {
            usleep(1000);
            length = receive(raw_message);
          }

          //Check if the handshake message failed
          if (not (length == handshake.size() and
                std::equal(handshake.begin(), handshake.end(), raw_message.begin()) )) {
            std::cerr<<"Failure during solver handshake. Received bytes were:\n";
            std::for_each(raw_message.begin(), raw_message.end(), [&](unsigned char c){ std::cerr<<'\t'<<(uint32_t)c;});
            return;
          }
        }
        
        setActive();

        while (not interrupted) {
          if (solver_server.messageAvailable(interrupted)) {
            std::cerr<<"Trying to get available packet\n";
            std::vector<unsigned char> raw_message = solver_server.getNextMessage(interrupted);

            setActive();

            //Handle the message according to its message type.
            solver::MessageID message_type = (solver::MessageID)raw_message[4];
            debug<<"Message id is "<<(uint32_t)raw_message[4]<<'\n';

            if ( solver::MessageID::keep_alive == message_type ) {
              std::cerr<<"Received keep alive from origin "<<std::string(origin.begin(), origin.end())<<'\n';
              setActive();
            }
            else if ( solver::MessageID::type_announce == message_type ) {
              debug<<"Received a type announcement message.\n";
              vector<solver::AliasType> aliases;
              pair<vector<solver::AliasType>&, u16string&>{aliases, origin} = solver::decodeTypeAnnounceMsg(raw_message);

              //Store the new attributes in a set so that standing queries can
              //be updated with new origin->attribute information.
              std::set<std::u16string> new_attributes;
              for (auto type_alias = aliases.begin(); type_alias != aliases.end(); ++type_alias) {
                //OnDemand types start off not sending data
                if (type_alias->on_demand) {
                  on_demand_status[type_alias->type] = std::set<u16string>();
                  {
                    //Zero the on demand request count for this on demand if it is new
                    std::unique_lock<std::mutex> lck(on_demand_lock);
                    if (od_req_counts.find(type_alias->type) == od_req_counts.end()) {
                      od_req_counts[type_alias->type] = std::multiset<u16string>();
                    }
                  }
                  //Register this as an on demand type with the world model
                  wm.registerTransient(type_alias->type, origin);
                }
                debug<<"Type "<<std::string(type_alias->type.begin(), type_alias->type.end())<<
                  " aliased to "<<type_alias->alias<<'\n';
                solution_types[type_alias->alias] = type_alias->type;
                solution_aliases[type_alias->type] = type_alias->alias;
                new_attributes.insert(type_alias->type);
              }
              //Now update the standing query origin to attribute map
              StandingQuery::addOriginAttributes(origin, new_attributes);
            }
            else if ( solver::MessageID::solver_data == message_type ) {
              debug<<"Received a solver data message.\n";
              //Insert this new data into the world model
              bool create_uris = false;
              std::vector<solver::SolutionData> solutions;
              std::tie(create_uris, solutions) = solver::decodeSolutionMsg(raw_message);
              map<URI, std::vector<world_model::Attribute>> new_data;
              for (auto soln = solutions.begin(); soln != solutions.end(); ++soln) {
                //Make sure that an alias for this type was received
                if (solution_types.find(soln->type_alias) != solution_types.end()) {
                  Attribute attr{solution_types[soln->type_alias], soln->time, 0, origin, soln->data};
                  new_data[soln->target].push_back(attr);
                  //Don't print anything out for on demand requests as they are quite numerous.
                  if (on_demand_status.empty() or
                      on_demand_status.end() == on_demand_status.find(solution_types[soln->type_alias])) {
                    debug<<"Inserting solution "<<
                      std::string(solution_types[soln->type_alias].begin(), solution_types[soln->type_alias].end())<<
                      " for URI "<<std::string(soln->target.begin(), soln->target.end())<<".\n";
                  }
                }
                else {
                  debug<<"No alias for this solution was received.\n";
                }
              }
              //Don't time out while pushing data
              setActive();
              vector<pair<URI, vector<Attribute>>> data_v(new_data.begin(), new_data.end());
              wm.insertData(data_v, create_uris);
            }
            else if ( solver::MessageID::create_uri == message_type ) {
              debug<<"Received a create URI message.\n";
              std::tuple<URI, grail_time, std::u16string> uri_origin = solver::decodeCreateURI(raw_message);
              wm.createURI(std::get<0>(uri_origin), std::get<2>(uri_origin), std::get<1>(uri_origin));
            }
            else if ( solver::MessageID::expire_uri == message_type ) {
              debug<<"Received an expire URI message.\n";
              std::tuple<URI, grail_time, std::u16string> uri_origin = solver::decodeExpireURI(raw_message);
              //TODO FIXME Verify the origin here
              wm.expireURI(std::get<0>(uri_origin), std::get<1>(uri_origin));
            }
            else if ( solver::MessageID::delete_uri == message_type ) {
              debug<<"Received a delete URI message.\n";
              std::pair<URI, std::u16string> uri_origin = solver::decodeDeleteURI(raw_message);
              //TODO FIXME Verify the origin here
              debug<<"Deleting URI "<<std::string(uri_origin.first.begin(), uri_origin.first.end())<<'\n';
              wm.deleteURI(uri_origin.first);
            }
            else if ( solver::MessageID::expire_attribute == message_type ) {
              debug<<"Received an expire URI attribute message.\n";
              std::tuple<URI, std::u16string, grail_time, std::u16string> uri_origin = solver::decodeExpireAttribute(raw_message);

              Attribute attr;
              attr.name = std::get<1>(uri_origin);
              attr.origin = std::get<3>(uri_origin);
              vector<Attribute> entries{attr};
              wm.expireURIAttributes(std::get<0>(uri_origin), entries, std::get<2>(uri_origin));
            }
            else if ( solver::MessageID::delete_attribute == message_type ) {
              debug<<"Received a delete URI attribute message.\n";
              std::tuple<URI, std::u16string, std::u16string> uri_origin = solver::decodeDeleteAttribute(raw_message);
              Attribute attr;
              attr.name = std::get<1>(uri_origin);
              attr.origin = std::get<2>(uri_origin);
              vector<Attribute> entries{attr};
              wm.deleteURIAttributes(std::get<0>(uri_origin), entries);
            }
          }
          else {
            //Sleep for a millisecond to wait for a new message.
            usleep(1);
          }
          //If this solver has any on demand data types check to see if their
          //on demand request status has changed.
          if (not on_demand_status.empty()) {
            std::vector<std::tuple<uint32_t, std::vector<std::u16string>>> start_aliases;
            std::vector<std::tuple<uint32_t, std::vector<std::u16string>>> stop_aliases;
            {
              std::unique_lock<std::mutex> lck(on_demand_lock);
              for (auto trans = on_demand_status.begin(); trans != on_demand_status.end(); ++trans) {
                //Check if this on demand is not being sent but was requested
                std::multiset<u16string>& uri_requests = od_req_counts[trans->first];
                auto new_req = std::make_tuple(solution_aliases[trans->first], vector<u16string>());
                auto stop_req = std::make_tuple(solution_aliases[trans->first], vector<u16string>());
                for (auto uri = uri_requests.begin(); uri != uri_requests.end(); ++uri) {
                  //Check for requests
                  if (0 == trans->second.count(*uri)) {
                    debug<<"Enabling on demand "<<std::string(trans->first.begin(), trans->first.end())<<
                      " on uri pattern "<<std::string(uri->begin(), uri->end())<<'\n';
                    trans->second.insert(*uri);
                    std::get<1>(new_req).push_back(*uri);
                  }
                }
                auto on_uri = trans->second.begin();
                while ( on_uri != trans->second.end()) {
                  //Alternatively if the on demand data is being sent but no longer
                  //needs to be turn it off
                  if (0 == uri_requests.count(*on_uri)) {
                    debug<<"Disabling on_demand "<<std::string(trans->first.begin(), trans->first.end())<<
                      " on uri pattern "<<std::string(on_uri->begin(), on_uri->end())<<'\n';
                    std::get<1>(stop_req).push_back(*on_uri);
                    on_uri = trans->second.erase(on_uri);
                  }
                  else {
                    ++on_uri;
                  }
                }
                if (not std::get<1>(new_req).empty()) {
                  start_aliases.push_back(new_req);
                }
                if (not std::get<1>(stop_req).empty()) {
                  stop_aliases.push_back(stop_req);
                }
              }
            }
            //Send these messages after releasing the lock
            if (not start_aliases.empty()) {
              send(solver::makeStartOnDemand(start_aliases));
            }
            if (not stop_aliases.empty()) {
              send(solver::makeStopOnDemand(stop_aliases));
            }
          }
          //Send a keep alive message if the connection has been idle
          //for half of the time out time.
          if (time(NULL) - lastSentTo() > timeout / 2.0) {
            send(solver::makeKeepAlive());
          }
        }
      } catch (std::exception& err) {
        std::cerr<<"Caught exception in solver connection: "<<err.what()<<"\n";
      }
    }
};

//Declarations of static class members.
int SolverConnection::total_connections = 0;
int ClientConnection::total_connections = 0;

bool client_done = false;
void clientListen(int client_port, WorldModel& wm) {
  client_done = false;
  //Set up a client server socket.
  ServerSocket ssock(AF_UNSPEC, SOCK_STREAM, SOCK_NONBLOCK, client_port);
  if (not ssock) {
    std::cerr<<"Could not make the client socket - aborting.\n";
    killed = true;
    return;
  }

  auto newClient = [&wm](ClientSocket&& cs)->ThreadConnection* {
    return new ClientConnection(std::forward<ClientSocket>(cs), wm);
  };
  while (not killed) {
    try {
      //Make these sockets nonblocking so that we can interrupt client connections
      ClientSocket cs(ssock.next(SOCK_NONBLOCK));
      if (cs) {
        ThreadConnection::makeNewConnection(std::move(cs), newClient);
      }
      usleep(10);
    } catch (std::exception& err) {
      std::cerr<<"An error occured: "<<err.what()<<'\n';
    }
  }
  client_done = true;
}

bool sweep_done = false;
//Sweeper thread that gets rid of old connections.
void sweeperThread() {
  sweep_done = false;
  while (not killed) {
    sleep(1);
    //Sweep finished threads from time to time.
    ThreadConnection::cleanFinished();
    //std::cerr<<"Sweeping - "<<ThreadConnection::total_connections<<" connections left.\n";
  }
  //Inerrupt all of the connections.
  sweep_done = true;
  return;
}

int main(int ac, char** av) {
#ifndef USE_MYSQL
  //sqlite3 world model
  if ( ac != 3 and ac != 1) {
    std::cerr<<"You must provide a port number to receive solver\n"<<
      "connections on and a port number to receive client connections on\n"<<
      "or provide no arguments and the default ports (7009 7010) will be used.\n";
    return 0;
  }

	std::cout<<"Starting sqlite3 world model\n";
	std::cout<<GIT_REPO_VERSION<<'\n';

  int solver_port = ac == 3 ? atoi(av[1]) : 7009;
  int client_port = ac == 3 ? atoi(av[2]) : 7010;
  std::cout<<"Listening for solver on port number "<<solver_port<<'\n';
  std::cout<<"Listening for client on port number "<<client_port<<'\n';

  SQLite3WorldModel wm("world_model.db");
#else

	std::cout<<"Starting mysql world model\n";
	std::cout<<GIT_REPO_VERSION<<'\n';

  //mysql world model parameters
  //All parameters will be read in from a configuration file
  std::string username;
  std::string password;
	std::string db_name;
	//Remember which options were set. If the username, password, or database are
	//not set then refuse to work.
	unsigned char options_set = 0;
	//Default to 7009 and 7010 if not specified
  int solver_port = 7009;
  int client_port = 7010;

	if (ac == 2) {
		std::string config_location(av[1]);
		std::cout<<"Reading configuration settings from "<<config_location<<'\n';
		std::ifstream in(config_location);
		unsigned int line_number = 0;
		//Keep going while there is more config information to read
		while (in) {
			++line_number;
			//Discard line on comment
			if ('#' == in.peek()) {
				in.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
			}
			//Otherwise the line isn't a comment
			//The string before the '=' character is the key, the rest of the
			//line is the value
			else {
				//Read the line into a string, split the string at the '=',
				//verify that the key is valid and store its value
				//Lines longer than 1000 characters are not valid
				std::array<char, 1000> buffer;
				std::fill(buffer.begin(), buffer.end(), 0);
				in.getline(&buffer[0], 1000);
				//Ignore empty lines
				if ('\0' != buffer.at(0)) {
					//Find the location of the '=' character
					std::array<char, 1000>::iterator eq_index = std::find(buffer.begin(), buffer.end(), '=');
					//Don't try to process a line without the '=' character
					if (eq_index == buffer.end()) {
						std::string invalid(buffer.begin(), buffer.end());
						std::cerr<<"Invalid line in config file at line number "<<line_number<<'\n';
					}
					else {
						std::string key(buffer.begin(), eq_index);
						std::string value(eq_index+1, buffer.end());
						if ("username" == key) {
							username = value;
							options_set |= 0x01;
						}
						else if ("password" == key) {
							password = value;
							options_set |= 0x02;
						}
						else if ("dbname" == key) {
							db_name = value;
							options_set |= 0x04;
						}
						else if ("solver_port" == key) {
							solver_port = std::stoi(value);
						}
						else if ("client_port" == key) {
							client_port = std::stoi(value);
						}
					}
				}
			}
		}
		//Done reading the configuration file
		in.close();
		if (0x7 != (0x7 & options_set)) {
			std::cout<<"Your configuration file must specify a username, password, "<<
				"and database name to use with mysql.\n";
			return 0;
		}
	}
	else {
		std::cout<<"Usage is: "<<av[0]<<" <configuration file>\n";
		std::cout<<"The world model defaults to ports 7009 and 7010 if none are specified.\n";
		return 0;
	}

  std::cout<<"Listening for solver on port number "<<solver_port<<'\n';
  std::cout<<"Listening for client on port number "<<client_port<<'\n';

	std::cout<<"Using db "<<db_name<<'\n';

  MysqlWorldModel wm(db_name, username, password);
#endif

  //Set up a signal handler to catch interrupt signals so we can close gracefully
  signal(SIGINT, handler);  

  //Spawn a new thread here to listen for connecting clients
  std::thread client_thread = std::thread(clientListen, client_port, std::ref(wm));
  client_thread.detach();

  //Set up a solver server socket.
  ServerSocket ssock(AF_UNSPEC, SOCK_STREAM, SOCK_NONBLOCK, solver_port);
  if (not ssock) {
    std::cerr<<"Could not make the solver socket - aborting.\n";
    return 1;
  }

  std::thread sweep_thread = std::thread(sweeperThread);
  sweep_thread.detach();

  auto newSolver = [&wm](ClientSocket&& cs)->ThreadConnection* {
    return new SolverConnection(std::forward<ClientSocket>(cs), wm);
  };
  while (not killed) {
    try {
      //Make these sockets nonblocking so that we can interrupt solver connections
      ClientSocket cs(ssock.next(SOCK_NONBLOCK));
      if (cs) {
        ThreadConnection::makeNewConnection(std::move(cs), newSolver);
      }
      usleep(10);
    } catch (std::exception& err) {
      std::cerr<<"An error occured: "<<err.what()<<'\n';
    }
  }

  std::cerr<<"Closing open sockets...\n";
  //First send them an interrupt signal
  ThreadConnection::forEach([&](ThreadConnection* tc) {
      tc->interrupt();});

  std::cerr<<"Waiting for client thread to stop...\n";
  while (not client_done) {
    usleep(100);
  }

  size_t intervals = 10;
  //Wait one second for sockets to close, but no longer.
  //No variable timing here since we want to close quickly for the user.
  for (int i = intervals; i > 0 and
                          SolverConnection::total_connections > 0 and
                          ClientConnection::total_connections > 0; --i) {
    usleep(100);
  }

  //We detached the sweep and client threads so we cannot join them here.
  std::cerr<<"Waiting for sweep thread to stop...\n";
  while (not sweep_done) {
    usleep(100);
  }

  std::cerr<<"Deleting any non-responsive sockets...\n";
  //Now close all of the sockets that are left
  ThreadConnection::forEach([&](ThreadConnection* tc) {delete tc;});
  std::cerr<<"World Model Server exiting\n";
  //TODO FIXME The std::thread class seem to be leaving some parts of itself
  //behind after being detached so the return statement here is never reached.
  //The exit statement does not clean up local storage so this is bad form.
  exit(0);
  return 0;
}


