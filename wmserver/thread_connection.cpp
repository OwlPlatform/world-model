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

#include "thread_connection.hpp"

#include <algorithm>
#include <iostream>
#include <chrono>

using namespace std::chrono;

//An internal list of connections
//std::list<ThreadConnection::ConnectionProcess> ThreadConnection::connections;
std::list<ThreadConnection*> ThreadConnection::connections;
//Locked before accessing the connections list
std::mutex ThreadConnection::tc_mutex;

//Removes any finished connections from the connections list.
void ThreadConnection::cleanFinished() {
  //Lock the connection mutex and sweep through the list, removing any finished connections.
  std::unique_lock<std::mutex> lck(tc_mutex);
  auto I = connections.begin();
  while (I != connections.end()) {
    //First time out the connection if it is stale.
    ThreadConnection* tc = *I;
    if (time(NULL) - std::max(tc->last_activity, tc->last_sent) > tc->timeout) {
      std::cerr<<"Timing out connection to "<<tc->sock.ip_address()<<'\n';
      //Interrupt the thread and remove this instance from the list
      tc->interrupt();
    }

    //Make sure that the thread has completed after setting finished to true
    if (tc->finished) {
      std::cerr<<"Erasing finished connection from thread list.\n";
      delete tc;
      I = connections.erase(I);
    }
    else {
      ++I;
    }
  }
}

void ThreadConnection::forEach(std::function<void(ThreadConnection*)> f) {
  std::unique_lock<std::mutex> lck(tc_mutex);
  std::for_each(connections.begin(), connections.end(), f);
}

void ThreadConnection::innerRun() {
  std::cerr<<"Running connection from "<<this->sock.ip_address()<<':'<<this->sock.port()<<'\n';
  try {
    this->run();
  }
  catch (std::system_error& err) {
    std::cerr<<"Thread connection dying with runtime error: "<<err.code().message()<<
      " in connection to "<<this->sock.ip_address()<<':'<<this->sock.port()<<'\n';
  }
  catch (std::runtime_error& err) {
    std::cerr<<"Thread connection dying with system error: "<<err.what()<<
      " in connection to "<<this->sock.ip_address()<<':'<<this->sock.port()<<'\n';
  }
  std::cerr<<"Thread connection thread is finished.\n";
  //Notify that this thread has completed, but protect the notification so that
  //memory isn't destroyed before the function can return.
  //TODO FIXME Use notify_all_at_thread_exit when it becomes available in standard compilers.
  finished = true;
}

///Timeout defaults to 60 seconds
ThreadConnection::ThreadConnection(ClientSocket&& ref_sock, time_t timeout) : sock(std::forward<ClientSocket>(ref_sock)), timeout(timeout){
	//Initialize activity timers to the current time
  last_activity = time(NULL);
  last_sent = time(NULL);
  finished = false;
};

void ThreadConnection::setActive() {
  last_activity = time(NULL);
}

time_t ThreadConnection::lastActive() {
  return last_activity;
}

time_t ThreadConnection::lastSentTo() {
  return last_sent;
}

ssize_t ThreadConnection::receive(std::vector<unsigned char>& buff) {
  ssize_t size = sock.receive(buff);
  last_activity = time(NULL);
  return size;
}
void ThreadConnection::send(const std::vector<unsigned char>& buff) {
  sock.send(buff);
	last_sent = time(NULL);
}
ClientSocket& ThreadConnection::sockRef() {
  return sock;
}

void ThreadConnection::makeNewConnection(ClientSocket&& sock, std::function<ThreadConnection* (ClientSocket&& sock)> fun) {
  if (sock.ip_address() != "") {
    std::cerr<<"Got a connection from "<<sock.ip_address()<<".\n";
  }
  if (sock) {
    //Make a new thread connection, giving it control over the socket's memory
    ThreadConnection* tc = fun(std::forward<ClientSocket>(sock));
    std::cerr<<"Starting connection.\n";
    std::thread(std::mem_fun(&ThreadConnection::innerRun), tc).detach();
    std::unique_lock<std::mutex> lck(tc_mutex);
    //Add this new connection to the solver connection list
    connections.push_front(tc);
  }
}

