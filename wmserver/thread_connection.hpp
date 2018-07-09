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
 * Class made to simplify having multiple threaded IP connections.
 *
 * @author Bernhard Firner
 ******************************************************************************/

#ifndef __THREAD_CONNECTION_HPP__
#define __THREAD_CONNECTION_HPP__

#include <atomic>
#include <functional>
#include <list>
#include <mutex>
#include <thread>
#include <time.h>
#include <future>
#include <utility>

#include <owl/simple_sockets.hpp>

/**
 * Threaded connection class that has functions to create new
 * threads associated with sockets.
 */
class ThreadConnection {
  private:
    //An internal list of connections
    static std::list<ThreadConnection*> connections;
    //Locked before accessing the connections list
    static std::mutex tc_mutex;

    //True once the connection can be deleted
    std::atomic_bool finished;

    //This is private so that inherited classes must be passed to the
    //makeNewConnection function to be set up with the socket.
    ClientSocket sock;

    /**
     * Makes sure that finished is set to true when run returns.
     * Returns true if the thread exits normally and false if it exits
     * due to an unhandled exception.
     */
    void innerRun();

    //Private copy semantics to retain control over sockets
    //(only one connection per socket)
    ThreadConnection& operator=(const ThreadConnection&) = delete;
    ThreadConnection(const ThreadConnection&) = delete;

    //Time of last received socket activity.
    time_t last_activity;
    //Time since the transmitted socket activity.
    time_t last_sent;

  public:
    //Maximum duration of socket inactivity before timing out the thread
    //and allowing cleanFinished to clean it away.
    //The idle time to wait before closing a connection.
    time_t timeout;

    ///Removes any finished connections from the connections list.
    static void cleanFinished();

    ///Instantiate and run a derived connection
    static void makeNewConnection(ClientSocket&& sock, std::function<ThreadConnection* (ClientSocket&& sock)> fun);

    ///Execute a function on each active ThreadConnection.
    static void forEach(std::function<void(ThreadConnection*)> f);

		/**
		 * Create a thread connection using the given socket. Timeout defaults to
		 * 60 seconds.
		 */
    ThreadConnection(ClientSocket&& ref_sock, time_t timeout = 60);

    virtual ~ThreadConnection(){};

    ///Virtual run function. Inheriting classes do stuff with the connection here.
    virtual void run() = 0;

    /**
     * An interrupt function that causes the thread to terminate.
     * This function must be thread safe.
     */
    virtual void interrupt() = 0;

    /*****
     * Functions to access the internal socket
     * Use these rather than going through the ClientSocket directly so
     * that socket activity can be monitored
     * ***/
    ssize_t receive(std::vector<unsigned char>& buff);
    void send(const std::vector<unsigned char>& buff);
    
    /**
     * Set the connection active status to avoid timing out.
     * This is automatically called when socket activity occurs.
     */
    void setActive();

    ///Return the time this thread was last active.
    time_t lastActive();

    ///Return the time this thread send a keep-alive message.
    time_t lastSentTo();

    ///Return a reference to the client socket
    ClientSocket& sockRef();
};

#endif
