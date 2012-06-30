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
 * A thread pool for packaged tasks.
 ******************************************************************************/

#ifndef __TASK_POOL__
#define __TASK_POOL__

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <vector>
#include <stdexcept>
#include <thread>

#include <mysql/mysql.h>
//#include "mysql_world_model.hpp"

template<typename T>
class QueryThread {
  private:
    //Memory for the threads
    static std::vector<QueryThread*> memory_pool;
    //The threads themselves
    static std::vector<std::thread> thread_pool;
    static std::mutex vector_mutex;
    static std::mutex mysql_mutex;
    static std::string db_name;
    static std::string user;
    static std::string password;
    static MYSQL* mysql;
    //TODO FIXME Think I am overdoing synchronization
    std::mutex read_mutex;
    std::mutex write_mutex;
    std::mutex exec_mutex;
    std::mutex delete_mutex;
    std::function<T(MYSQL*)> task;
    T data;
    //Used to indicate when a new task is assigned to this thread
    std::condition_variable exec_cond;
    std::condition_variable write_cond;
    std::condition_variable del_cond;
    std::atomic_bool cancelled;
    std::atomic_bool running;
    std::atomic_bool complete;
    MYSQL* handle;
  public:

    QueryThread(std::function<T(MYSQL*)>& task) : task(task) {
      cancelled = false;
      running = false;
      complete = false;
    };

    static void setDBInfo(std::string& db_name, std::string& user, std::string& password, MYSQL* mysql) {
      QueryThread::db_name = db_name;
      QueryThread::user = user;
      QueryThread::password = password;
      QueryThread::mysql = mysql;
    }

    /**
     * Factory to make a QueryThread and return it.
     * The query thread will create its own connection to the mysql server,
     * using the mysql_mutex to lock this action, and will initialize
     * itself to handle the given task.
     */
    static QueryThread* genQueryThread(std::function<T(MYSQL*)>& task) {
      std::unique_lock<std::mutex> lck(vector_mutex);
      memory_pool.push_back(new QueryThread(task));
      //Lock the read mutex so that the caller can immediately execute the task
      //The lock protects the data variable until the task completes and the data is read.
      //std::cerr<<"Locking read mutex!\n";
      memory_pool.back()->read_mutex.lock();
      //std::cerr<<"Making thread!\n";
      thread_pool.emplace_back(std::thread(std::mem_fun(&QueryThread<T>::run), memory_pool.back()));
      thread_pool.back().detach();
      std::cerr<<"There are now "<<thread_pool.size()<<" threads.\n";
      return memory_pool.back();
    }

    //Clean up all threads and objects
    static void destroyThreads() {
      //std::cerr<<"Trying to destroy threads!\n";
      //To do away with all threads, cancel, then join the threads,
      //then delete the QueryThread objects
      for (QueryThread* mem : memory_pool) {
        mem->cancel();
        //Wait for the thread to die (they were detached and thus cannot be joined)
        std::unique_lock<std::mutex> lck(mem->delete_mutex);
        while (not mem->complete) {
          mem->del_cond.wait(lck);
        }
      }
      for (QueryThread* mem : memory_pool) {
        delete mem;
      }
      memory_pool.clear();
      thread_pool.clear();
    }

    static size_t num_threads() {
      return memory_pool.size();
    }

    static T assignTask(std::function<T(MYSQL*)>& task) {
      //Find the first available thread and assign this task to it
      typename std::vector<QueryThread*>::iterator available = memory_pool.end();
      QueryThread* available_p = nullptr;
      {
        std::unique_lock<std::mutex> lck(vector_mutex);
        available = std::find_if(memory_pool.begin(), memory_pool.end(),
            [&](QueryThread* qt) { return qt->read_mutex.try_lock();});
        if (memory_pool.end() != available) {
          available_p = *available;
        }
      }
      //If no thread was available create a new one
      if (nullptr == available_p) {
        //std::cerr<<"Creating new thread!\n";
        //The read mutex starts off locked
        QueryThread* qt(genQueryThread(task));
        if (nullptr == qt) {
          //TODO FIXME Probably throw an error here
          return T();
        }
        //std::cerr<<"Query object created!\n";
        T result = qt->execute(task);
        //std::cerr<<"Result obtained!\n";
        qt->read_mutex.unlock();
        return result;
      }
      else {
        //std::cerr<<"Using existing thread!\n";
        T result = available_p->execute(task);
        available_p->read_mutex.unlock();
        return result;
      }
    }

    ~QueryThread() {
    }


    //To do away with all threads, cancel, then join the threads,
    //then delete the QueryThread objects
    void cancel() {
      cancelled = true;
      //Wake up the thread so that it will exit
      exec_cond.notify_one();
    }

    //Add a new task to execute
    T execute(std::function<T(MYSQL*)>& task) {
      //std::cerr<<"Copying task!\n";
      //The read_mutex should be locked to make sure that only one thread
      //enters this function at a time.
      this->task = task;
      //std::cerr<<"Notifying!\n";
      //Notify the thread that we are running and wake it
      running = true;
      {
        std::unique_lock<std::mutex> lck(exec_mutex);
        exec_cond.notify_one();
      }
      //std::this_thread::yield();
      //If we obtain this lock then the thread has completed execution
      //std::cerr<<"Locking write mutex!\n";
      std::unique_lock<std::mutex> lck(write_mutex);
      //std::cerr<<"Waiting for write cond!\n";
      //Notify the thread to execute and then wait for a result
      while (running) {
        write_cond.wait(lck);
        //write_cond.wait_for(lck, std::chrono::milliseconds(50));
      }
      return data;
    }

    void run() {
      {
        std::unique_lock<std::mutex> lck(mysql_mutex);
        //mysql_thread_init();
        //Need to make a new connection.
        handle = mysql_init(NULL);
        if (NULL == handle) {
          std::cerr<<"Error connecting to mysql: "<<mysql_error(handle)<<'\n';
        }
        else {
          //Enable multiple statement in a single string sent to mysql
          if (NULL == mysql_real_connect(handle,"localhost", user.c_str(), password.c_str(),
                NULL, 0, NULL,CLIENT_MULTI_STATEMENTS)) {
            std::cerr<<"Error connecting to database: "<<mysql_error(handle)<<'\n';
            mysql_close(handle);
            handle = nullptr;
          }
          else {
            //Set the character collation
            {
              std::string statement_str = "set collation_connection = utf16_unicode_ci;";
              if (mysql_query(handle, statement_str.c_str())) {
                std::cerr<<"Error setting collate to utf16.\n";
                mysql_close(handle);
                handle = nullptr;
              }
            }
            //Now try to switch to the database
            if (mysql_select_db(handle, db_name.c_str())) {
              std::cerr<<"Error switching to database for world model: "<<mysql_error(handle)<<"\n";
              mysql_close(handle);
              handle = nullptr;
            }
          }
        }
      }
      while (handle != nullptr and not cancelled) {
        //std::cerr<<"Thread waiting for task!\n";
        //Wait for a new task
        {
          //std::cerr<<"Locking exec mutex!\n";
          std::unique_lock<std::mutex> lck(exec_mutex);
          //std::cerr<<"Waiting on exec cond!\n";
          //Waiting until a task is running
          while (not (running or cancelled)) {
            exec_cond.wait(lck);
            //exec_cond.wait_for(lck, std::chrono::milliseconds(50));
          }
        }
        if (not cancelled) {
          //std::cerr<<"Running task!\n";
          //Execute the new task
          data = task(handle);
        }
        running = false;
        std::unique_lock<std::mutex> lck(write_mutex);
        write_cond.notify_one();
        //std::this_thread::yield();
        //std::cerr<<"Looping!\n";
      }
      std::unique_lock<std::mutex> lck(delete_mutex);
      //Release mysql resources bound to this thread
      mysql_thread_end();
      if (handle != nullptr) {
        mysql_close(handle);
      }
      complete = true;
      del_cond.notify_one();
      //std::notify_all_at_thread_exit(del_cond, std::move(lck));
    }
};

//Memory for the threads
template<typename T> std::vector<QueryThread<T>*> QueryThread<T>::memory_pool;
//The threads themselves
template<typename T> std::vector<std::thread> QueryThread<T>::thread_pool;
template<typename T> std::mutex QueryThread<T>::vector_mutex;
template<typename T> std::mutex QueryThread<T>::mysql_mutex;
template<typename T> std::string QueryThread<T>::db_name;
template<typename T> std::string QueryThread<T>::user;
template<typename T> std::string QueryThread<T>::password;
template<typename T> MYSQL* QueryThread<T>::mysql;

#endif

