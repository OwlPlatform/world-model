/*******************************************************************************
 * Implementation of counting semaphores using the resource allocation in
 * initialization pattern for waiting and signalling.
 ******************************************************************************/

#ifndef __SEMAPHORE_HPP__
#define __SEMAPHORE_HPP__

#include <condition_variable>
#include <mutex>

class Semaphore {
  private:
    //TODO FIXME This should be std::atomic_size_t and the count_m mutex could go away.
    size_t count;
    //Lock during an intermediate step in the lock() function.
    std::mutex count_m;
    //Lock to prevent all flag and lock actions.
    std::mutex master;
    //Used to notify locking commands when the count changes.
    std::condition_variable cond;

  public:

    Semaphore();

    ///The Semaphore::lock method locks the master lock and makes this call block
    void flag();
    void unflag();

    /**
     * This waits until count is zero and makes other calls to
     * lock or notify block until unlock is called.
     */
    void lock();
    void unlock();

};

class SemaphoreFlag {
  private:
    Semaphore& s;
  public:
    SemaphoreFlag(Semaphore& s);
    ~SemaphoreFlag();
};

class SemaphoreLock {
  private:
    Semaphore& s;
  public:
    SemaphoreLock(Semaphore& s);
    ~SemaphoreLock();
};

#endif //defined __SEMAPHORE_HPP__

