#include <condition_variable>
#include <mutex>

#include <semaphore.hpp>

Semaphore::Semaphore() {
  count = 0;
}

void Semaphore::flag() {
  std::unique_lock<std::mutex> lck(master);
  ++count;
}
void Semaphore::unflag() {
  std::unique_lock<std::mutex> lck(master);
  --count;
  //If a lock call is blocking on the count let it know that count
  //has been decremented
  cond.notify_one();
}

void Semaphore::lock() {
  //While this is locked new calls to flag will block.
  std::unique_lock<std::mutex> lck(count_m);
  while(count != 0) {
    cond.wait(lck);
  }
  //Lock once more so that a call to unlock() is needed.
  master.lock();
}
void Semaphore::unlock() {
  master.unlock();
}

SemaphoreFlag::SemaphoreFlag(Semaphore& s) : s(s){
  s.flag();
}
SemaphoreFlag::~SemaphoreFlag() {
  s.unflag();
}

SemaphoreLock::SemaphoreLock(Semaphore& s) : s(s) {
  s.lock();
}
SemaphoreLock::~SemaphoreLock() {
  s.unlock();
}

