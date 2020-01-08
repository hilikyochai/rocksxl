#ifndef XN_HYBRID_PENDING_WRITES_HPP__
#define XN_HYBRID_PENDING_WRITES_HPP__

#include "io_request.hpp"
#include "write_request.hpp"
#include "partition.hpp"
namespace xn
{
namespace raid
{
  typedef std::list<std::pair<WriteRequest *, size_t> > WritesList;

  class PendingWrites
  {
  public:
    PendingWrites();
    ~PendingWrites();
  public:
    void setPartition(DrivePartition *partition) {
      XN_ASSERT(m_partition==0); m_partition = partition;
    }
    void blockRequests() {
      m_partition = 0;
      abortPendingWrites();
    }
    void abortPendingWrites() { // TBD
    }
  public:
    void flush(size_t maxSize);
    void appendWriteRequest(WriteRequest *, size_t startAddress, size_t ioSize);
    void ioRequestDone(int status);
    void init() {
      m_startAddress = m_totalSize = m_queueLength = 0;
      m_flushDone = false;
    }
  private:
    void appendWriteRequestNoLock(WriteRequest *, size_t startAddress, size_t ioSize);
    void sendPendingRequests();
    static int writeDone(IoRequest *ioRequest);    
  private:
    pthread_mutex_t      m_mutex;
    DrivePartition      *m_partition;    
    WritesList          *m_pendingWrites;
    RefCountPage        *m_lastPage;
    size_t              m_startAddress;
    size_t              m_totalSize;
    size_t              m_queueLength;
    bool                m_flushDone; 

  };
}
}

#endif
