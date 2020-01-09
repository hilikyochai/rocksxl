#pragma once
#include "disk_space.hpp"
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace rocksxl
{
  namespace  aio_interface
  {
    struct AioData;
  }


namespace disk
{
#pragma pack(push,1)
  static const size_t s_diskBlockSize = 0x2000; // must be multiplaction of 4K
  static const size_t s_nBlocksInPartition = s_partitionSizeBytes/s_diskBlockSize;
  struct DiskBlock
  {
    char data[s_diskBlockSize];
  };
  
  typedef std::shared_ptr<DiskBlock> DiskBlockPtr;
  typedef std::vector< DiskBlockPtr > FileData;

  // return when the read is done!!!
  class DiskSyncRead {
  public:
    DiskSyncRead(const DiskPartitionId partition,
		 const size_t blockNum);
    ~DiskSyncRead() {}
    DiskBlockPtr getData() {return m_data;}
  private:
    static void fetchDone(aio_interface::AioData *);
    DiskBlockPtr m_data;
    
    // to implement wait for response ... 
    std::mutex                                      m_mutex;
    std::condition_variable                         m_cond;
  };

  
  class DiskFetcher
  {
  public:
    DiskFetcher(const Locations &locations);
    DiskBlockPtr getBlock();
    void      terminate();
    
  private:
    ~DiskFetcher() {}; // must call to doneWithFetcher!!!    
  private:    
    const Locations                                 &m_locations;
    std::list< DiskBlockPtr >                       m_fetchedData;
    size_t                                          m_activeRequests;
    size_t                                          m_maxActiveRequests;
    std::pair <Locations::const_iterator, size_t>   m_nextFetchLocation;
    std::mutex                                      m_mutex;
    std::condition_variable                         m_cond;
    bool m_terminated;
    void fetch();
    // callback from aioInterface..
    static void fetchDone(aio_interface::AioData *);
  };
  class DiskWriter;
  class WriteSignal
  {
  public:
    virtual ~WriteSignal() {};
    virtual void writeDone(DiskWriter *) = 0;
  };


  class DiskWriter
  {
  public:
    DiskWriter(FileData  &dataToWrite,
	       WriteSignal     *writeSignal);
    ~DiskWriter() {
      //for (auto d : m_dataToWrite)
      //delete d;
    }
    void getNextBlockForWrite(aio_interface::AioData &writeData,
			      bool &lastDataForLoaction,
			      bool &lastData);

    void   newWrite() {m_numActiveWrites++;};
    void   writeDone() {assert(m_numActiveWrites > 0);
      m_numActiveWrites--;
      if (m_numActiveWrites == 0 && m_dataLocation == m_dataToWrite.size()) {
	m_writeSignal->writeDone(this);
      }
    };
    const Locations                           &locations() {return m_locations;}
  private:
    Locations                                 m_locations;
    const     FileData                        m_dataToWrite;
    size_t                                    m_lastLocationOffset;
    size_t                                    m_dataLocation;
    size_t                                    m_numActiveWrites;
    WriteSignal                               *m_writeSignal;
  };

  class DiskWriteManager
  {
  public:
    static DiskWriteManager *s_diskWriteManager;
    static void init(size_t concurentWrites = 2) {
      s_diskWriteManager = new DiskWriteManager(concurentWrites);
    }    
  public:    
    void appendWriter(DiskWriter *);
    bool hasWriters() const {return
	m_numActiveWrites != 0 || 
	!m_writers.empty();}
  private:
    DiskWriteManager(size_t concurentWrites ) :
      m_maxConcurentWrites(concurentWrites),
      m_numActiveWrites(0)
    {};
    
  private:
    typedef std::list<DiskWriter *> Writers;
    Writers                         m_writers;
    Writers::iterator               m_curLocation;
    std::mutex                      m_mutex;
    size_t                          m_maxConcurentWrites;
    std::atomic<size_t>             m_numActiveWrites;
    void                            scheduleWrites();
    static void                     writeDone(aio_interface::AioData *);
  };
}
}
