#include "disk_io_manager.hpp"
#include "../aio_interface/libaio_int.hpp"
namespace rocksxl
{
namespace disk
{

  DiskSyncRead::DiskSyncRead(const DiskPartitionId partition,
			     const size_t blockNum) :
    m_data(new DiskBlock)
  {
    std::unique_lock<std::mutex> lk(m_mutex);
    const size_t lba = partition * s_partitionSizeBytes + blockNum *s_diskBlockSize ;
    auto aioData = new aio_interface::AioData(lba, m_data.get(), s_diskBlockSize,
					      this, fetchDone);   
    aio_interface::Read(aioData);
    m_cond.wait(lk);
  }

  void DiskSyncRead::fetchDone(aio_interface::AioData *data)
  {
    assert(data->status == 0);
    auto me = (DiskSyncRead *) data->userCntxt;
    std::lock_guard<std::mutex> lk(me->m_mutex);;
    delete data;
    me->m_cond.notify_one();
  }
  
  
  DiskFetcher::DiskFetcher(const Locations &locations) :
    m_locations(locations),
    m_maxActiveRequests(1) ,
    m_activeRequests(0),
    m_nextFetchLocation(m_locations.cbegin(), 0),
    m_terminated(false)
  {
    std::unique_lock<std::mutex> lk(m_mutex);
    fetch();    
  }

  // return empty when fetch is done!!! 
  DiskBlockPtr DiskFetcher::getBlock()
  {
    DiskBlockPtr ret;
    std::unique_lock<std::mutex> lk(m_mutex);
    do { 
      if (!m_fetchedData.empty()) {
	ret = m_fetchedData.front();
	m_fetchedData.pop_front();
	break;	  
      }
      if (m_activeRequests == 0)
	break;
      m_cond.wait(lk);
    } while(1);
    fetch();
    return ret;
  }
  
  
  void DiskFetcher::fetch()
  {
    if (m_terminated)
      return;
    while (m_activeRequests + m_fetchedData.size() <  m_maxActiveRequests) {
      if (m_nextFetchLocation.first == m_locations.cend()) {
	break;
      }
      m_activeRequests++;
      const size_t lba = (*m_nextFetchLocation.first) *s_partitionSizeBytes + m_nextFetchLocation.second;
      auto aioData = new aio_interface::AioData(lba, new DiskBlock, s_diskBlockSize,
						  this, fetchDone);
      
      aio_interface::Read(aioData);
      m_nextFetchLocation.second += s_diskBlockSize;
      if (m_nextFetchLocation.second >= s_partitionSizeBytes) {
	m_nextFetchLocation.first++;
	m_nextFetchLocation.second=0;
      }
    }
  }

  void DiskFetcher::fetchDone(aio_interface::AioData *data)
  {
    assert(data->status == 0);
    auto me = (DiskFetcher *) data->userCntxt;
    bool toDelete = false;
    {
      std::lock_guard<std::mutex> lk(me->m_mutex);;
      me->m_activeRequests--;
      if (me->m_terminated && me->m_activeRequests == 0) {
	toDelete = true;
      }  else {    
	me->m_fetchedData.push_back(DiskBlockPtr((DiskBlock *)data->data));
	me->m_cond.notify_one();
      }
      delete data;
    }
    if (toDelete) {
      delete me;
    }
  }

  void DiskFetcher::terminate()
  {
    bool toDelete = false;
    {
      std::unique_lock<std::mutex> lk(m_mutex);
      m_terminated = true;
      toDelete = m_activeRequests == 0;
    }
    if (toDelete) {
      delete this;
    }
  }
    


  DiskWriter::DiskWriter(FileData &dataToWrite,
			 WriteSignal *writeSignal) :
    m_lastLocationOffset(0),
    m_dataLocation(0),
    m_numActiveWrites(0),
    m_dataToWrite(dataToWrite),    
    m_writeSignal(writeSignal)
  {
    DiskWriteManager::s_diskWriteManager->appendWriter(this);
  }

  void DiskWriter::getNextBlockForWrite(aio_interface::AioData &aioData,
					bool &lastDataForLoaction,
					bool &lastData)
  {
    assert( m_dataLocation < m_dataToWrite.size());
    if (m_lastLocationOffset == 0) {
      m_locations.push_back(DiskSpaceManager::s_diskSpaceManager
			    ->getFreePlace());
    } 
    aioData.aioLba = m_locations.back() * s_partitionSizeBytes + m_lastLocationOffset;
    aioData.data = const_cast<char *>( m_dataToWrite[m_dataLocation]->data);
    aioData.size = s_diskBlockSize;
    m_dataLocation++;
    m_lastLocationOffset += s_diskBlockSize;
    if (m_lastLocationOffset >= s_partitionSizeBytes) {
      m_lastLocationOffset  = 0;
      lastDataForLoaction = true;
    } else {
      lastDataForLoaction = false;
    }	
    lastData = 	m_dataLocation == m_dataToWrite.size();
  }
  
  DiskWriteManager *DiskWriteManager::s_diskWriteManager;

  void DiskWriteManager::appendWriter(DiskWriter *diskWriter)
  {
    std::lock_guard<std::mutex> lk(m_mutex);    
    m_writers.push_back(diskWriter);
    if (m_writers.size() == 1) {
      m_curLocation = m_writers.begin();
    }
    if (m_numActiveWrites < m_maxConcurentWrites) {
      scheduleWrites();
    }
  }

  //lock is held
  void DiskWriteManager::scheduleWrites()
  {    
    while (!m_writers.empty() && m_numActiveWrites < m_maxConcurentWrites) {
      if (m_curLocation == m_writers.end()) {
	m_curLocation = m_writers.begin();
      }      
      auto aioData = new aio_interface::AioData(0,0,0,*m_curLocation,writeDone);
      bool lastDataForLoaction = false;
      bool lastData = false;
      (*m_curLocation)->getNextBlockForWrite(*aioData, lastDataForLoaction, lastData);
      (*m_curLocation)->newWrite();
      if (lastData) {
	auto tmp = m_curLocation;
	m_curLocation++;
	m_writers.erase(tmp);
      } else {
	if (lastDataForLoaction) {
	  m_curLocation++;
	}
      }
      m_numActiveWrites++;
      aio_interface::Write(aioData);
    }
  }

  void DiskWriteManager::writeDone(aio_interface::AioData *aioData)    
  {
    DiskWriter *diskWriter = (DiskWriter *)aioData->userCntxt;
    diskWriter->writeDone();
    std::lock_guard<std::mutex> lk(s_diskWriteManager->m_mutex);    
    s_diskWriteManager->m_numActiveWrites--;
    if (!s_diskWriteManager->m_writers.empty()) {
      s_diskWriteManager->scheduleWrites();
    }
    delete aioData;
  }

}
}

#ifdef DISK_IO_UTESTS
#include <thread>
#include <unistd.h>

using namespace rocksxl;
struct test_file
{
  disk::Locations   file_chunks;
  std::mutex        *mutex;
};

std::vector<test_file>       test_files;
static   std::atomic<int>    nWriters;

class UtestWriteDone : public disk::WriteSignal
{
public:
  UtestWriteDone(int index) : m_index(index)
  {
  }
  void writeDone(disk::DiskWriter *writer) {
    auto &f = test_files[m_index];
    f.file_chunks = writer->locations();
    auto spaceManager = disk::DiskSpaceManager::s_diskSpaceManager;
    for (auto fl : f.file_chunks) {
      spaceManager->doneWithWrite(fl);
    }
      
    f.mutex->unlock();
    delete writer;
    nWriters--;    
  }
private:
  int m_index;
  
}; 

class UtestCompactWriteDone : public UtestWriteDone
{
public:
  UtestCompactWriteDone(int index,
			std::vector<size_t> compactedFiles) :
    UtestWriteDone(index),
    m_compactedFiles(compactedFiles)
  {
  }
  void writeDone(disk::DiskWriter *writer) {
    UtestWriteDone::writeDone(writer);    
    auto spaceManager = disk::DiskSpaceManager::s_diskSpaceManager;
    for (auto f : m_compactedFiles) {      
      for (auto l : test_files[f].file_chunks) {
	spaceManager->freeLocation(l);
      }
      test_files[f].file_chunks.clear();
      test_files[f].mutex->unlock();
    }
  }
private:
  int m_index;
  std::vector<size_t> m_compactedFiles;
}; 


void runWriter(uint indexNum)
{
  while (nWriters  > 32) {
    usleep(1);
  }
  nWriters++;
  int fileSize = rand() % 16 + 1;
  uint numBlocks = fileSize * (disk::s_partitionSizeBytes/disk::s_diskBlockSize);
  disk::FileData dataToWrite(numBlocks);
  for (uint i = 0; i < numBlocks; i++) {    
    dataToWrite[i] = disk::DiskBlockPtr(new disk::DiskBlock); 
  }
  
  auto diskWriter = new disk::DiskWriter(dataToWrite, new UtestWriteDone(indexNum));
  
}

void runCompact(std::vector<size_t> filesToCompact,
		uint targetFile)
{
  std::vector<disk::DiskFetcher *> fetchers(filesToCompact.size());
  for (uint i = 0; i < filesToCompact.size(); i++) {    
    fetchers[i] = new disk::DiskFetcher(test_files[filesToCompact[i]].file_chunks);
  }
  std::vector<disk::DiskBlockPtr > prefechedBlock(filesToCompact.size());
  for (uint i = 0; i < filesToCompact.size(); i++) {
    prefechedBlock[i] = fetchers[i]->getBlock();
  }
  
  std::vector< disk::DiskBlockPtr >compactOutput;
  size_t count = fetchers.size()*16* disk::s_nBlocksInPartition;
  for (size_t j=0; j<count; j++)   {
    // randomly select a block from the fetched
    int i = rand() % fetchers.size();
    if (fetchers[i]) {
      auto block = fetchers[i]->getBlock();
      if (!block) {
	fetchers[i]->terminate();
	fetchers[i] = 0;
      } else {
	// to get back about the same size we use rand() % fetchers.size...
	if (rand() % fetchers.size() == 0) {
	  compactOutput.push_back(block);
	}
      }
    }
  }
  
  while (nWriters  > 32) {
    usleep(1);
  }
  nWriters++;
  auto diskWriter = new disk::DiskWriter(compactOutput, new UtestCompactWriteDone(targetFile, filesToCompact));
  for (auto f : fetchers) {
    if (f)
      f->terminate();
  }
  
  
}

int selectTarget()
{
  for (uint i = 0;i< test_files.size(); i++) {
    auto &t = test_files[i];
    if (t.file_chunks.empty()) {
      if (t.mutex->try_lock()) {
	if (t.file_chunks.empty()) {
	  // keep the lock
	  return i;
	} else {
	  t.mutex->unlock();
	}
      }
    }
  }
  return -1;
}
	  
int selectSource()
{
  for (uint i = 0;i< test_files.size(); i++) {
    auto &t = test_files[i];
    if (!t.file_chunks.empty()) {
      if (t.mutex->try_lock()) {
	if (!t.file_chunks.empty()) {
	  // keep the lock
	  return i;
	} else {
	  t.mutex->unlock();
	}
      }
    }
  }
  return -1;
}
    
  

void threadRun()
{
  for (int i = 0; i < 100; i++) {
    if ((rand() % 16)) {
      runWriter(selectTarget());
    } else  {      
      // compaction job  define 16 fetchers and one target
      std::vector<size_t> fetchers(16);
      for (int k = 0; k < 16; k++) {
	fetchers[k] = selectSource();
	if (fetchers[k] == -1) {
	  fetchers.resize(k);
	  break;
	}	  
      }
      if (fetchers.size()) {
	runCompact(fetchers, selectTarget());
      }	
    } 
  }
}

int main()
{
  const size_t s_fileSize = 1024ll * 1024 * 1024 * 16;
  aio_interface::aioInit();
  disk::DiskWriteManager::init();
  disk::DiskSpaceManager::init(s_fileSize);
  test_files.resize(1024);
  for (int i = 0; i < 1024; i++) {
    test_files[i].mutex = new std::mutex;
  }
  threadRun();
  auto diskWriteManager = disk::DiskWriteManager::s_diskWriteManager;
  while (nWriters > 0 && diskWriteManager->hasWriters()) {
    printf("waiting for writes to finish\n");
    sleep(1);
  }
  
}
#endif
    
  

  
  
  
  
    
    
  
