#include "pending_writes.hpp"


namespace xn
{
namespace raid
{
  class WriteData
  {    
  public:
    WriteData(PendingWrites *i_pendingWrites, WritesList *i_wrList,
	      RefCountPage *frontPage) :
      pendingWrites(i_pendingWrites), wrList(i_wrList)
    {
      if (frontPage)
	pagesList.push_back(frontPage);	
    }
    ~WriteData() {
      delete wrList;
    }
    void pushPage(RefCountPage *page) {
      pagesList.push_back(page);
    }
    RefCountPage *lastPage() const {
      return pagesList.back();
    }
      
  public:
    PendingWrites *pendingWrites;
    WritesList    *wrList;
    PagesList      pagesList;
  };
  
  PendingWrites::PendingWrites() 
  {
    pthread_mutex_init(&m_mutex, 0);
    m_lastPage = 0;
    m_startAddress = 0;
    m_totalSize = 0;
    m_queueLength = 0;
    m_pendingWrites = 0;
    m_partition = 0;
    m_flushDone = false;
  }

  PendingWrites::~PendingWrites()
  {
    xn_panic(m_pendingWrites == 0 && m_queueLength == 0  && m_lastPage == 0,
	     "early delete of pending writes");
  }
  
  void PendingWrites::appendWriteRequest(WriteRequest *writeRequest, size_t startAddress, size_t ioSize)
  {
    if (!m_partition) {
      writeRequest->decRequest(-1);
      return;
    }
    pthread_mutex_lock(&m_mutex);
    appendWriteRequestNoLock(writeRequest, startAddress, ioSize);
    pthread_mutex_unlock(&m_mutex);
  }
  
  void PendingWrites::flush(size_t maxSize)
  {
    static size_t zeroData = 0;    
    pthread_mutex_lock(&m_mutex);
    m_flushDone = true;
    
    if (m_startAddress + m_totalSize < maxSize - sizeof(size_t)) {
      // need to add a write of zero
      WriteRequest *writeRequest = new WriteRequest((uint8_t *)&zeroData, sizeof(size_t),
						    WriteRequest::deleteWriteRequest);
      writeRequest->incRequest();
      appendWriteRequestNoLock(writeRequest, m_startAddress + m_totalSize, sizeof(size_t));
      
    } else if (!m_pendingWrites) {
      if (m_lastPage) {
	m_lastPage->decRefCount();
	m_lastPage = 0;
      }
    }	
    pthread_mutex_unlock(&m_mutex);
    
  }


  
  void PendingWrites::ioRequestDone(int status)
  {
    pthread_mutex_lock(&m_mutex);
    m_queueLength --;
    if (status != 0)
      blockRequests();
    else if (m_queueLength == 0 && m_pendingWrites) {
      sendPendingRequests();
    }
    pthread_mutex_unlock(&m_mutex);
  }

  static void 	safebcopy(const uint8_t *src,
			  uint8_t *trg,
			  size_t copySize,
			  size_t srcSize)
  {
    size_t actCopySize = std::min(copySize, srcSize);
    bcopy(src, trg, actCopySize);
    bzero(trg+actCopySize, copySize-actCopySize);
  }
			  


  void PendingWrites::sendPendingRequests()
  {
    if (!m_pendingWrites)
      return;
        
    WriteData *writeData = new WriteData(this, m_pendingWrites, m_lastPage);
    RefCountPage *curPage = m_lastPage;
    size_t ioStartAddress = (m_startAddress/ XN_PAGE_SIZE_BYTES) * XN_PAGE_SIZE_BYTES;

    
    if (m_lastPage) {
      xn_panic(m_startAddress > ioStartAddress,
	       "start address must be after current address %lu %lu\n",
	       m_startAddress , ioStartAddress);
      m_lastPage->incRefCount();
    } else {
      xn_panic(m_startAddress == ioStartAddress,  "start address must be equeal to current address %lu %lu\n", m_startAddress , ioStartAddress); 
    }
    
    if (trace_raid) 
      xn_log(trace_raid, "sending writes %lu %lu %lu %lu \n",	      
	      m_startAddress, m_totalSize, ioStartAddress, m_pendingWrites ?  m_pendingWrites->size() : 0 );
    
    
    for (WritesList::iterator iter = m_pendingWrites->begin();
	 iter != m_pendingWrites->end();
	 iter++) {
      
      WriteRequest *writeRequest = (*iter).first;      
      size_t ioSize =  (*iter).second;
      size_t srcPlace = 0;
      size_t curOffset = m_startAddress % XN_PAGE_SIZE_BYTES;
      
      
      while (srcPlace < ioSize ) {
	size_t copySize = std::min(ioSize , XN_PAGE_SIZE_BYTES - curOffset);	
	if (curOffset == 0) {
	  curPage = new RefCountPage();
	  writeData->pushPage(curPage);
	} else {
	  xn_panic(curPage != 0, "cur page should not be zero when appending data");
	}
	safebcopy((const uint8_t *) writeRequest->data()+ srcPlace,
		  curPage->data() + curOffset,
		  copySize,
		  writeRequest->size() > srcPlace ? writeRequest->size() - srcPlace : 0);
		  
	srcPlace += copySize;
	curOffset = 0;
      }
      m_startAddress += ioSize;
    }
    size_t curOffset = m_startAddress % XN_PAGE_SIZE_BYTES;    
    if (curOffset != 0) {
      if (m_lastPage != writeData->lastPage()) {
	if (m_lastPage)
	  m_lastPage->decRefCount();
	m_lastPage = writeData->lastPage();
	m_lastPage->incRefCount();
	bzero(m_lastPage->data() + curOffset, XN_PAGE_SIZE_BYTES - curOffset);
      }
      
      if (m_flushDone) {
	m_lastPage->decRefCount();
	m_lastPage = 0;
      }	
    } else  if (m_lastPage) {
      m_lastPage->decRefCount();
      m_lastPage = 0;
    }
    
    size_t iovcnt;
    iovec *iovp;
    writeData->pagesList.buildIovec(&iovcnt, &iovp);
    Drive::sendRequest(new IoRequest(m_partition->vuId(),
				     m_partition->drive()->id(), iovcnt, iovp,
				     m_partition->offset(ioStartAddress),
				     IO_WRITEV, writeData, writeDone));
    m_queueLength++;
    m_pendingWrites = 0;    
  }

  void PendingWrites::appendWriteRequestNoLock(WriteRequest *writeRequest, size_t startAddress, size_t ioSize)
  {
    if (trace_raid) 
      xn_log(trace_raid, "handling writeRequest %u %u %lu %lu %lu %lu\n",
	     writeRequest->addr().rgId().vuId,  writeRequest->addr().rgId().rgNum,
	     startAddress, ioSize,
	     m_startAddress, m_pendingWrites ?
	      m_pendingWrites->size() : 0 );
    
    if (m_pendingWrites == 0) {
      m_pendingWrites = new WritesList;
      m_startAddress = startAddress;
      m_totalSize = 0;
    } else {
      xn_panic(m_startAddress + m_totalSize  == startAddress,
	       "append mismatch %lu, %lu, %lu",
	       m_startAddress ,m_totalSize  ,startAddress);
    }
    
    m_pendingWrites->push_back(std::make_pair(writeRequest, ioSize));
    m_totalSize += ioSize;
    if (m_queueLength == 0) {
      sendPendingRequests();
    }
  }


    
      

  int PendingWrites::writeDone(IoRequest *ioRequest)
  {
    WriteData *wrData = (WriteData *)ioRequest->user_data;
    wrData->pendingWrites->ioRequestDone(ioRequest->status);
    for (WritesList::iterator iter = wrData->wrList->begin();
	 iter != wrData->wrList->end();
	 iter++) {
      
      WriteRequest *wrRequest = (*iter).first;
      wrRequest->decRequest(ioRequest->status);
    }
    delete wrData;
    if (ioRequest->optype == IO_WRITEV)
      delete [] ioRequest->iov;
    delete ioRequest;     
    return 0;
  }

}
}
