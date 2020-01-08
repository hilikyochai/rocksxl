#include "disk_space.hpp"
namespace rocksxl
{
namespace disk
{
  DiskSpaceManager *DiskSpaceManager::s_diskSpaceManager;
  // manintain a virtual disk with pre-determine size
  DiskSpaceManager::DiskSpaceManager(size_t diskSizeBytes) :
    m_curSize(diskSizeBytes)
  {
    size_t nDisksLocations = diskSizeBytes/s_diskChunkSize;
    m_allLocations.resize(nDisksLocations);
    for (uint i = 0; i < nDisksLocations; i++) {
      m_allLocations[i].id = i;
      m_freeList.push_back(i);
    }
  }
  DiskSpaceManager::DiskSpaceManager(const std::string &from)
  {
    
    const char * data = from.data();
    m_curSize  = *(size_t *)data;
    data += sizeof(size_t);
    size_t nDisksLocations = m_curSize/s_diskChunkSize;
    m_allLocations.resize(nDisksLocations);    
    for (uint i = 0; i < nDisksLocations; i++) {
      m_allLocations[i].id = i;
      m_allLocations[i].status = (DiskLocation::LocationStat) *data;
      data++;
      if (m_allLocations[i].status == DiskLocation::freeSpace) 
	m_freeList.push_back(i);
    }
  }
  
  void DiskSpaceManager::enlarge(size_t newDiskSize)
  {
    assert(newDiskSize >= m_curSize);
    
    uint start = m_curSize /s_diskChunkSize;
    m_curSize  = newDiskSize;    
    size_t nDisksLocations = m_curSize /s_diskChunkSize;
    m_allLocations.resize(nDisksLocations);    
    for (uint i = start; i < nDisksLocations; i++) {
      m_allLocations[i].id = i;
      m_freeList.push_back(i);
    }
    
    
  }
  void DiskSpaceManager::save(std::string &to)
  {
    to.resize(sizeof(size_t) + m_allLocations.size() );
    char *data = const_cast<char *>(to.data());
    
    *(size_t *)data = m_curSize;
    data += sizeof(size_t);

    for (auto e : m_allLocations) {
      *data = (char) e.status;
      data++;                  
    }
  }

  
}
}

  
