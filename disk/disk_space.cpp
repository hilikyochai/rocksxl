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
    DiskPartitionId nDisksLocations = diskSizeBytes/s_partitionSizeBytes;
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
    DiskPartitionId nDisksLocations = m_curSize/s_partitionSizeBytes;
    m_allLocations.resize(nDisksLocations);    
    for (uint i = 0; i < nDisksLocations; i++) {
      m_allLocations[i].id = i;
      m_allLocations[i].status = (DiskPartition::LocationStat) *data;
      data++;
      if (m_allLocations[i].status == DiskPartition::freeSpace) 
	m_freeList.push_back(i);
    }
  }
  
  void DiskSpaceManager::enlarge(size_t newDiskSize)
  {
    assert(newDiskSize >= m_curSize);
    
    uint start = m_curSize /s_partitionSizeBytes;
    m_curSize  = newDiskSize;    
    DiskPartitionId nDisksLocations = m_curSize /s_partitionSizeBytes;
    m_allLocations.resize(nDisksLocations);    
    for (uint i = start; i < nDisksLocations; i++) {
      m_allLocations[i].id = i;
      m_freeList.push_back(i);
    }
    
    
  }
  void DiskSpaceManager::save(std::string &to)
  {
    to.resize(sizeof(size_t) + (m_allLocations.size()*sizeof(DiskPartitionId)));
    char *data = const_cast<char *>(to.data());
    
    *(DiskPartitionId *)data = m_curSize;
    data += sizeof(DiskPartitionId);

    for (auto e : m_allLocations) {
      *data = (char) e.status;
      data++;                  
    }
  }

  
}
}

  
