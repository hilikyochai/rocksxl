#pragma once
#include <vector>
#include <list>
#include <stdint.h>
#include <assert.h>
#include <string>
#include <mutex>

namespace rocksxl
{
namespace disk
{
  // management of allocation of chunks
  static const size_t s_partitionSizeBytes = 1024 * 1024 * 8; // 8M partition Size
#pragma pack(push,1)
  typedef uint32_t DiskPartitionId;

  struct DiskPartition {
    enum  LocationStat : uint8_t {freeSpace, inWrite, allocated} ;
    DiskPartition(DiskPartitionId id_=-1u) : status(freeSpace), id(id_)
    {}
    
    LocationStat status;    
    uint32_t id;
  };
#pragma pack(pop)

  
  class Locations : public std::list<DiskPartitionId>
  {
  public:
    Locations() {}
    Locations(const Locations &sec) {
      for (auto s: sec)
	push_back(s);
    }

    Locations &operator = (const Locations &sec) {
      for (auto s: sec)
	push_back(s);
      return *this;
    }

    DiskPartitionId popLocation() {
      assert(!empty());
      std::lock_guard<std::mutex> lk(m_mutex);
      DiskPartitionId ret = front();
      pop_front();
      return ret;
    }
    
    
    void  pushLocation(DiskPartitionId location) {
      std::lock_guard<std::mutex> lk(m_mutex);      
      push_front(location);
    }
    size_t sizeInBytes() const {return size() * s_partitionSizeBytes;}
  private:
    std::mutex m_mutex;
  };

  class DiskSpaceManager
  {
  public:
    static void init(size_t diskSize) {
      s_diskSpaceManager = new DiskSpaceManager(diskSize);
    }    
    static void load(const std::string &from);
    static DiskSpaceManager    *s_diskSpaceManager;
    
  public:
    void enlarge(size_t newDiskSize);
    void save(std::string &to);

    DiskPartitionId getFreePlace() {
      auto ret = m_freeList.popLocation();
      m_allLocations[ret].status = DiskPartition::inWrite;
      return ret;
    } 
    void doneWithWrite(DiskPartitionId locationId) {
      auto &diskLocation = m_allLocations[locationId];
      assert(diskLocation.status == DiskPartition::inWrite);
      diskLocation.status = DiskPartition::allocated;
    }
    
    void writeAborted(DiskPartitionId locationId) {
      auto &diskLocation = m_allLocations[locationId];
      assert(diskLocation.status == DiskPartition::inWrite);
      diskLocation.status = DiskPartition::freeSpace;
      m_freeList.pushLocation(locationId);
    }
    void freeLocation(DiskPartitionId locationId) {
      auto &diskLocation = m_allLocations[locationId];
      assert(diskLocation.status == DiskPartition::allocated);      
      diskLocation.status = DiskPartition::freeSpace;
      m_freeList.pushLocation(locationId);
    }
    size_t freeSpaceSize() const {return m_freeList.sizeInBytes();}
  private:
    DiskSpaceManager(size_t diskSize);    
    DiskSpaceManager(const std::string &from);
    
  private:
    size_t                    m_curSize;
    Locations                 m_freeList;
    std::vector<DiskPartition> m_allLocations; 
  };
}
}
