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
  static const size_t s_diskChunkSize = 1024 * 1024 * 8; // perform 8 M i/o to the disk!
#pragma pack(push,1)
  struct DiskLocation {
    enum  LocationStat : uint8_t {freeSpace, inWrite, allocated} ;
    DiskLocation(uint32_t id_=-1u) : status(freeSpace), id(id_)
    {}
    
    LocationStat status;    
    uint32_t id;
  };
#pragma pack(pop)
  
  class Locations : public std::list<size_t>
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

    size_t popLocation() {
      assert(!empty());
      std::lock_guard<std::mutex> lk(m_mutex);
      size_t ret = front();
      pop_front();
      return ret;
    }
    
    
    void  pushLocation(size_t location) {
      std::lock_guard<std::mutex> lk(m_mutex);      
      push_front(location);
    }
    size_t sizeInBytes() const {return size() * s_diskChunkSize;}
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

    size_t getFreePlace() {
      auto ret = m_freeList.popLocation();
      m_allLocations[ret].status = DiskLocation::inWrite;
      return ret;
    } 
    void doneWithWrite(size_t locationId) {
      auto &diskLocation = m_allLocations[locationId];
      assert(diskLocation.status == DiskLocation::inWrite);
      diskLocation.status = DiskLocation::allocated;
    }
    
    void writeAborted(size_t locationId) {
      auto &diskLocation = m_allLocations[locationId];
      assert(diskLocation.status == DiskLocation::inWrite);
      diskLocation.status = DiskLocation::freeSpace;
      m_freeList.pushLocation(locationId);
    }
    void freeLocation(size_t locationId) {
      auto &diskLocation = m_allLocations[locationId];
      assert(diskLocation.status == DiskLocation::allocated);      
      diskLocation.status = DiskLocation::freeSpace;
      m_freeList.pushLocation(locationId);
    }
    size_t freeSpaceSize() const {return m_freeList.sizeInBytes();}
  private:
    DiskSpaceManager(size_t diskSize);    
    DiskSpaceManager(const std::string &from);
    
  private:
    size_t                    m_curSize;
    Locations                 m_freeList;
    std::vector<DiskLocation> m_allLocations; 
  };
}
}
