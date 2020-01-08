#include "memtable.hpp"

namespace rocksxl
{

  std::atomic<size_t> ObjectEntry::s_sequenceId;

namespace memtable {
  HashTable::HashTable(size_t nEntries) : m_hashTable(nEntries), m_mutexes(1024)
  {
  }

  void HashTable::put(ObjectEntry *obj)
  {
    size_t hashEntry = hash(obj->key) % nEntries;
    auto &mutex = m_mutexes[hashEntry % m_mutexes.size()];
    mutex->lock();
    m_hashTable[hashEntry].push_back(obj);
    mutex->unlock();
  }

  void HashTable::get(std::string &key, std::list<ObjectEntry *> &entries) const
  {
    size_t hashEntry = hash(obj->key) % nEntries;
    auto mutex = const_cast <std::mutex *> (m_mutexes[hashEntry % m_mutexes.size()]);
    mutex->lock();
    for (auto const &e : m_hashTable[hashEntry]) {
      if (e->key == key)
	emtries.push_back(e);
    }
    mutex->unlock();
  }

  MemTable::MemTable(size_t requiredSize) :
    m_curSizeBytes(0),
    m_requiredSize(requiredSize),
    m_hashTable(requiredSize/1000),
    m_status(MemTable::RW)
  {
  }

  bool MemTable::put(ObjectEntry *obj)
  {
    if ((m_curSizeBytes += obj->saveSize())  > m_requiredSize) {
      // memtable is full do not insert the object
      return false;
    }
    m_puts++;
    m_hashTable.put(obj);
    m_puts--;
    return true;
  }

  void MemTable::get(std::string &key, std::list<ObjectEntry *> &entries) const
  {
    m_hashTable.get(key, entries);
  }

  // MemTableList
  MemTableList::MemTableList() :
    m_currentMemTable(new MemTable)
    
  {
  }

  // MemTableList
  void MemTableList::get(std::string &key, std::list<ObjectEntry *> &entries) const
  {
    m_currentMemTable->get(key, entries);
    for (auto const &t : entries) {
      if (t->type != OnjectEntry::Update) {
	return; // final version of object
      }
    }
    m_pendingListUpdates.lock_shared();
    
    for (auto const &memTable: m_pendingFlushList) { 
      memTable->get(key, entries);
      for (auto const &t : entries) {
	if (t->type != OnjectEntry::Update) {
	  break; // final version of object
	}
      }
    }

    m_pendingListUpdates.unlock_shared();
  }

  void MemTableList::put(ObjectEntry * &entry) 
  {
    while (!m_currentMemTable->put(entry)) {
      m_pendingListUpdates.lock(); //exclusive lock
      // check under the lock
      if (m_currentMemTable->flushNeeded()) {
	m_pendingListUpdates.push_back(m_currentMemTable);
	m_currentMemTable->pendingForFlush();
	m_currentMemTable = new MemTable;
      }      
      m_pendingListUpdates.unlock(); //exclusive lock
    }
  }

  
}
}

