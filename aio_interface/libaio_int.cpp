#include <sys/types.h>
#include <sys/stat.h>
#include <libaio.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <libaio.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <malloc.h>
#include <thread>
#include "libaio_int.hpp"
#include <atomic>

namespace rocksxl
{
namespace aio_interface
{
#define MAX_OPEN_REQUEST_NUM 4096 
io_context_t ctx;

std::atomic<uint> n_requests; 


static void requestDone(struct iocb *iocb, long size, long status)
{
  if (size <= 0 || status != 0)  {
    printf("failed to run cmd : %s  device %d lba %lld size=%ld return code  size %ld status %ld\n",
	   iocb->aio_lio_opcode == IO_CMD_PREAD ? "Read" : "Write",
	   iocb->aio_fildes,
	   iocb->u.c.offset,
	   iocb->u.c.nbytes,
	   size,
	   status);
	   
    assert(size >0 && status == 0);
  }
  n_requests --;
  auto aioData = (AioData *)iocb->data;
  aioData->callbackFunc(aioData);
  
}
    
  

  
  
static void *aioThread()
{
  struct io_event e[20];
  while(1){
    bzero(e, sizeof(e));
    int count = io_getevents(ctx, 1, 20, e, 0);
    if (count > 0) {
      for (int i = 0; i < count; i++) {
	requestDone(e[i].obj, e[i].res, e[i].res2);
	free(e[i].obj);
      }
    }
  }
  return 0;
}
  
static int fd;
#ifdef LOCAL
const char *driveName = "/home/hilik/test_disk/tmpfile";
#else
const char *driveName = "/home/hiliky/test_disk/tmpfile";
#endif
void aioInit()
{
  if(io_setup(MAX_OPEN_REQUEST_NUM, &ctx)!=0){ //init
    printf("io_setup error\n");
    assert(0);
  }
  
  fd =  open(driveName, O_RDWR| O_NONBLOCK| O_CREAT | O_LARGEFILE   ,
	     S_IRWXU);
  assert(fd > 0);
  new std::thread(aioThread);
}
  

static bool submit(struct iocb *iocb_p)
{
  static size_t count;
  uint n_req = ++n_requests;
  if (++count % 10000 == 0) {
    printf("%lu : %u %lu \n", time(0), n_req, count);
  }
  if (n_req >=  MAX_OPEN_REQUEST_NUM ) {
    int safeCount = 0;
    while (n_requests >= MAX_OPEN_REQUEST_NUM ) {
      // printf("sleeping queue is to long\n");
      safeCount++;
      assert (safeCount < 1000000);
      usleep(1);
    }
  }
  
  int ret = io_submit(ctx, 1, &iocb_p);
  while (ret ==  EAGAIN)  {
    usleep(1);
    ret = io_submit(ctx, 1, &iocb_p);            
  }
  assert(ret >=0);

}


bool Read(AioData *aioData)
{  
  struct iocb *iocb_p = (struct iocb *)malloc(sizeof(struct iocb));
  bzero(iocb_p, sizeof(*iocb_p));
  io_prep_pread(iocb_p, fd, aioData->data, aioData->size, aioData->aioLba);
  iocb_p->data = aioData;
  return submit(iocb_p);
}

	    
bool Write(AioData *aioData)
{
  struct iocb *iocb_p = (struct iocb *)malloc(sizeof(struct iocb));
  io_prep_pwrite(iocb_p, fd, aioData->data, aioData->size, aioData->aioLba);
  iocb_p->data = aioData;
  
  return submit(iocb_p);
}


}
}
