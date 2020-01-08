namespace rocksxl
{
namespace aio_interface
{
  struct AioData
  {
    typedef  void   (*cb)(AioData *) ;
    AioData(size_t aioLba_,
	     void   *data_,
	     size_t  size_,
	     void   *userCntxt_,
	     cb      callbackFunc_) :
      aioLba(aioLba_), data(data_), size(size_),
      userCntxt(userCntxt_), status(0),
      callbackFunc(callbackFunc_) {};
	     
    size_t  aioLba;
    void   *data;
    size_t  size;
    void   *userCntxt;
    size_t  status;
    cb      callbackFunc;
  };


  void read(AioData *);
  void write(AioData *);
  void init(size_t fileSize);
}
}
  
