/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb
{
using namespace std;

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs)
{
	bufDescTable = new BufDesc[bufs];

	for (FrameId i = 0; i < bufs; i++)
	{
		bufDescTable[i].frameNo = i;
		bufDescTable[i].valid = false;
	}

	bufPool = new Page[bufs];

	int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
	hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

	clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{
	delete hashTable;
	delete[] bufPool;
	delete[] bufDescTable;
}

void BufMgr::advanceClock()
{
	clockHand++;
	if (clockHand >= numBufs)
	{
		/* 取模 */
		clockHand %= numBufs;
	}
}

void BufMgr::allocBuf(FrameId &frame)
{
	/**
	 * The number of pages pinned currently.
	 * If all the pages are pinned, raise a BufferExceededException.
	 */
	unsigned pinned = 0;
	/* Search for an available page. */
	while (true)
	{
		/* Inc the clock ptr */
		advanceClock();
		if (!bufDescTable[clockHand].valid)
		{
			/* Use it directly, call Set() later in other functions */
			frame = clockHand;
			return;
		}
		if (bufDescTable[clockHand].refbit)
		{
			/* clear the bit */
			bufDescTable[clockHand].refbit = false;
			continue;
		}
		//Is the page pinned?
		if (bufDescTable[clockHand].pinCnt)
		{
			/* Page is pinned */
			pinned++;
			if (pinned == numBufs)
			{
				/* All pages are pinned; raise an exception */
				throw BufferExceededException();
			}
			else
				continue;
		}
		if (bufDescTable[clockHand].dirty)
		{
			/* dirty bit is set; write back to disk */
			bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			bufDescTable[clockHand].dirty = false;
		}
		/* Page is not dirty; allocate it for further use.
		 * Notice that we need to remove it from the hashtable.
		 */
		frame = clockHand;
		if (bufDescTable[clockHand].valid)
		{
			/* remove from hash table */
			try
			{
				hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			}
			catch (HashNotFoundException &)
			{
				//not in table; do nothing
			}
		}
		break;
	}
}

void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
	FrameId frame;
	try
	{
		hashTable->lookup(file, pageNo, frame);
		//page found in the pool
		bufDescTable[frame].refbit = true;
		bufDescTable[frame].pinCnt++;
		page = (bufPool + frame);
	}
	catch (HashNotFoundException &)
	{
		/* page not in the pool */
		allocBuf(frame);
		//read a page and put it into the pool
		bufPool[frame] = file->readPage(pageNo);
		//insert into hashtable
		hashTable->insert(file, pageNo, frame);
		//finally...
		bufDescTable[frame].Set(file, pageNo);
		page = (bufPool + frame);
	}
}

void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
	FrameId frame;
	try
	{
		hashTable->lookup(file, pageNo, frame);
	}
	catch (HashNotFoundException &)
	{
		//not present; do nothing
		cerr << "Warning: unpinning a nonexistent page" << endl;
		return;
	}
	//found the frame
	if (bufDescTable[frame].pinCnt > 0)
	{
		/* decrease the pin count and set dirty if applicable */
		bufDescTable[frame].pinCnt--;
		if (dirty)
			bufDescTable[frame].dirty = true;
	}
	else
	{
		/* Not pinned; raise an exception */
		throw PageNotPinnedException(bufDescTable[frame].file->filename(), bufDescTable[frame].pageNo, frame);
	}
}

void BufMgr::flushFile(const File *file)
{
	//scan each page in the file
	for (FrameId fi = 0; fi < numBufs; fi++)
	{
		if (bufDescTable[fi].file == file)
		{
			if (!bufDescTable[fi].valid)
			{
				/* invalid page; throw an exception */
				throw BadBufferException(fi, bufDescTable[fi].dirty, bufDescTable[fi].valid, bufDescTable[fi].refbit);
			}
			if (bufDescTable[fi].pinCnt > 0)
			{
				/* page pinned; throw an exception */
				throw PagePinnedException(file->filename(), bufDescTable[fi].pageNo, fi);
			}
			if (bufDescTable[fi].dirty)
			{
				/* Write back the page.
				 * Since writePage() is not a const method, 
				 * we cannot call file->writePage() directly. Wtf!
				 */
				bufDescTable[fi].file->writePage(bufPool[fi]);
				bufDescTable[fi].dirty = false;
			}
			//remove the page from the hashtable
			hashTable->remove(file, bufDescTable[fi].pageNo);
			//and then clear the page
			bufDescTable[fi].Clear();
		}
	}
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
	//allocate a new page and assign a frame to it
	FrameId frame;
	Page p = file->allocatePage();
	allocBuf(frame);
	bufPool[frame] = p;
	pageNo = p.page_number();
	hashTable->insert(file, pageNo, frame);
	bufDescTable[frame].Set(file, pageNo);
	page = bufPool + frame;
}

void BufMgr::disposePage(File *file, const PageId PageNo)
{
	FrameId frame;
	try
	{
		/* Try finding the frame in the buf pool */
		hashTable->lookup(file, PageNo, frame);
		//remove from hash table
		hashTable->remove(file, PageNo);
		//clear it for further use
		bufDescTable[frame].Clear();
	}
	catch (HashNotFoundException &)
	{
		//not in the table; do nothing
	}
	//Finally delete the page
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
	BufDesc *tmpbuf;
	int validFrames = 0;

	for (unsigned i = 0; i < numBufs; i++)
	{
		tmpbuf = &(bufDescTable[i]);
		cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

		if (tmpbuf->valid == true)
			validFrames++;
	}

	cout << "Total Number of Valid Frames:" << validFrames << endl;
}

} // namespace badgerdb
