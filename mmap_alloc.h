#ifndef MMAP_ALLOC_H
#define MMAP_ALLOC_H

#include <vector>
#include <string>
#include <iostream>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>


/*
 * Implements an allocator with no overhead, but memory cannot be freed (except by destroying the whole allocator)
 * May be backed by (hidden) swap files on the filesystem, so it can allocate more than the available physical memory (works best on 64-bits machines)
 */
class mmap_alloc
{
private:
	struct chunk_descriptor {
		void *ptr;
		size_t size;
	};

	std::string directory;
	std::vector< chunk_descriptor > chunks;
	size_t last_alloc_sz;
	size_t min_alloc_sz;
	size_t max_alloc_sz;
	char *cur_chunk;
	size_t next_alloc_offset;
	size_t cur_chunk_left;

	int alloc_new_chunk( size_t want )
	{
		chunk_descriptor descr;

		if ( last_alloc_sz < min_alloc_sz )
			last_alloc_sz = min_alloc_sz;
		else if ( last_alloc_sz < max_alloc_sz )
			last_alloc_sz *= 2;

		descr.size = last_alloc_sz;
		if ( descr.size < want + sizeof(size_t) )
			descr.size = want + sizeof(size_t);

		int fd = -1;
		int flags = MAP_PRIVATE | MAP_ANONYMOUS;
		if ( directory.size() )
		{
			// allocate a new file mapping in the target directory
			for ( char i = '0' ; i <= '~' ; ++i )	// '0' > '/' which is forbidden in paths
			{
				std::string path = directory + "/tmp_swap_" + i;
				fd = open( path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600 );
				if ( fd != -1 )
				{
					unlink( path.c_str() );
					break;
				}

				if ( errno != EEXIST )
				{
					std::cerr << "cannot create tmpswap: " << strerror( errno ) << std::endl;
					return 1;
				}
			}

			// set the file size
			lseek( fd, descr.size-1, SEEK_SET );
			if ( write( fd, "", 1 ) != 1 )
			{
				std::cerr << "mmap_alloc: cannot allocate new file: " << strerror( errno ) << std::endl;
				close( fd );
				return 1;
			}

			flags = MAP_SHARED;
		}

		descr.ptr = mmap( NULL, descr.size, PROT_READ | PROT_WRITE, flags, fd, 0 );
		if ( descr.ptr == MAP_FAILED )
		{
			std::cerr << "mmap_alloc: cannot mmap: " << strerror( errno ) << std::endl;
			if ( fd != -1 )
				close( fd );
			return 1;
		}
		if ( fd != -1 )
			close( fd );

		chunks.push_back( descr );

		// setup vars used by alloc()
		cur_chunk = (char *)descr.ptr;
		next_alloc_offset = 0;
		cur_chunk_left = descr.size;

		return 0;
	}

public:
	explicit mmap_alloc( const std::string &dir, size_t min = 16*1024*1024, size_t max=256*1024*1024 ) :
		directory(dir),
		last_alloc_sz(0),
		min_alloc_sz(min),
		max_alloc_sz(max),
		cur_chunk(NULL),
		next_alloc_offset(0),
		cur_chunk_left(0)
	{
	}

	~mmap_alloc()
	{
		for ( unsigned i = 0 ; i < chunks.size() ; ++i )
			munmap( chunks[ i ].ptr, chunks[ i ].size );
	}

	void *alloc( size_t size, size_t align )
	{
		size_t pad = 0;
		if ( align > 1 && (next_alloc_offset & (align-1)) )
			pad = align - (next_alloc_offset & (align-1));

		if ( size + pad > cur_chunk_left )
			if ( alloc_new_chunk( size + pad ) )
				return NULL;

		void *ret = (void *)(cur_chunk + pad + next_alloc_offset);
		next_alloc_offset += size + pad;
		cur_chunk_left -= size + pad;

		return ret;
	}

private:
	mmap_alloc ( const mmap_alloc& );
	mmap_alloc& operator=( const mmap_alloc& );
};

#endif
