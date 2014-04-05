#include <stdlib.h>
#include <getopt.h>
#include <iostream>
#include <vector>
#include <regex.h>
#include <tr1/unordered_set>
#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "output_buffer.h"
#include "csv_reader.h"

#define CSV_AGGREG_VERSION "20140404"

static std::string str_downcase( const std::string &str )
{
	std::string ret;
	for ( unsigned i = 0 ; i < str.size() ; ++i )
		ret.push_back( tolower( str[ i ] ) );
	return ret;
}

/*
 * murmur3 hash function
 */
static uint32_t murmur3_32( const void *key, size_t len, uint32_t seed = 0 )
{
	uint32_t hash = seed;
	uint32_t *key_u32 = (uint32_t *)key;
	uint32_t k;

	while (len >= 4)
	{
		k = *key_u32++;
		len -= 4;

		k *= 0xcc9e2d51;
		k = (k << 15) | (k >> (32-15));
		k *= 0x1b873593;

		hash ^= k;

		hash = (hash << 13) | (hash >> (32-13));
		hash = hash * 5 + 0xe6546b64;
	}

	uint8_t *key_u8 = (uint8_t *)key_u32;

	k = 0;
	switch (len & 3)
	{
	case 3: k ^= key_u8[2] << 16;
	case 2: k ^= key_u8[1] << 8;
	case 1: k ^= key_u8[0];

		k *= 0xcc9e2d51;
		k = (k << 15) | (k >> (32-15));
		k *= 0x1b873593;

		hash ^= k;
	}

	hash ^= len;
	hash ^= (hash >> 16);
	hash *= 0x85ebca6b;
	hash ^= (hash >> 13);
	hash *= 0xc2b2ae35;
	hash ^= (hash >> 16);

	return hash;
}

static uint32_t murmur3_32( const std::string *s, uint32_t seed = 0 )
{
	return murmur3_32( s->data(), s->size(), seed );
}


/*
 * list of aggregator functions
 */

union u_data {
	long long ll;
	std::string *str;
	std::vector< std::string > *vec_str;
};

static void str_alloc( u_data *ptr )
{
	ptr->str = new std::string;
}

static void str_free( u_data *ptr )
{
	delete ptr->str;
}

static std::string str_key( const std::string &field )
{
	return std::string( field );
}

static std::string str_out( u_data *ptr )
{
	return csv_reader::escape_csv_string( *ptr->str );
}

static std::string downcase_key( const std::string &field )
{
	return str_downcase( field );
}

static void top_alloc( u_data *ptr )
{
	ptr->vec_str = new std::vector< std::string >;
}

static void top_free( u_data *ptr )
{
	delete ptr->vec_str;
}

static void top20_aggreg( u_data *ptr, const std::string *field, int first )
{
	(void)first;

	if ( ptr->vec_str->size() >= 20 )
		return;

	for ( unsigned i = 0 ; i < ptr->vec_str->size() ; ++i )
		if ( (*ptr->vec_str)[ i ] == *field )
			return;

	ptr->vec_str->push_back( *field );
}

static void top20_merge( u_data *ptr, const std::string *field, int first )
{
	size_t last = 0, next = field->find( ',' );
	while ( next != std::string::npos )
	{
		const std::string &tmp = field->substr( last, next-last );
		top20_aggreg( ptr, &tmp, first );
		last = next + 1;
		next = field->find( ',', last );
	}

	const std::string &tmp = field->substr( last );
	top20_aggreg( ptr, &tmp, first );
}

static std::string top_out( u_data *ptr )
{
	std::string tmp;

	for ( unsigned i = 0 ; i < ptr->vec_str->size() ; ++i )
	{
		if ( i > 0 )
			tmp.push_back( ',' );
		tmp.append( (*ptr->vec_str)[ i ] );
	}

	return csv_reader::escape_csv_string( tmp );
}

static void min_aggreg( u_data *ptr, const std::string *field, int first )
{
	long long val = strtoll( field->c_str(), 0, 0 );
	if ( first || val < ptr->ll )
		ptr->ll = val;
}

static void max_aggreg( u_data *ptr, const std::string *field, int first )
{
	long long val = strtoll( field->c_str(), 0, 0 );
	if ( first || val > ptr->ll )
		ptr->ll = val;
}

static void minstr_aggreg( u_data *ptr, const std::string *field, int first )
{
	if ( first || *field < *ptr->str )
		*ptr->str = *field;
}

static void maxstr_aggreg( u_data *ptr, const std::string *field, int first )
{
	if ( first || *field > *ptr->str )
		*ptr->str = *field;
}

static void count_aggreg( u_data *ptr, const std::string *field, int first )
{
	(void)field;
	if ( first )
		ptr->ll = 1;
	else
		++(ptr->ll);
}

static void count_merge( u_data *ptr, const std::string *field, int first )
{
	if ( first )
		ptr->ll = 0;
	ptr->ll += strtoll( field->c_str(), 0, 0 );
}

static std::string int_out( u_data *ptr )
{
	char buf[16];
	unsigned buf_sz = snprintf( buf, sizeof(buf), "%lld", ptr->ll );
	if ( buf_sz > sizeof(buf) )
		buf_sz = sizeof(buf);
	return std::string( buf, buf_sz );
}


/*
 * list of aggregators
 */

struct aggreg_descriptor {
	// aggregator name, used in config/help messages
	const char *name;
	// called when allocated a new aggregated_data, with ptr pointing at the designated offset
	void (*alloc)( u_data *ptr );
	void (*free)( u_data *ptr );
	// called during aggregation, ptr is the same as for alloc(), field is the unescaped csv field value, first = 1 if field is the 1st entry to be aggregated here
	void (*aggreg)( u_data *ptr, const std::string *field, int first );
	// called during merge, similar to aggreg, but field points to the result of a previous out(aggreg())
	void (*merge)( u_data *ptr, const std::string *field, int first );
	// determine aggregation key, should append data to key. field is the raw csv field value, it is neither escaped nor unescaped.
	std::string (*key)( const std::string &field );
	// called when dumping aggregation results, ptr is the same as for alloc().
	std::string (*out)( u_data *ptr );
} aggreg_descriptors[] =
{
	{
		"str",
		str_alloc,
		str_free,
		NULL,
		NULL,
		str_key,
		str_out,
	},
	{
		"downcase",
		str_alloc,
		str_free,
		NULL,
		NULL,
		downcase_key,
		str_out,
	},
	{
		"top20",
		top_alloc,
		top_free,
		top20_aggreg,
		top20_merge,
		NULL,
		top_out,
	},
	{
		"min",
		NULL,
		NULL,
		min_aggreg,
		min_aggreg,
		NULL,
		int_out,
	},
	{
		"max",
		NULL,
		NULL,
		max_aggreg,
		max_aggreg,
		NULL,
		int_out,
	},
	{
		"minstr",
		str_alloc,
		str_free,
		minstr_aggreg,
		minstr_aggreg,
		NULL,
		str_out,
	},
	{
		"maxstr",
		str_alloc,
		str_free,
		maxstr_aggreg,
		maxstr_aggreg,
		NULL,
		str_out,
	},
	{
		"count",
		NULL,
		NULL,
		count_aggreg,
		count_merge,
		NULL,
		int_out,
	}
};

#define aggreg_descriptors_count ( sizeof(aggreg_descriptors) / sizeof(aggreg_descriptors[0]) )

/*
 * Implements an allocator with no overhead, but memory cannot be freed (except by destroying the whole allocator)
 * May be backed by (hidden) swap files on the filesystem, so it can allocate more than the available physical memory (works best on 64-bits machines)
 */
class mem_alloc
{
private:
	std::string directory;
	std::vector<size_t *> chunks;
	size_t last_alloc_sz;
	size_t min_alloc_sz;
	size_t max_alloc_sz;
	size_t align;
	uint8_t *next_alloc;
	size_t cur_chunk_left;

	int alloc_new_chunk( size_t want )
	{
		if ( last_alloc_sz < min_alloc_sz )
			last_alloc_sz = min_alloc_sz;
		else if ( last_alloc_sz < max_alloc_sz )
			last_alloc_sz *= 2;

		size_t alloc_sz = last_alloc_sz;
		if ( alloc_sz < want + sizeof(size_t) )
			alloc_sz = want + sizeof(size_t);

		int fd = -1;
		int flags = MAP_PRIVATE | MAP_ANONYMOUS;
		if ( directory.size() )
		{
			// allocate a new file mapping in the target directory
			for ( char i = '0' ; i <= '~' ; ++i )	// '0' > '/' which is forbidden in paths
			{
				std::string path = directory + "/tmp_swap_" + i;
				fd = open( path.c_str(), O_RDWR | O_CREAT | O_EXCL );
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
			lseek( fd, alloc_sz-1, SEEK_SET );
			if ( write( fd, "", 1 ) != 1 )
			{
				std::cerr << "mem_alloc: cannot allocate new file: " << strerror( errno ) << std::endl;
				close( fd );
				return 1;
			}

			flags = MAP_SHARED;
		}

		size_t *chunk = (size_t *)mmap( NULL, alloc_sz, PROT_READ | PROT_WRITE, flags, fd, 0 );
		if ( chunk == MAP_FAILED )
		{
			std::cerr << "mem_alloc: cannot mmap: " << strerror( errno ) << std::endl;
			close( fd );
			return 1;
		}
		close( fd );

		// store the chunk size at the beginning of the chunk, for munmap()
		*chunk = alloc_sz;
		chunks.push_back( chunk );

		// setup vars used by alloc()
		next_alloc = (uint8_t *)(chunk + 1);
		cur_chunk_left = alloc_sz - sizeof(size_t);

		return 0;
	}

public:
	mem_alloc( std::string &dir, size_t min = 16*1024*1024, size_t max=256*1024*1024, size_t align=4 ) :
		directory(dir),
		last_alloc_sz(0),
		min_alloc_sz(min),
		max_alloc_sz(max),
		align(align),
		next_alloc(NULL),
		cur_chunk_left(0)
	{
	}

	~mem_alloc()
	{
		for ( unsigned i = 0 ; i < chunks.size() ; ++i )
			munmap( (void*)chunks[ i ], *chunks[ i ] );
	}

	void *alloc( size_t size )
	{
		if ( align > 1 && (size & (align-1)) )
			size += align - (size & (align-1));

		if ( size > cur_chunk_left )
			if ( alloc_new_chunk( size ) )
				return NULL;

		void *ret = (void *)next_alloc;
		next_alloc += size;
		cur_chunk_left -= size;

		return ret;
	}
};


class csv_aggreg
{
private:
	// csv reader line_max
	unsigned line_max;

	// describe one output (aggregated) column
	struct aggreg_col {
		// output column name
		std::string outname;
		// input column name (may be empty)
		std::string colname;
		// offset into the aggregated u_data * blob for this column
		unsigned aggreg_idx;
		// pointer to the aggregation functions
		struct aggreg_descriptor *aggregator;

		explicit aggreg_col() : outname(), colname(), aggreg_idx(0), aggregator(NULL) {}
	};

	// aggregation configuration (list of output columns)
	std::vector< struct aggreg_col > conf;
	unsigned aggreg_data_sz;

	// internal cache: maps input column indexes to a vector of output columns (NULL if input col is unused)
	std::vector< std::vector< struct aggreg_col * > > inv_conf;
	// points to aggreg_cols not listed in inv_conf (ie not linked to an input column)
	std::vector< struct aggreg_col * > inv_conf_other;
	std::vector< unsigned > conf_keys;

	friend struct f_hash;
	struct f_hash
	{
		csv_aggreg *ca;
		explicit f_hash( csv_aggreg *ca ) : ca(ca) {}
		size_t operator()( const u_data * ptr ) const
		{
			size_t hash = 0;
			for ( unsigned i = 0 ; i < ca->conf_keys.size() ; ++i )
				hash ^= murmur3_32( ptr[ ca->conf_keys[ i ] ].str );
			return hash;
		}
	};

	friend struct f_equal;
	struct f_equal
	{
		csv_aggreg *ca;
		explicit f_equal( csv_aggreg *ca ) : ca(ca) {}
		bool operator()( const u_data *p1, const u_data *p2 ) const
		{
			bool match = true;
			for ( unsigned i = 0 ; i < ca->conf_keys.size() ; ++i )
				if ( *p1[ ca->conf_keys[ i ] ].str != *p2[ ca->conf_keys[ i ] ].str )
					match = false;
			return match;
		}
	};

	// aggregated data store
	typedef std::tr1::unordered_set< u_data *, f_hash, f_equal> u_data_aggreg;
	u_data_aggreg aggreg;

	// find an aggregator struct by name (eg "count", "min"...)
	struct aggreg_descriptor *find_aggregator( const std::string &name )
	{
		for ( unsigned j = 0 ; j < aggreg_descriptors_count ; ++j )
			if ( name == aggreg_descriptors[ j ].name )
				return aggreg_descriptors + j;

		return NULL;
	}

	// create a csv_reader for aggregation, read headerline, setup caches from aggreg_conf
	csv_reader *start_reader_aggreg( const char *filename )
	{
		std::vector< std::string > *headers;
		csv_reader *reader = new csv_reader( filename, ',', '"', line_max );

		if ( reader->failed_to_open() )
			goto fail;

		if ( !reader->fetch_line() )
			goto fail;

		headers = reader->parse_line();

		// populate the invert lookup cache from the header line + conf
		inv_conf.clear();
		inv_conf_other.clear();
		inv_conf.resize( headers->size() );
		for ( unsigned i_c = 0 ; i_c < conf.size() ; ++i_c )
		{
			int seen = 0;
			for ( unsigned i_h = 0 ; i_h < headers->size() ; ++i_h )
				if ( str_downcase( conf[ i_c ].colname ) == str_downcase( headers->at( i_h ) ) )
				{
					inv_conf[ i_h ].push_back( &conf[ i_c ] );
					seen = 1;
				}

			if ( !seen )
			{
				if ( conf[ i_c ].colname.size() )
				{
					std::cerr << "Column not found: " << conf[ i_c ].colname << ", skipping file" << std::endl;
					delete headers;
					goto fail;
				}

				inv_conf_other.push_back( &conf[ i_c ] );
			}
		}

		delete headers;

		reader->fetch_line();

		if ( reader->eos() )
			goto fail;

		return reader;

	fail:
		delete reader;
		return NULL;
	}

	// create a csv_reader for merging, ensure columns match
	csv_reader *start_reader_merge( const char *filename )
	{
		std::vector< std::string > *headers = NULL;
		csv_reader *reader = new csv_reader( filename, ',', '"', line_max );

		if ( reader->failed_to_open() )
			goto fail;

		if ( !reader->fetch_line() )
			goto fail;

		headers = reader->parse_line();

		if ( headers->size() != conf.size() )
		{
			std::cerr << "Merge: column count differs, skipping file" << std::endl;
			goto fail;
		}

		for ( unsigned i = 0 ; i < conf.size() ; ++i )
			if ( str_downcase( conf[ i ].outname ) != str_downcase( headers->at( i ) ) )
			{
				std::cerr << "Merge: columns do not match (" << headers->at( i ) << " != " << conf[ i ].colname << "), skipping file" << std::endl;
				goto fail;
			}

		reader->fetch_line();

		if ( reader->eos() )
			goto fail;

		delete headers;

		return reader;

	fail:
		if ( headers )
			delete headers;
		delete reader;
		return NULL;
	}

	// return the pointer for a given key, allocate it if necessary
	// sets *first = 1 if a new buffer was allocated
	u_data *aggreg_find_or_create( u_data *key, int *first )
	{
		u_data_aggreg::const_iterator it = aggreg.find( key );
		if ( it != aggreg.end() )
			return *it;

		*first = 1;

		u_data *val = (u_data *)malloc( aggreg_data_sz );
		if ( !val )
			// TODO return NULL & dump partial content ?
			throw std::bad_alloc();

		for ( unsigned i = 0 ; i < conf.size() ; ++i )
			if ( conf[ i ].aggregator->alloc )
				conf[ i ].aggregator->alloc( val + conf[ i ].aggreg_idx );

		for ( unsigned i = 0 ; i < conf_keys.size() ; ++i )
			*val[ conf_keys[ i ] ].str = *key[ conf_keys[ i ] ].str;

		aggreg.insert( val );

		return val;
	}

public:
	explicit csv_aggreg ( unsigned line_max = 64*1024 ) :
		line_max(line_max),
		aggreg_data_sz(0),
		aggreg(10, f_hash(this), f_equal(this))
	{
	}

	// parse an aggregation descriptor string into self.conf
	// ex:
	//  count()
	//  some_column
	//  out_col=some_col
	//  some_col,min(other_col)
	//  outname1=downcase(col1),outname2=min(col2),outname3=max(col2),outname4=count()
	int parse_aggregate_descriptor( const std::string &aggreg_str )
	{
		std::string outname;
		std::string tmp;
		struct aggreg_col *col = NULL;
		int parens = 0;
		size_t i, start_off = 0;
		char c = 0;

		conf.clear();
		conf_keys.clear();

		for ( i = 0 ; i < aggreg_str.size() ; ++i )
		{
			c = aggreg_str[ i ];

			if ( c == '=' && parens == 0 )
			{
				outname = tmp;
				tmp.clear();
			}
			else if ( c == '(' )
			{
				++parens;

				if ( parens == 1 )
				{
					col = new aggreg_col();

					if ( conf.size() )
						col->aggreg_idx = conf.back().aggreg_idx + 1;

					col->aggregator = find_aggregator( tmp );
					if ( !col->aggregator ) {
						std::cerr << "Invalid aggregator function " << tmp << std::endl;
						delete col;
						return 1;
					}

					tmp.clear();
				}
			}
			else if ( c == ')' )
			{
				--parens;

				if ( parens == 0 )
				{
					if ( col )
					{
						if ( tmp.size() )
							col->colname = tmp;

						if ( outname.size() )
							col->outname = outname;
						else
							col->outname = aggreg_str.substr( start_off, i-start_off+1 );
						outname.clear();

						conf.push_back( *col );
						delete col;
						col = NULL;
					}

					tmp.clear();
				}
			}
			else if ( c == ',' && parens == 0 )
			{
				if ( tmp.size() )
				{
					// colname only, implicit str(colname)
	implicit_str:
					col = new aggreg_col();

					if ( outname.size() )
						col->outname = outname;
					else
						col->outname = aggreg_str.substr( start_off, i-start_off );
					outname.clear();

					if ( conf.size() )
						col->aggreg_idx = conf.back().aggreg_idx + 1;

					col->aggregator = find_aggregator( "str" );
					if ( !col->aggregator )
					{
						std::cerr << "Internal error: no aggregator named \"str\" ?" << std::endl;
						return 1;
					}

					col->colname = tmp;

					conf.push_back( *col );
					delete col;
					col = NULL;

					tmp.clear();
				}

				start_off = i + 1;
			}
			else if ( c != ' ' )
				tmp.push_back( c );
		}

		if ( parens == 0 && tmp.size() )
			goto implicit_str;

		if ( parens != 0 )
		{
			std::cerr << "Syntax error: missing parenthesis in aggregator: " << parens << std::endl;
			return 1;
		}

		if ( !conf.size() )
		{
			std::cerr << "Empty aggregator" << std::endl;
			return 1;
		}

		for ( unsigned i = 0 ; i < conf.size() ; ++i )
			if ( conf[ i ].aggregator->key )
				conf_keys.push_back( i );

		aggreg_data_sz = sizeof( u_data ) * (conf.back().aggreg_idx + 1);

		return 0;
	}


	// read an input file, aggregate the data inside into the global aggregation structure
	void aggregate( const char *filename )
	{
		csv_reader *reader = start_reader_aggreg( filename );
		if ( !reader )
			return;

		// for each input column, hold either the unescaped field or an empty string (if the column is not needed)
		std::vector< std::string > elems;
		elems.resize( inv_conf.size() );

		// for each input column, set to 1 if the column is used as a key for the aggregation
		std::vector< int > key_col;
		key_col.resize( inv_conf.size() );

		for ( unsigned i = 0 ; i < inv_conf.size() ; ++i )
			for ( unsigned j = 0 ; j < inv_conf[ i ].size() ; ++j )
				if ( inv_conf[ i ][ j ]->aggregator->key )
					key_col[ i ] = 1;

		// we store here the current csv line keys
		u_data cur_data[ conf.size() ];
		for ( unsigned i = 0 ; i < conf_keys.size() ; ++i )
			cur_data[ conf_keys[ i ] ].str = new std::string;

		do
		{
			// read elements we're interested in from the line
			char *f = NULL;
			unsigned f_len = 0;
			unsigned idx_in = 0;
			while ( reader->read_csv_field( &f, &f_len ) )
			{
				if ( idx_in >= inv_conf.size() )
					continue;

				if ( inv_conf[ idx_in ].size() )
				{
					char *uf = f;
					unsigned uf_len = f_len;
					elems[ idx_in ].clear();
					reader->unescape_csv_field( &uf, &uf_len, &elems[ idx_in ] );
				}

				++idx_in;
			}

			// populate cur_data[ keys ]
			for ( unsigned i = 0 ; i < inv_conf.size() ; ++i )
				if ( key_col[ i ] )
					for ( unsigned j = 0 ; j < inv_conf[ i ].size() ; ++j )
					{
						struct aggreg_col *a = inv_conf[ i ][ j ];
						if ( !a->aggregator->key )
							continue;
						*cur_data[ a->aggreg_idx ].str = a->aggregator->key( elems[ i ] );
					}

			// aggregate
			int first = 0;
			u_data *aggreg_entry = aggreg_find_or_create( cur_data, &first );
			for ( unsigned i = 0 ; i < inv_conf.size() ; ++i )
				for ( unsigned j = 0 ; j < inv_conf[ i ].size() ; ++j )
				{
					struct aggreg_col *a = inv_conf[ i ][ j ];
					if ( !a->aggregator->aggreg )
						continue;
					a->aggregator->aggreg( aggreg_entry + a->aggreg_idx, &elems[ i ], first );
				}

			// aggregate output columns not in inv_conf (eg count())
			for ( unsigned i = 0 ; i < inv_conf_other.size() ; ++i )
			{
				struct aggreg_col *a = inv_conf_other[ i ];
				if ( !a->aggregator->aggreg )
					continue;
				a->aggregator->aggreg( aggreg_entry + a->aggreg_idx, NULL, first );
			}

		} while ( reader->fetch_line() );

		for ( unsigned i = 0 ; i < conf_keys.size() ; ++i )
			delete cur_data[ conf_keys[ i ] ].str;

		delete reader;
	}


	// read already-aggregated data, integrate it into the global aggregated store (ie the reduce in map-reduce)
	void merge( const char *filename )
	{
		csv_reader *reader = start_reader_merge( filename );
		if ( !reader )
			return;

		// for each input column, hold either the unescaped field or an empty string (if the column is not needed)
		std::vector< std::string > elems;
		elems.resize( conf.size() );

		// we store here the current csv line keys
		u_data cur_data[ conf.size() ];
		for ( unsigned i = 0 ; i < conf_keys.size() ; ++i )
			cur_data[ conf_keys[ i ] ].str = new std::string;

		do
		{
			// read fields
			char *f = NULL;
			unsigned f_len = 0;
			unsigned idx_in = 0;
			while ( reader->read_csv_field( &f, &f_len ) )
			{
				if ( idx_in >= conf.size() )
					continue;

				char *uf = f;
				unsigned uf_len = f_len;
				elems[ idx_in ].clear();
				reader->unescape_csv_field( &uf, &uf_len, &elems[ idx_in ] );
				if ( conf[ idx_in ].aggregator->key )
					*cur_data[ idx_in ].str = conf[ idx_in ].aggregator->key( elems[ idx_in ] );

				++idx_in;
			}

			// aggregate
			int first = 0;
			u_data *aggreg_entry = aggreg_find_or_create( cur_data, &first );
			for ( unsigned i = 0 ; i < conf.size() ; ++i )
				if ( conf[ i ].aggregator->merge )
					conf[ i ].aggregator->merge( aggreg_entry + i, &elems[ i ], first );

		} while ( reader->fetch_line() );

		for ( unsigned i = 0 ; i < conf_keys.size() ; ++i )
			delete cur_data[ conf_keys[ i ] ].str;

		delete reader;
	}


	// dump all aggregated data to an output CSV
	// clears aggreg
	void dump_output( const char *filename )
	{
		output_buffer outbuf( filename, 1024*1024 );

		for ( unsigned i = 0 ; i < conf.size() ; ++i )
		{
			if ( i > 0 )
				outbuf.append( ',' );
			outbuf.append( '"' );
			outbuf.append( conf[ i ].outname );
			outbuf.append( '"' );
		}
		outbuf.append_nl();

		for ( u_data_aggreg::const_iterator it = aggreg.begin() ; it != aggreg.end() ; ++it )
		{
			for ( unsigned i = 0 ; i < conf.size() ; ++i )
			{
				if ( i > 0 )
					outbuf.append( ',' );

				if ( conf[ i ].aggregator->out )
					outbuf.append( conf[ i ].aggregator->out( *it + conf[ i ].aggreg_idx ) );

				if ( conf[ i ].aggregator->free )
					conf[ i ].aggregator->free( *it + conf[ i ].aggreg_idx );
			}
			outbuf.append_nl();
			free( *it );
		}

		aggreg.clear();
	}
};



static const char *usage =
"Usage: csv_aggr <aggregate_spec> <files>\n"
" Options:\n"
"          -V                 display version information and exit\n"
"          -h                 display help (this text) and exit\n"
"          -o <outfile>       specify output file (default=stdout)\n"
"          -L <max line len>  specify maximum line length allowed (default=64k)\n"
"          -m                 inputs are partial outputs from csv_aggr (map-reduce style)\n"
;


static const char *version_info =
"CSV aggregator version " CSV_AGGREG_VERSION "\n"
"Copyright (c) 2014 Yoann Guillot\n"
"Licensed under the WtfPLv2, see http://www.wtfpl.net/\n"
;


int main ( int argc, char * argv[] )
{
	int opt;
	char *outfile = NULL;
	unsigned line_max = 64*1024;
	bool merge = false;

	while ( (opt = getopt(argc, argv, "hVo:L:m")) != -1 )
	{
		switch (opt)
		{
		case 'h':
			std::cout << usage << std::endl;
			return EXIT_SUCCESS;

		case 'V':
			std::cout << version_info << std::endl;
			return EXIT_SUCCESS;

		case 'o':
			outfile = optarg;
			break;

		case 'L':
			line_max = strtoul( optarg, NULL, 0 );
			break;

		case 'm':
			merge = true;
			break;

		default:
			std::cerr << "Unknwon option: " << opt << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
	}

	if ( optind >= argc )
	{
		std::cerr << "No aggregate specified" << std::endl << usage << std::endl;
		return EXIT_FAILURE;
	}

	csv_aggreg aggregator( line_max );

	if ( aggregator.parse_aggregate_descriptor( argv[ optind++ ] ) )
		return EXIT_FAILURE;

	if ( optind >= argc )
	{
		if ( merge )
			aggregator.merge( NULL );
		else
			aggregator.aggregate( NULL );
	}
	else
	{
		for ( int i = optind ; i < argc ; ++i )
			if ( merge )
				aggregator.merge( argv[ i ] );
			else
				aggregator.aggregate( argv[ i ] );
	}

	aggregator.dump_output( outfile );

	return EXIT_SUCCESS;
}
