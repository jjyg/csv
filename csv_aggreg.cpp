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

#include "output_buffer.h"
#include "csv_reader.h"
#include "mmap_alloc.h"
#include "murmur3.h"

#define CSV_AGGREG_VERSION "20140410"

static std::string str_downcase( const std::string &str )
{
	std::string ret;
	for ( unsigned i = 0 ; i < str.size() ; ++i )
		ret.push_back( tolower( str[ i ] ) );
	return ret;
}


/*
 * holds all possible forms of aggregation fields ; update as needed if you add aggregators
 */
union u_data {
	long long ll;
	char *key;
	std::string *str;
	std::vector< std::string > *vec_str;
};

/*
 * list of aggregator functions
 */

static void str_key( char **field, size_t *field_len )
{
	(void)field;
	(void)field_len;
}

static void key_out( u_data *ptr, output_buffer &out )
{
	out.append( '"' );
	size_t len = strlen( ptr->key );
	char *tmp = ptr->key, *tmp2;
	while ( len > 0 && (tmp2 = (char *)memchr( tmp, '"', len )) )
	{
		++tmp2;
		out.append( tmp, tmp2 - tmp );
		out.append( '"' );
		len -= tmp2 - tmp;
		tmp = tmp2;
	}
	if ( len > 0 )
		out.append( tmp, len );
	out.append( '"' );
}

static void downcase_key( char **field, size_t *field_len )
{
	for ( unsigned i = 0 ; i < *field_len ; ++i )
		(*field)[ i ] = tolower( (*field)[ i ] );
}

static void top20_aggreg( u_data *ptr, const std::string *field, int first )
{
	if ( first )
		ptr->vec_str = new std::vector< std::string >;

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
		first = 0;
		last = next + 1;
		next = field->find( ',', last );
	}

	const std::string &tmp = field->substr( last );
	top20_aggreg( ptr, &tmp, first );
}

static void top_out( u_data *ptr, output_buffer &out )
{
	std::string tmp;

	for ( unsigned i = 0 ; i < ptr->vec_str->size() ; ++i )
	{
		if ( i > 0 )
			tmp.push_back( ',' );
		tmp.append( (*ptr->vec_str)[ i ] );
	}

	out.append( csv_reader::escape_csv_string( tmp ) );

	delete ptr->vec_str;
	ptr->vec_str = NULL;
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

static void str_out( u_data *ptr, output_buffer &out )
{
	out.append( csv_reader::escape_csv_string( *ptr->str ) );

	delete ptr->str;
	ptr->str = NULL;
}

static void minstr_aggreg( u_data *ptr, const std::string *field, int first )
{
	if ( first )
		ptr->str = new std::string;

	if ( first || *field < *ptr->str )
		*ptr->str = *field;
}

static void maxstr_aggreg( u_data *ptr, const std::string *field, int first )
{
	if ( first )
		ptr->str = new std::string;

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

static void int_out( u_data *ptr, output_buffer &out )
{
	char buf[16];
	unsigned buf_sz = snprintf( buf, sizeof(buf), "%lld", ptr->ll );
	if ( buf_sz > sizeof(buf) )
		buf_sz = sizeof(buf);
	out.append( buf, buf_sz );
}


/*
 * list of aggregators
 */

struct aggreg_descriptor {
	// aggregator name, used in config/help messages
	const char *name;
	// called during aggregation, ptr is the same as for alloc(), field is the unescaped csv field value, first = 1 if field is the 1st entry to be aggregated here
	void (*aggreg)( u_data *ptr, const std::string *field, int first );
	// called during merge, similar to aggreg, but field points to the result of a previous out(aggreg())
	void (*merge)( u_data *ptr, const std::string *field, int first );
	// determine aggregation key, should append data to key. field is the raw csv field value, it is neither escaped nor unescaped.
	void (*key)( char **k, size_t *klen );
	// called when dumping aggregation results, ptr is the same as for alloc().
	void (*out)( u_data *ptr, output_buffer &out );
} aggreg_descriptors[] =
{
	{
		"str",
		NULL,
		NULL,
		str_key,
		key_out,
	},
	{
		"downcase",
		NULL,
		NULL,
		downcase_key,
		key_out,
	},
	{
		"top20",
		top20_aggreg,
		top20_merge,
		NULL,
		top_out,
	},
	{
		"min",
		min_aggreg,
		min_aggreg,
		NULL,
		int_out,
	},
	{
		"max",
		max_aggreg,
		max_aggreg,
		NULL,
		int_out,
	},
	{
		"minstr",
		minstr_aggreg,
		minstr_aggreg,
		NULL,
		str_out,
	},
	{
		"maxstr",
		maxstr_aggreg,
		maxstr_aggreg,
		NULL,
		str_out,
	},
	{
		"count",
		count_aggreg,
		count_merge,
		NULL,
		int_out,
	}
};

#define aggreg_descriptors_count ( sizeof(aggreg_descriptors) / sizeof(aggreg_descriptors[0]) )

/*
 * an entry in the aggregation hash set
 * corresponds to one output line
 */
struct hash_element
{
	uint64_t hash;
	u_data list[];
};


/*
 * a hash set with low memory overhead
 * insert-only, does not support element deletion
 * actually an array of hash_elements, with quadratic probing (loosely inspired from google sparsehash)
 */
class hashset
{
private:
	uint64_t n_elements;	/* size of the array */
	uint64_t n_elements_inuse;	/* number of elements in use ; should stay < 0.7*n_elements */
	// TODO split this table if it becomes too large
	hash_element **table;

public:
	explicit hashset( uint64_t nelem = 1024 )
	{
		n_elements = nelem;
		n_elements_inuse = 0;
		table = (hash_element **)malloc( nelem * sizeof(*table) );
		if ( !table )
			throw std::bad_alloc();
		memset( table, 0, nelem * sizeof(*table) );
	}

	~hashset()
	{
		free( table );
	}

	friend struct const_iterator;
	struct const_iterator
	{
		const hashset *hash;
		uint64_t idx;

		explicit const_iterator( const hashset *hash = NULL, uint64_t idx = 0 ) : hash( hash ), idx( idx ) {}

		hash_element * operator->() const { return hash->table[ idx ]; }

		bool operator==(const const_iterator& it) const { return hash == it.hash && idx == it.idx; }
		bool operator!=(const const_iterator& it) const { return !(*this == it); }

		const_iterator & operator++()
		{
			while ( ++idx < hash->n_elements )
				if ( hash->table[ idx ] )
					break;
			return *this;
		}
	};

	const_iterator begin() const
	{
		const_iterator it( this, 0 );
		if ( !table[ 0 ] )
			++it;
		return it;
	}

	const_iterator end() const
	{
		return const_iterator( this, n_elements );
	}

	void resize( uint64_t new_nelem )
	{
		hash_element **new_table = (hash_element **)malloc( new_nelem * sizeof(*table) );
		if ( !new_table )
			throw std::bad_alloc();

		memset( new_table, 0, new_nelem * sizeof(*table) );

		for ( uint64_t i = 0 ; i < n_elements ; ++i )
			if ( table[ i ] )
			{
				uint64_t idx = table[ i ]->hash;
				for ( unsigned quad_probe = 0 ; quad_probe < n_elements_inuse ; idx += ++quad_probe )
				{
					hash_element **cur = &new_table[ idx % new_nelem ];
					if ( !*cur )
					{
						*cur = table[ i ];
						break;
					}
				}
			}

		free( table );
		table = new_table;
		n_elements = new_nelem;
	}

	/* look for an entry with a given hash / set of keys, return it or NULL
	 * keys should be in the same order as hash_elem->list, and hold non-NULL for every key
	 */
	hash_element *find( uint64_t hash, const std::vector<char *> key, const std::vector< size_t > key_len )
	{
		uint64_t idx = hash;

		// use arbitrary upper bound to avoid infinite loops with full table ; should never happen unless table is too full
		for ( unsigned quad_probe = 0 ; quad_probe < n_elements_inuse + 1 ; idx += ++quad_probe )
		{
			hash_element *cur = table[ idx % n_elements ];
			if ( !cur )
				// end of collision list
				return NULL;

			if ( cur->hash == hash )
			{
				// check for hash collisions
				bool same = true;
				for ( unsigned i = 0 ; same && i < key.size() ; ++i )
					if ( key[ i ] )
					{
						size_t sz = strlen( cur->list[ i ].key );
						if ( key_len[ i ] != sz ||
						     memcmp( key[ i ], cur->list[ i ].key, sz ) != 0 )
							same = false;
					}

				if ( same )
					return cur;
			}
		}

		std::cerr << "hash table failed ! (find)" << std::endl;
		// XXX resize( 2*n_elements ) ?

		return NULL;
	}

	/* inserts a new entry
	 * may trigger a resize( 2*oldsize )
	 * does not check for a pre-existing entry with the same key, only use after find_entry returned NULL */
	void insert( hash_element *entry )
	{
		uint64_t idx = entry->hash;
		++n_elements_inuse;

		if ( n_elements_inuse > n_elements * 7 / 10 )
			resize( n_elements * 2 );

		for ( unsigned quad_probe = 0 ; quad_probe < n_elements_inuse ; idx += ++quad_probe )
		{
			hash_element **cur = &table[ idx % n_elements ];
			if ( !*cur )
			{
				*cur = entry;
				return;
			}
		}

		std::cerr << "hash table failed ! (insert)" << std::endl;
		// XXX
	}

private:
	hashset ( const hashset& );
	hashset& operator=( const hashset& );
};


class csv_aggreg
{
private:
	// allocator for u_data & keys
	mmap_alloc memalloc;

	// csv reader line_max
	unsigned line_max;

	// describe one output (aggregated) column
	struct aggreg_col {
		// output column name
		std::string outname;
		// input column name (may be empty)
		std::string colname;
		// index into the aggregated u_data * blob for this column ( = index of the entry in conf[] )
		unsigned aggreg_idx;
		// index of the input column used for this output column (may change for each input file)
		int input_col_idx;
		// pointer to the aggregation functions
		struct aggreg_descriptor *aggregator;

		explicit aggreg_col() : outname(), colname(), aggreg_idx(0), input_col_idx(-1), aggregator(NULL) {}
	};

	// aggregation configuration (list of output columns)
	std::vector< struct aggreg_col > conf;

	// aggregated data store
	hashset u_data_aggreg;

	// find an aggregator struct by name (eg "count", "min"...)
	struct aggreg_descriptor *find_aggregator( const std::string &name )
	{
		for ( unsigned j = 0 ; j < aggreg_descriptors_count ; ++j )
			if ( name == aggreg_descriptors[ j ].name )
				return aggreg_descriptors + j;

		return NULL;
	}

	// create a csv_reader for aggregation, read headerline, setup caches from aggreg_conf
	csv_reader *start_reader_aggreg( const char *filename,
			std::vector< std::vector< struct aggreg_col * > > &inv_conf,
			std::vector< struct aggreg_col * > &inv_conf_other )
	{
		std::vector< std::string > *headers;
		csv_reader *reader = new csv_reader( filename, ',', '"', line_max );

		if ( reader->failed_to_open() )
			goto fail;

		if ( !reader->fetch_line() )
			goto fail;

		headers = reader->parse_line();

		// populate the invert lookup cache from the header line + conf
		inv_conf.resize( headers->size() );
		for ( unsigned i_c = 0 ; i_c < conf.size() ; ++i_c )
		{
			conf[ i_c ].input_col_idx = -1;
			for ( unsigned i_h = 0 ; i_h < headers->size() ; ++i_h )
				if ( str_downcase( conf[ i_c ].colname ) == str_downcase( headers->at( i_h ) ) )
				{
					conf[ i_c ].input_col_idx = i_h;
					if ( conf[ i_c ].aggregator->aggreg )
						inv_conf[ i_h ].push_back( &conf[ i_c ] );
				}

			if ( conf[ i_c ].input_col_idx == -1 )
			{
				if ( conf[ i_c ].colname.size() )
				{
					std::cerr << "Column not found: " << conf[ i_c ].colname << ", skipping file" << std::endl;
					delete headers;
					goto fail;
				}

				if ( conf[ i_c ].aggregator->aggreg )
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


	/*
	 * return the pointer for a given set of keys, allocate it if necessary
	 * sets *first = 1 if a new buffer was allocated
	 * copies keys bytes to memalloc
	 */
	hash_element *aggreg_find_or_create( const std::vector< char * > key, const std::vector< size_t > key_len, int *first )
	{
		uint64_t hash = 0;
		for ( unsigned i = 0 ; i < key.size() ; ++i )
			if ( key[ i ] )
				hash = murmur3_64( key[ i ], key_len[ i ], hash );

		hash_element *ret = u_data_aggreg.find( hash, key, key_len );
		if ( ret )
			return ret;

		*first = 1;

		ret = (hash_element *)memalloc.alloc( sizeof(uint64_t) + conf.size() * sizeof(u_data), sizeof(void*) );
		if ( !ret )
			// TODO return NULL & dump partial content ?
			throw std::bad_alloc();

		ret->hash = hash;

		// copy keys
		for ( unsigned i = 0 ; i < key.size() ; ++i )
		{
			if ( key[ i ] )
			{
				char *ptr = (char *)memalloc.alloc( key_len[ i ] + 1, 1 );
				if ( !ptr )
					throw std::bad_alloc();

				memcpy( ptr, key[ i ], key_len[ i ] );
				ptr[ key_len[ i ] ] = 0;
				ret->list[ i ].key = ptr;
			}
		}

		u_data_aggreg.insert( ret );

		return ret;
	}


public:
	explicit csv_aggreg ( const std::string &bigtmp_directory = "", unsigned line_max = 64*1024 ) :
		memalloc( bigtmp_directory ),
		line_max(line_max)
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
					col->aggreg_idx = conf.size();

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
					col->aggreg_idx = conf.size();

					if ( outname.size() )
						col->outname = outname;
					else
						col->outname = aggreg_str.substr( start_off, i-start_off );
					outname.clear();

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

		return 0;
	}


	// read an input file, aggregate the data inside into the global aggregation structure
	void aggregate( const char *filename )
	{
		// internal cache: maps input column indexes to a vector of output columns (NULL if input col is unused)
		std::vector< std::vector< struct aggreg_col * > > inv_conf;
		// points to aggreg_cols not listed in inv_conf (ie not linked to an input column)
		std::vector< struct aggreg_col * > inv_conf_other;

		csv_reader *reader = start_reader_aggreg( filename, inv_conf, inv_conf_other );
		if ( !reader )
			return;

		// hold unescaped field values for the current line
		std::vector< char * > field( inv_conf.size() );
		std::vector< size_t > field_len( inv_conf.size() );

		// hold unescaped key data for the current line
		std::vector< char * > key( conf.size() );
		std::vector< size_t > key_len( conf.size() );

		// current line csv fields
		char *line = NULL;
		std::vector< unsigned > field_off( inv_conf.size() );

		// hold pointers to string that were allocated for unescaping (unusual) ; to be freed at the end of the line
		std::vector< std::string * > str_tofree( inv_conf.size() );

		// maps input column -> output column index for keys
		std::vector< int > key_idx( inv_conf.size(), -1 );
		for ( unsigned i = 0 ; i < conf.size() ; ++i )
			if ( conf[ i ].aggregator->key )
				key_idx[ conf[ i ].input_col_idx ] = i;

		do
		{
			// split line in csv fields
			unsigned f_off = 0;
			unsigned f_len = 0;
			unsigned n_fields = 0;
			while ( reader->read_csv_field( &line, &f_off, &f_len ) )
			{
				if ( n_fields >= inv_conf.size() )
					continue;

				field_off[ n_fields ] = f_off;
				field_len[ n_fields ] = f_len;

				++n_fields;
			}

			if ( n_fields < inv_conf.size() )
			{
				unsigned snap_sz = f_off + f_len;
				if ( snap_sz > 32 )
					snap_sz = 32;
				std::cerr << "Bad field count, skipping line near " << std::string( line, snap_sz ) << std::endl;

				continue;
			}

			// unescape the csv fields we're interested in
			for ( unsigned i = 0 ; i < n_fields ; ++i )
			{
				int ki = key_idx[ i ];

				if ( ki != -1 || inv_conf[ i ].size() )
				{
					char *uf = line + field_off[ i ];
					unsigned ul = field_len[ i ];
					std::string *s = reader->unescape_csv_field( &uf, &ul );

					if ( s )
					{
						str_tofree[ i ] = s;
						uf = (char *)s->data();
						ul = s->size();
					}
					field[ i ] = uf;
					field_len[ i ] = ul;

					if ( ki != -1 )
					{
						key[ ki ] = field[ i ];
						key_len[ ki ] = field_len[ i ];

						conf[ ki ].aggregator->key( &key[ ki ], &key_len[ ki ] );
					}
				}
			}

			// aggregate
			int first = 0;
			hash_element *entry = aggreg_find_or_create( key, key_len, &first );

			for ( unsigned i = 0 ; i < n_fields ; ++i )
			{
				// TODO aggreg( fptr, flen )
				if ( inv_conf[ i ].size() )
				{
					std::string str( field[ i ], field_len[ i ] );
					for ( unsigned j = 0 ; j < inv_conf[ i ].size() ; ++j )
					{
						struct aggreg_col *a = inv_conf[ i ][ j ];
						a->aggregator->aggreg( entry->list + a->aggreg_idx, &str, first );
					}
				}

				if ( str_tofree[ i ] )
				{
					delete str_tofree[ i ];
					str_tofree[ i ] = NULL;
				}
			}

			// aggregate output columns not in inv_conf (eg count())
			for ( unsigned i = 0 ; i < inv_conf_other.size() ; ++i )
			{
				struct aggreg_col *a = inv_conf_other[ i ];
				a->aggregator->aggreg( entry->list + a->aggreg_idx, NULL, first );
			}

		} while ( reader->fetch_line() );

		delete reader;
	}


	// read already-aggregated data, integrate it into the global aggregated store (ie the reduce in map-reduce)
	void merge( const char *filename )
	{
		csv_reader *reader = start_reader_merge( filename );
		if ( !reader )
			return;

		// hold unescaped field values for the current line
		std::vector< char * > field( conf.size() );
		std::vector< size_t > field_len( conf.size() );

		// hold unescaped key data for the current line
		std::vector< char * > key( conf.size() );
		std::vector< size_t > key_len( conf.size() );

		// current line csv fields
		char *line = NULL;
		std::vector< unsigned > field_off( conf.size() );

		// hold pointers to string that were allocated for unescaping (unusual) ; to be freed at the end of the line
		std::vector< std::string * > str_tofree( conf.size() );

		do
		{
			// read fields
			unsigned f_off = 0;
			unsigned f_len = 0;
			unsigned n_fields = 0;
			while ( reader->read_csv_field( &line, &f_off, &f_len ) )
			{
				if ( n_fields >= conf.size() )
					continue;

				field_off[ n_fields ] = f_off;
				field_len[ n_fields ] = f_len;

				++n_fields;
			}
			if ( n_fields < conf.size() )
			{
				unsigned snap_sz = f_off + f_len;
				if ( snap_sz > 32 )
					snap_sz = 32;
				std::cerr << "Bad field count, skipping line near " << std::string( line, snap_sz ) << std::endl;

				continue;
			}

			for ( unsigned i = 0 ; i < n_fields ; ++i )
			{
				char *uf = line + field_off[ i ];
				unsigned ul = field_len[ i ];
				std::string *s = reader->unescape_csv_field( &uf, &ul );

				if ( s )
				{
					str_tofree[ i ] = s;
					uf = (char *)s->data();
					ul = s->size();
				}
				field[ i ] = uf;
				field_len[ i ] = ul;

				if ( conf[ i ].aggregator->key )
				{
					key[ i ] = field[ i ];
					key_len[ i ] = field_len[ i ];

					conf[ i ].aggregator->key( &key[ i ], &key_len[ i ] );
				}
			}

			// aggregate
			int first = 0;
			hash_element *entry = aggreg_find_or_create( key, key_len, &first );

			for ( unsigned i = 0 ; i < n_fields ; ++i )
			{
				if ( conf[ i ].aggregator->merge )
				{
					std::string s( field[ i ], field_len[ i ] );
					conf[ i ].aggregator->merge( entry->list + i, &s, first );
				}

				if ( str_tofree[ i ] )
				{
					delete str_tofree[ i ];
					str_tofree[ i ] = NULL;
				}
			}

		} while ( reader->fetch_line() );

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

		for ( hashset::const_iterator it = u_data_aggreg.begin() ; it != u_data_aggreg.end() ; ++it )
		{
			for ( unsigned i = 0 ; i < conf.size() ; ++i )
			{
				if ( i > 0 )
					outbuf.append( ',' );

				if ( conf[ i ].aggregator->out )
					conf[ i ].aggregator->out( it->list + i, outbuf );
			}
			outbuf.append_nl();
		}
	}

private:
	csv_aggreg ( const csv_aggreg& );
	csv_aggreg& operator=( const csv_aggreg& );
};



static const char *usage =
"Usage: csv_aggr <aggregate_spec> <files>\n"
" Options:\n"
"          -V                 display version information and exit\n"
"          -h                 display help (this text) and exit\n"
"          -o <outfile>       specify output file (default=stdout)\n"
"          -L <max line len>  specify maximum line length allowed (default=64k)\n"
"          -m                 inputs are partial outputs from csv_aggr (map-reduce style)\n"
"          -d <directory>     directory to store temporary swap files ; should have lots of free space\n"
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
	std::string bigtmpdir = "";

	while ( (opt = getopt(argc, argv, "hVo:L:md:")) != -1 )
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

		case 'd':
			bigtmpdir = std::string( optarg );
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

	csv_aggreg aggregator( bigtmpdir, line_max );

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
