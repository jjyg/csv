#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <errno.h>
#include <regex.h>
#include <tr1/unordered_set>

#define CSV_TOOL_VERSION "20140312"

// wraps an istream, provide an efficient interface to read lines
// skips UTF-8 BOM
// interprets UTF-16 BOMs, return iso codepoints - out of range characters are converted to '?'
class line_reader
{
private:
	std::istream *input;
	bool should_delete_input;
	bool badfile;

	// maximum line length
	unsigned buf_cur;
	unsigned buf_end;
	unsigned buf_size;
	char *buf;

	// bitmask
	enum {
		INPUT_FILTER_UTF16BE=1,
		INPUT_FILTER_UTF16LE=2,
	};
	int input_filter;

	// convert utf16 codepoints inplace in buf
	// return the offset after the last valid character converted
	unsigned filter_input ( unsigned off_start, unsigned off_end )
	{
		if ( input_filter & (INPUT_FILTER_UTF16BE | INPUT_FILTER_UTF16LE) )
		{
			unsigned off_in = off_start;
			unsigned off_out = off_start;
			unsigned high = 0, low = 1;
			if ( input_filter & INPUT_FILTER_UTF16LE )
				high = 1, low = 0;

			while ( off_in < off_end )
			{
				if ( buf[ off_in + high ] )
				{
					buf[ off_out++ ] = '?';
					off_in += 2;
				}
				else
				{
					buf[ off_out++ ] = buf[ off_in + low ];
					off_in += 2;
				}
			}

			return off_out;
		}

		return off_end;
	}

	// memmove buf_cur to buf_start, fill buf_end..buf_size with freshly read data
	// convert utf16 according to input_filter
	void refill_buffer ( )
	{
		if ( buf_cur > 0 )
		{
			if ( buf_end < buf_cur )
				buf_end = buf_cur;

			memmove( buf, buf + buf_cur, buf_end - buf_cur );
			buf_end -= buf_cur;
			buf_cur = 0;
		}

		if ( buf_end < buf_size )
		{
			unsigned toread = buf_size - buf_end;
			unsigned old_end = buf_end;
			if ( input_filter & (INPUT_FILTER_UTF16BE | INPUT_FILTER_UTF16LE) )
				toread -= toread & 1;

			if ( toread == 0 )
				return;

			input->read( buf + buf_end, toread );
			buf_end += input->gcount();
			if (buf_end > buf_size)
				buf_end = buf_cur;	// just in case

			if ( input_filter )
				buf_end = filter_input( old_end, buf_end );
		}
	}

public:
	bool failed_to_open ( ) const
	{
		return badfile;
	}

	// return true if no more data is available from input
	bool eos ( ) const
	{
		if ( input->good() )
			return false;

		if ( buf_cur < buf_end )
			return false;

		return true;
	}

	explicit line_reader ( const char *filename, const unsigned line_max = 64*1024 ) :
		badfile(false),
		buf_cur(0),
		buf_end(0),
		buf_size(line_max),
		input_filter(0)
	{
		buf = new char[buf_size];

		if ( filename && filename[ 0 ] == '-' && filename[ 1 ] == 0 )
		{
			should_delete_input = false;
			input = &std::cin;
		}
		else if ( filename )
		{
			should_delete_input = true;
			input = new std::ifstream(filename);
			if ( ! *input )
			{
				std::cerr << "Cannot open " << filename << ": " << strerror( errno ) << std::endl;
				badfile = true;
				return;
			}
		}
		else if ( isatty( 0 ) )
		{
			std::cerr << "Won't read from <stdin>, is a tty. To force, use '-'." << std::endl;
			badfile = true;
			return;
		}
		else
		{
			should_delete_input = false;
			input = &std::cin;
		}

		input->read( buf, (buf_size > 4096 ? buf_size/16 : buf_size) );
		buf_end = input->gcount();

		if ( buf_end >= 3 && buf[0] == '\xef' && buf[1] == '\xbb' && buf[2] == '\xbf' )
		{
			// discard utf-8 BOM
			buf_cur += 3;
		}
		else if ( buf_end >= 2 && buf[0] == '\xfe' && buf[1] == '\xff')
		{
			// utf-16be BOM
			buf_cur += 2;
			input_filter = INPUT_FILTER_UTF16BE;
			buf_end = filter_input( buf_cur, buf_end );
		}
		else if ( buf_end >= 2 && buf[0] == '\xff' && buf[1] == '\xfe')
		{
			// utf-16le BOM
			buf_cur += 2;
			input_filter = INPUT_FILTER_UTF16LE;
			buf_end = filter_input( buf_cur, buf_end );
		}
	}

	~line_reader ( )
	{
		if ( should_delete_input )
		       delete input;

		delete[] buf;
	}

	// read one line from input, starting at buf_cur
	// returns the line start in line_start and the line length in line_length
	// line_length includes the newline character(s)
	// the line is not null-terminated
	// internally, advances buf_cur to the beginning of next line
	// returns false iff line_start = NULL, happens after EOF or on lines > line_max
	// eof is considered as a newline
	// the returned pointer is only valid until the next call to read_line
	bool read_line ( char* *line_start, unsigned *line_length )
	{
		char *nl = NULL;

		if ( buf_cur < buf_end )
			nl = (char*)memchr( (void*)(buf + buf_cur), '\n', buf_end - buf_cur );

		// newline found ?
		if ( nl )
		{
			*line_start = buf + buf_cur;
			*line_length = nl + 1 - *line_start;

			buf_cur += *line_length;
			if (buf_cur > buf_end)
				buf_cur = buf_end;	// just in case

			return true;
		}

		// end of file ?
		if ( !input->good() )
		{
			if ( buf_cur < buf_end )
			{
				*line_start = buf + buf_cur;
				*line_length = buf_end - buf_cur;
				buf_cur = buf_end;

				return true;
			}
			else
			{
				*line_start = NULL;
				*line_length = 0;

				return false;
			}
		}

		// slide existing buffer, read more from input, and retry
		if ( buf_cur > 0 )
		{
			refill_buffer();

			return read_line( line_start, line_length );
		}

		// no newline in whole buffer
		*line_start = NULL;
		*line_length = 0;

		std::string sample( buf, (buf_end > 64 ? 64 : buf_end) );
		std::cerr << "Line too long, near '" << sample << "'" << std::endl;

		// slide buffer anyway, to avoid infinite loop in badly written clients
		buf_cur = 0;
		buf_end = 0;
		refill_buffer();

		return false;
	}

	// read raw data (dont mix with read_line)
	void read ( char* *ptr, unsigned *len )
	{
		if ( *len > ( buf_end - buf_cur ) )
			refill_buffer();

		if ( *len > ( buf_end - buf_cur ) )
			*len = buf_end - buf_cur;

		*ptr = buf + buf_cur;
		buf_cur += *len;
	}

private:
	line_reader ( const line_reader& );
	line_reader& operator=( const line_reader& );
};

class output_buffer
{
private:
	std::ostream *output;
	bool should_delete_output;
	bool badfile;

	unsigned buf_end;
	unsigned buf_size;
	char *buf;

public:
	bool failed_to_open ( ) const
	{
		return badfile;
	}

	void flush ( )
	{
		if ( buf_end > 0 )
		{
			output->write( buf, buf_end );
			buf_end = 0;
		}

		output->flush();
	}

	void append ( const char *s, const unsigned len )
	{
		unsigned len_left = len;

		while ( len_left >= buf_size - buf_end )
		{
			if ( buf_end < buf_size )
				memcpy( buf + buf_end, s, buf_size - buf_end );
			output->write( buf, buf_size );
			len_left -= buf_size - buf_end;
			s += buf_size - buf_end;
			buf_end = 0;
		}

		if ( len_left > 0 )
		{
			memcpy( buf + buf_end, s, len_left );
			buf_end += len_left;
		}
	}

	void append ( const std::string *str )
	{
		append( str->data(), str->size() );
	}

	void append ( const std::string &str )
	{
		append( &str );
	}

	void append ( const char c )
	{
		if ( buf_end < buf_size - 1 )
			buf[ buf_end++ ] = c;
		else
			append( &c, 1 );
	}

	void append_nl ( )
	{
		append( '\r' );
		append( '\n' );
	}

	explicit output_buffer ( const char *filename, const unsigned buf_size = 64*1024 ) :
		badfile(false),
		buf_end(0),
		buf_size(buf_size)
	{
		buf = new char[buf_size];

		if ( filename )
		{
			should_delete_output = true;
			output = new std::ofstream(filename);
			if ( ! *output )
			{
				std::cerr << "Cannot open " << filename << ": " << strerror( errno ) << std::endl;
				badfile = true;
				return;
			}
		}
		else
		{
			should_delete_output = false;
			output = &std::cout;
		}
	}

	~output_buffer ( )
	{
		flush();

		if ( should_delete_output )
			delete output;

		delete[] buf;
	}

private:
	output_buffer ( const output_buffer& );
	output_buffer& operator=( const output_buffer& );
};

class csv_reader
{
private:
	line_reader *input_lines;
	unsigned line_max;
	char *line_copy;
	bool failed;

	char sep;
	char quot;

	char *cur_line;
	unsigned cur_line_length;
	unsigned cur_line_length_nl;
	unsigned cur_field_offset;

	// set cur_line_length from cur_line_length_nl, trim \r\n
	void trim_newlines ( )
	{
		cur_line_length = cur_line_length_nl;

		if ( cur_line_length > 0 && cur_line[ cur_line_length - 1 ] == '\n' )
			cur_line_length--;

		if ( cur_line_length > 0 && cur_line[ cur_line_length - 1 ] == '\r' )
			cur_line_length--;
	}

public:
	bool failed_to_open ( ) const
	{
		return input_lines->failed_to_open();
	}

	// return true if no more data is available from input_lines
	bool eos ( ) const
	{
		if ( failed )
			return true;

		if ( cur_field_offset <= cur_line_length )
			return false;

		if ( ! input_lines->eos() )
			return false;

		return true;
	}

	// reset cur_field_offset to 0, so that subsequent read_csv_field() re-output the current row fields
	void reset_cur_field_offset ( )
	{
		cur_field_offset = 0;
	}

	// line_max is passed to the line_reader, it is also the limit for a full csv row (that may span many lines)
	explicit csv_reader ( const char *filename, const char sep = ',', const char quot = '"', const unsigned line_max = 64*1024 ) :
		line_max(line_max),
		line_copy(NULL),
		failed(false),
		sep(sep),
		quot(quot),
		cur_line(NULL),
		cur_line_length(0),
		cur_line_length_nl(0),
		cur_field_offset(1)
	{
		input_lines = new line_reader(filename, line_max);
	}

	~csv_reader ( )
	{
		if ( line_copy )
			delete[] line_copy;

		delete input_lines;
	}

	// read one line from input_lines
	// invalidates previous read_csv_field pointers
	// return false after EOF
	bool fetch_line ( )
	{
		if ( failed )
			return false;

		if ( input_lines->read_line( &cur_line, &cur_line_length_nl ) )
		{
			cur_field_offset = 0;
			trim_newlines();
			
			return true;
		}

		failed = true;
		cur_field_offset = 1;
		cur_line_length = cur_line_length_nl = 0;

		return false;
	}

	// read one csv field from the current line
	// returns false if no more fields are available in the line, or if there is a syntax error (unterminated quote, end quote followed by neither a quote nor a separator)
	// returns a pointer to the line start, the offset of the current field, and its length
	// the 'field_offset' returned by previous calls for the same line is still valid relative to the new 'line_start' (which may change if one field crosses a line boundary, in that case the lines are copied into an internal buffer)
	bool read_csv_field ( char* *line_start, unsigned *field_offset, unsigned *field_length )
	{
		if ( failed )
			return false;

		if ( cur_field_offset > cur_line_length )
			return false;

		*field_offset = cur_field_offset;
		*line_start = cur_line;

		if ( cur_field_offset == cur_line_length )
		{
			// line ends in a coma
			++cur_field_offset;
			*field_length = 0;

			return true;
		}

		if ( cur_line[ cur_field_offset ] != quot )
		{
			// unquoted field
			char *psep = (char*)memchr( (void*)(cur_line + cur_field_offset), sep, cur_line_length - cur_field_offset );

			if ( psep )
			{
				*field_length = psep - (cur_line + cur_field_offset);
				cur_field_offset += *field_length + 1;
			}
			else
			{
				*field_length = cur_line_length - cur_field_offset;
				cur_field_offset += *field_length + 1;
			}

			return true;
		}

		// quoted field
		*field_length = 1;	// field length until now, including opening quote
		while (1)
		{
			char *pquot = NULL;

			if ( cur_field_offset + *field_length < cur_line_length )
				pquot = (char*)memchr( (void*)(cur_line + cur_field_offset + *field_length), quot,
						cur_line_length - (cur_field_offset + *field_length) );

			if ( pquot )
			{
				// found end quote
				*field_length = pquot - (cur_line + cur_field_offset) + 1;

				if ( cur_field_offset + *field_length < cur_line_length )
				{
					if ( cur_line[ cur_field_offset + *field_length ] == sep )
					{
						// end of field
						cur_field_offset += *field_length + 1;
						return true;
					}

					if ( cur_line[ cur_field_offset + *field_length ] == quot )
					{
						// escaped quote
						++*field_length;
						continue;
					}

					// syntax error
					cur_field_offset += *field_length;
					return false;
				}

				// end quote was at end of line
				cur_field_offset += *field_length + 1;
				return true;
			}

			// no closing quote on current input_lines line
			if ( cur_line != line_copy )
			{
				// copy current line to internal buffer
				if ( !line_copy )
					line_copy = new char[line_max];

				memcpy( line_copy, cur_line, cur_line_length_nl );

				*line_start = cur_line = line_copy;
			}

			char *next_line = NULL;
			unsigned next_line_length_nl = 0;

			input_lines->read_line( &next_line, &next_line_length_nl );

			if ( next_line && cur_line_length_nl + next_line_length_nl <= line_max )
			{
				// next line fits in internal buffer: append
				memcpy( line_copy + cur_line_length_nl, next_line, next_line_length_nl );

				*field_length = cur_line_length_nl - cur_field_offset;

				cur_line_length_nl += next_line_length_nl;

				trim_newlines();
			}
			else
			{
				// reached end of input_lines / line_max with no end quote: return syntax error
				if ( next_line )
				{
					std::string sample( cur_line, 64 );
					std::cerr << "Csv row too long (maybe unclosed quote?) near '" << sample << "'" << std::endl;
				}

				if ( ! input_lines->eos() )
					std::cerr << "Ignoring end of file" << std::endl;

				failed = true;
				cur_field_offset = cur_line_length + 1;
				return false;
			}
		}
	}

	// same as read_csv_field ( line_start, field_offset, field_length ) with simpler args
	// returned values are only valid until the next call to this function (with the 3-args version, it is valid until fetch_line())
	bool read_csv_field ( char* *field_start, unsigned *field_length )
	{
		char *line_start = NULL;
		unsigned field_offset = 0;

		if ( ! read_csv_field( &line_start, &field_offset, field_length ) )
			return false;

		*field_start = line_start + field_offset;

		return true;
	}

	// return an unescaped csv field
	// if unescaped is not NULL, fill it with unescaped data (data appended, string should be empty on call)
	// if unescaped is NULL, and field has no escaped quote, only update field_start and field_length to reflect unescaped data (zero copy)
	// if unescaped is NULL, and field has escaped quotes, allocate a new string and fill it with unescaped data. Caller should free it.
	// returns a pointer to unescaped
	// on return, if unescaped is not NULL, field_start and field_length are undefined.
	std::string* unescape_csv_field ( char* *field_start, unsigned *field_length, std::string* unescaped = NULL ) const
	{
		if ( *field_length <= 0 )
			return unescaped;

		if ( (*field_start)[ 0 ] != quot )
		{
			if ( unescaped )
				unescaped->append( *field_start, *field_length );

			return unescaped;
		}

		++*field_start;
		--*field_length;
		--*field_length;

		while (1)
		{
			char *pquot = NULL;

			if ( *field_length > 0 )
				pquot = (char*)memchr( (void*)*field_start, quot, *field_length );

			if ( pquot )
			{
				// found quote: must be an escape
				if ( ! unescaped )
					unescaped = new std::string;

				// append string start, including 1 quot
				unescaped->append( *field_start, pquot - *field_start + 1 );
				*field_length -= pquot - *field_start + 2;
				*field_start = pquot + 2;
			}
			else
			{
				if ( unescaped )
					unescaped->append( *field_start, *field_length );

				return unescaped;
			}
		}
	}

	// return the escaped version of an unescaped string
	std::string escape_csv_field ( const std::string &str ) const
	{
		std::string ret;

		if ( str.size() == 0 )
			return ret;

		ret.push_back( quot );

		size_t last = 0, next = str.find( quot );
		while ( next != std::string::npos )
		{
			ret.append( str.substr( last, next-last ) );
			ret.push_back( quot );
			ret.push_back( quot );

			last = next + 1;
			next = str.find( quot, last );
		}

		ret.append( str.substr( last ) );
		ret.push_back( quot );

		return ret;
	}

	// parse the current csv line (after a call to fetch_line())
	// return a pointer to a vector of unescaped strings, should be delete by caller
	std::vector<std::string>* parse_line ( )
	{
		std::vector<std::string> *vec = new std::vector<std::string>;
		char *field_start = NULL;
		unsigned field_length = 0;

		while ( read_csv_field( &field_start, &field_length ) )
		{
			std::string unescaped;
			unescape_csv_field( &field_start, &field_length, &unescaped );
			vec->push_back( unescaped );
		}

		return vec;
	}

	// read raw data (dont mix with read_*)
	void read ( char* *ptr, unsigned *len )
	{
		if ( cur_field_offset < cur_line_length_nl )
		{
			if ( *len > ( cur_line_length_nl - cur_field_offset ) )
				*len = cur_line_length_nl - cur_field_offset;
			*ptr = cur_line + cur_field_offset;
			cur_field_offset += *len;
		}
		else
		{
			input_lines->read( ptr, len );
		}
	}

private:
	csv_reader ( const csv_reader& );
	csv_reader& operator=( const csv_reader& );
};

enum {
	NO_HEADERLINE,
	RE_NOCASE,
	RE_INVERT,
	UNIQ_COLS,
	EXTRACT_ZERO,
};

class csv_tool
{
private:
	char sep;
	char sep_out;
	char quot;
	unsigned line_max;
public:
	unsigned csv_flags;
private:

#define HAS_FLAG(f) ( csv_flags & ( 1 << f ) )

	output_buffer *outbuf;

	csv_reader *reader;
	std::vector<std::string> *headers;
	std::vector<int> indexes;
	std::vector< std::vector<unsigned> > inv_indexes;
	unsigned max_index;
	std::string out_colspec;


	void cleanup ( )
	{
		if ( reader )
		{
			delete reader;
			reader = NULL;
		}

		if ( headers )
		{
			delete headers;
			headers = NULL;
		}

		indexes.clear();
		inv_indexes.clear();
		out_colspec.clear();
	}

	void count_max_index ( )
	{
		if ( headers )
		{
			max_index = headers->size();
			return;
		}

		// count columns on the current row
		char *ptr = NULL;
		unsigned len = 0;

		max_index = 0;
		while ( reader->read_csv_field( &ptr, &len ) )
			++max_index;

		// reset internal ptr for subsequent read_csv_fields
		reader->reset_cur_field_offset();
	}

	// parse an unsigned long long
	// return 0 on invalid character
	// handle 0x prefix
	int str_ull( const std::string &str, unsigned long long *ret ) const
	{
		*ret = 0;

		if ( str.size() > 2 && str[0] == '0' && str[1] == 'x' )
		{
			for ( unsigned i = 2 ; i < str.size() ; ++i )
			{
				if ( (*ret >> 60) > 0 )
					return 0;
				else if ( str[i] >= '0' && str[i] <= '9' )
					*ret = *ret * 16 + (str[i] - '0');
				else if ( str[i] >= 'A' && str[i] <= 'F' )
					*ret = *ret * 16 + (str[i] - 'A' + 10);
				else if ( str[i] >= 'a' && str[i] <= 'f' )
					*ret = *ret * 16 + (str[i] - 'a' + 10);
				else
					return 0;
			}
		}
		else
		{
			for ( unsigned i = 0 ; i < str.size() ; ++i )
			{
				if ( (*ret >> 60) > 0 )
					return 0;
				else if ( str[i] >= '0' && str[i] <= '9' )
					*ret = *ret * 10 + (str[i] - '0');
				else
					return 0;
			}
		}

		return 1;
	}

	int str_ul( const std::string &str, unsigned long *ret ) const
	{
		unsigned long long ull;
		*ret = 0;

		if ( ! str_ull( str, &ull ) )
			return 0;

		if ( (ull >> 32) > 0 )
			return 0;

		*ret = (unsigned long)ull;

		return 1;
	}

	// return the index of the string str in the string vector headers (case insensitive)
	// checks for numeric indexes if not found (decimal, [0-9]+), < max_index
	// return -1 if not found
	int parse_index_uint( const std::string &str ) const
	{
		if ( str.size() == 0 )
			return -1;

		if ( headers )
			for ( unsigned i = 0 ; i < headers->size() ; ++i )
				if ( ! strcasecmp( str.c_str(), (*headers)[ i ].c_str() ) )
					return i;

		unsigned long ret;
		if ( ! str_ul( str, &ret ) )
			return -1;

		if ( ret < max_index )
			return ret;

		return -1;
	}

	// Parse a colspec string (coma-separated list of column names), populate 'indexes'
	//
	// colspec is a coma-separated list of column names, or a coma-separated list of column indexes (numeric, 0-based)
	// colspec may include ranges of columns, with "begin-end". Omit "begin" to start at 0, omit "end" to finish at last column.
	//
	// if a column is not found, its index is set as -1. For ranges, if begin or end is not found, add a single -1 index.
	// without a header list, the current line is parsed and reset to count the fields per row
	//
	// populates out_colspec with the expanded ranges, keeps colnames (including unknown cols) when possible
	void parse_colspec( const std::string &colspec_str )
	{
		// parse colspec_str: split on comas
		std::vector<std::string> colspec_vec;

		if ( colspec_str.size() > 0 )
		{
			size_t off = 0, idx = -1;
			while ( (idx = colspec_str.find( ',', off )) != std::string::npos )
			{
				colspec_vec.push_back( colspec_str.substr( off, idx-off ) );
				off = idx + 1;
			}
			colspec_vec.push_back( colspec_str.substr( off ) );
		}

		// uniq_cols stuff: list colnames directly specified in colspec
		std::vector<int> direct_cols;
		if ( HAS_FLAG( UNIQ_COLS ) )
			for ( unsigned i = 0 ; i < colspec_vec.size() ; ++i )
			{
				int idx = parse_index_uint( colspec_vec[ i ] );
				if ( idx >= 0 )
					direct_cols.push_back( idx );
			}


		// generate indexes, handle ranges in colspec_vec
		for ( unsigned i = 0 ; i < colspec_vec.size() ; ++i )
		{
			int idx = parse_index_uint( colspec_vec[ i ] );

			if ( idx != -1 )
			{
				indexes.push_back( idx );

				if ( i > 0 )
					out_colspec.push_back( ',' );
				out_colspec.append( colspec_vec[ i ] );
			}
			else
			{
				// check for ranges
				// handle col names including '-'
				size_t dash_off = -1;

				while (1)
				{
					dash_off = colspec_vec[ i ].find( '-', dash_off + 1 );
					if ( dash_off == std::string::npos )
					{
						std::cerr << "Column not found: " << colspec_vec[ i ] << std::endl;
						indexes.push_back( -1 );

						if ( i > 0 )
							out_colspec.push_back( ',' );
						out_colspec.append( colspec_vec[ i ] );

						break;
					}

					int min, max;
					if ( dash_off == 0 )
						min = 0;
					else
						min = parse_index_uint( colspec_vec[ i ].substr( 0, dash_off ) );

					if ( dash_off == colspec_vec[ i ].size() - 1 )
						max = max_index - 1;
					else
						max = parse_index_uint( colspec_vec[ i ].substr( dash_off + 1 ) );

					if ( min >= 0 && max >= 0 )
					{
						for ( int range_idx = min ; range_idx <= max ; ++range_idx )
						{
							bool in_direct = false;
							for ( unsigned j = 0 ; j < direct_cols.size() ; ++j )
								if ( direct_cols[ j ] == range_idx )
									in_direct = true;

							if ( ! in_direct )
							{
								indexes.push_back( range_idx );

								if ( out_colspec.size() > 0 )
									out_colspec.push_back( ',' );

								if ( headers && (unsigned)range_idx < headers->size() )
									out_colspec.append( (*headers)[ range_idx ] );
								else
									out_colspec.append( ull_str( range_idx ) );
							}
						}

						break;
					}
				}
			}
		}

		// inv[ input col idx ] = [ out col idx, out col idx ]
		inv_indexes.resize( max_index );
		for ( unsigned idx_out = 0 ; idx_out < indexes.size() ; ++idx_out )
		{
			int idx_in = indexes[ idx_out ];

			if ( idx_in == -1 )
				continue;

			inv_indexes[ idx_in ].push_back( idx_out );
		}
	}

	// create a csv reader, populate indexes from colspec
	// returns true if everything is fine
	bool start_reader ( const std::string &colspec, const char *filename )
	{
		cleanup();

		reader = new csv_reader( filename, sep, quot, line_max );

		if ( reader->failed_to_open() )
		{
			cleanup();
			return false;
		}

		if ( ! HAS_FLAG( NO_HEADERLINE ) )
		{
			if ( ! reader->fetch_line() )
			{
				cleanup();
				return false;
			}

			headers = reader->parse_line();
		}

		reader->fetch_line();
		count_max_index();

		parse_colspec( colspec );

		return true;
	}

	// split a string "k1=v1,k2=v2,k3=v3" into vectors [k1, k2, k3] and [v1, v2, v3]
	// k may be omitted with -H
	bool split_colvalspec( const std::string &colval, std::vector<std::string> *cols, std::vector<std::string> *vals )
	{
		size_t off = 0;

		while (1)
		{
			size_t eq_off = colval.find( '=', off );
			if ( eq_off == std::string::npos )
			{
				if ( ! HAS_FLAG( NO_HEADERLINE ) )
				{
					std::cerr << "Invalid colval: no '=' after " << colval.substr( off ) << std::endl;
					return false;
				}

				cols->push_back( colval.substr( 0, 0 ) );
			}
			else
			{
				cols->push_back( colval.substr( off, eq_off - off ) );

				off = eq_off + 1;
			}

			size_t next_off = colval.find( ',', off );
			if ( next_off == std::string::npos )
			{
				vals->push_back( colval.substr( off ) );
				return true;
			}

			vals->push_back( colval.substr( off, next_off - off ) );

			off = next_off + 1;
		}
	}

	std::string ull_str ( const unsigned long long nr, const char *fmt = "%llu" )
	{
		char buf[16];
		unsigned buf_sz = snprintf( buf, sizeof(buf), fmt, nr );
		if ( buf_sz > sizeof(buf) )
			buf_sz = sizeof(buf);

		std::string ret( buf, buf_sz );
		return ret;
	}

	std::string str_downcase( const std::string &str )
	{
		std::string ret;

		for ( unsigned i = 0 ; i < str.size() ; ++i )
			ret.push_back( tolower( str[ i ] ) );

		return ret;
	}

public:
	explicit csv_tool ( output_buffer *outbuf, char sep = ',', char sep_out = ',', char quot = '"', unsigned line_max = 64*1024, unsigned csv_flags = 0 ) :
		sep(sep),
		sep_out(sep_out),
		quot(quot),
		line_max(line_max),
		csv_flags(csv_flags),
		outbuf(outbuf),
		reader(NULL),
		headers(NULL),
		max_index(0)
	{
		indexes.clear();
		inv_indexes.clear();
		out_colspec.clear();
	}

	~csv_tool ( )
	{
		cleanup();
	}


	// read one column name (specified in colspec), and dump to outbuf every unescaped row field content for this column
	void extract ( const std::string &colspec, const char *filename )
	{
		if ( ! start_reader( colspec, filename ) )
			return;

		if ( indexes.size() != 1 || indexes[0] < 0 )
		{
			std::cerr << "Invalid colspec" << std::endl;
			return;
		}

		if ( reader->eos() )
			return;

		int zero = 0;
		if ( HAS_FLAG( EXTRACT_ZERO ) )
			zero = 1;	// lol!

		do
		{
			char *ptr = NULL;
			unsigned len = 0;
			unsigned idx_in = 0;

			while ( reader->read_csv_field( &ptr, &len ) )
			{
				if ( ( idx_in < inv_indexes.size() ) && ( inv_indexes[ idx_in ].size() > 0 ) )
				{
					std::string *str = reader->unescape_csv_field( &ptr, &len );
					if ( str )
					{
						outbuf->append( str );
						delete str;
					}
					else
						outbuf->append( ptr, len );
				}

				// cannot break: a later field may include a newline
				++idx_in;
			}
			if ( zero )
				outbuf->append( '\0' );
			else
				outbuf->append_nl();

		} while ( reader->fetch_line() );
	}


	// output a csv containing the columns from colspec of the input csv
	// return the expanded colspec used for the file (to reuse for next files)
	std::string select ( const std::string &colspec, const char *filename, bool show_headers )
	{
		if ( ! start_reader( colspec, filename ) )
			return out_colspec;

		if ( show_headers && headers )
		{
			for ( unsigned i = 0 ; i < indexes.size() ; ++i )
			{
				if ( i > 0 )
					outbuf->append( sep_out );

				int idx_in = indexes[ i ];

				if ( idx_in != -1 )
					outbuf->append( reader->escape_csv_field( (*headers)[ idx_in ] ) );
			}
			outbuf->append_nl();
		}

		if ( reader->eos() )
			return out_colspec;

		unsigned idx_len = indexes.size();
		unsigned *fld_off = new unsigned[ idx_len ];
		unsigned *fld_len = new unsigned[ idx_len ];

		do
		{
			for ( unsigned idx_out = 0 ; idx_out < idx_len ; ++idx_out )
				fld_off[ idx_out ] = (unsigned)-1;

			// parse input row
			char *line = NULL;
			unsigned f_off = 0, f_len = 0;
			unsigned idx_in = 0;
			while ( reader->read_csv_field( &line, &f_off, &f_len ) )
			{
				if ( idx_in < inv_indexes.size() && inv_indexes[ idx_in ].size() )
				{
					// current input field appears in output, save its off+len
					for ( unsigned i = 0 ; i < inv_indexes[ idx_in ].size() ; ++i )
					{
						unsigned idx_out = inv_indexes[ idx_in ][ i ];
						fld_off[ idx_out ] = f_off;
						fld_len[ idx_out ] = f_len;
					}
				}
				++idx_in;
			}

			// generate output row
			for ( unsigned idx_out = 0 ; idx_out < idx_len ; ++idx_out )
			{
				if ( idx_out > 0 )
					outbuf->append( sep_out );

				if ( fld_off[ idx_out ] != (unsigned)-1 )
					outbuf->append( line + fld_off[ idx_out ], fld_len[ idx_out ] );
			}
			outbuf->append_nl();

		} while ( reader->fetch_line() );


		delete[] fld_off;
		delete[] fld_len;

		return out_colspec;
	}


	// output a csv with some columns removed
	void deselect ( const std::string &colspec, const char *filename )
	{
		if ( ! start_reader( colspec, filename ) )
			return;

		if ( headers )
		{
			unsigned colnum_out = 0;

			for ( unsigned i = 0 ; i < headers->size() ; ++i )
			{
				if ( inv_indexes[ i ].size() )
					continue;

				if ( colnum_out++ > 0 )
					outbuf->append( sep_out );

				outbuf->append( reader->escape_csv_field( (*headers)[ i ] ) );

				++colnum_out;
			}
			outbuf->append_nl();
		}

		if ( reader->eos() )
			return;

		do
		{
			char *fld = NULL;
			unsigned fld_len = 0;
			unsigned colnum = 0;
			unsigned colnum_out = 0;

			while ( reader->read_csv_field( &fld, &fld_len ) )
			{
				if ( inv_indexes[ colnum++ ].size() )
					continue;

				if ( colnum_out++ > 0 )
					outbuf->append( sep_out );

				outbuf->append( fld, fld_len );
			}

			outbuf->append_nl();

		} while ( reader->fetch_line() );
	}


	// list columns of the file (indexes if -H)
	void listcol ( const char *filename )
	{
		if ( ! start_reader( "", filename ) )
			return;

		if ( headers )
		{
			for ( unsigned i = 0 ; i < headers->size() ; ++i )
			{
				outbuf->append( (*headers)[ i ] );
				outbuf->append_nl();
			}
		}
		else
		{
			for ( unsigned i = 0 ; i < max_index ; ++i )
			{
				outbuf->append( ull_str( i ) );
				outbuf->append_nl();
			}
		}
	}


	// prepend fields to every row (ignore added colnames if -H)
	void addcol ( const std::string &colval, const char *filename )
	{
		std::vector<std::string> cols;
		std::vector<std::string> vals;
		if ( ! split_colvalspec( colval, &cols, &vals ) )
			return;

		if ( ! start_reader( "", filename ) )
			return;

		if ( headers )
		{
			for ( unsigned i = 0 ; i < cols.size() ; ++i )
			{
				outbuf->append( reader->escape_csv_field( cols[i] ) );
				outbuf->append( sep_out );
			}

			for ( unsigned i = 0 ; i < headers->size() ; ++i )
			{
				outbuf->append( reader->escape_csv_field( (*headers)[i] ) );

				if ( i + 1 < headers->size() )
					outbuf->append( sep_out );
			}

			outbuf->append_nl();
		}

		if ( reader->eos() )
			return;

		do
		{
			for ( unsigned i = 0 ; i < vals.size() ; ++i )
			{
				if ( i > 0 )
					outbuf->append( sep_out );
				outbuf->append( vals[ i ] );
			}

			char *fld = NULL;
			unsigned fld_len = 0;

			while ( reader->read_csv_field( &fld, &fld_len ) )
			{
				outbuf->append( sep_out );
				outbuf->append( fld, fld_len );
			}

			outbuf->append_nl();

		} while ( reader->fetch_line() );
	}


	// filter csv, display only lines whose field value match a regexp
	void grepcol ( const std::string &colval, const char *filename )
	{
		std::vector<std::string> cols;
		std::vector<std::string> vals;
		if ( ! split_colvalspec( colval, &cols, &vals ) )
			return;

		// merge cols in a colspec
		std::string colspec;
		for ( unsigned i = 0 ; i < cols.size() ; ++i )
		{
			if ( i > 0 )
				colspec.push_back( ',' );

			colspec.append( cols[ i ] );
		}

		regex_t *vals_re = new regex_t[ vals.size() ];

		int flags = REG_NOSUB | REG_EXTENDED;
		if ( HAS_FLAG( RE_NOCASE ) )
			flags |= REG_ICASE;

		for ( unsigned i = 0 ; i < vals.size() ; ++i )
		{

			int err = regcomp( &vals_re[ i ], vals[ i ].c_str(), flags );
			if ( err )
			{
				char errbuf[1024];
				regerror( err, &vals_re[ i ], errbuf, sizeof(errbuf) );
				std::cerr << "Invalid regexp /" << vals[ i ] << "/ : " << errbuf << std::endl;

				for ( unsigned j = 0 ; j < i ; ++j )
					regfree( &vals_re[ j ] );
				delete[] vals_re;

				return;
			}
		}

		if ( ! start_reader( colspec, filename ) )
		{
			for ( unsigned i = 0 ; i < vals.size() ; ++i )
				regfree( &vals_re[ i ] );
			delete[] vals_re;

			return;
		}

		if ( headers )
		{
			for ( unsigned i = 0 ; i < headers->size() ; ++i )
			{
				if ( i > 0 )
					outbuf->append( sep_out );

				outbuf->append( reader->escape_csv_field( (*headers)[i] ) );
			}

			outbuf->append_nl();
		}

		if ( reader->eos() )
		{
			for ( unsigned i = 0 ; i < vals.size() ; ++i )
				regfree( &vals_re[ i ] );
			delete[] vals_re;

			return;
		}

		const unsigned stats_batch_size = 16*1024;
		unsigned stats_seen = 0;
		unsigned stats_match = (headers ? 1 : 0);
		bool invert = HAS_FLAG( RE_INVERT );
		do
		{
			char *line = NULL;
			unsigned f_off = 0, f_len = 0;
			unsigned idx_in = 0;
			bool show = false;

			while ( reader->read_csv_field( &line, &f_off, &f_len ) )
			{
				if ( ( idx_in < inv_indexes.size() ) && ( inv_indexes[ idx_in ].size() ) )
				{
					std::string str;
					char *ptr = line + f_off;
					unsigned len = f_len;
					reader->unescape_csv_field( &ptr, &len, &str );

					for ( unsigned i = 0 ; i < inv_indexes[ idx_in ].size() ; ++i )
					{
						unsigned idx_g = inv_indexes[ idx_in ][ i ];
						if ( idx_g < vals.size() && regexec( &vals_re[ idx_g ], str.c_str(), 0, NULL, 0 ) != REG_NOMATCH )
							show = true;
					}
				}
				++idx_in;
			}

			if ( show ^ invert )
			{
				outbuf->append( line, f_off + f_len );
				outbuf->append_nl();
				++stats_match;
			}
			++stats_seen;

			// manually flush if output is sparse
			if ( stats_seen > stats_batch_size )
			{
				if ( ( stats_match > 0 ) && ( stats_match < stats_batch_size / 8 ) )
					outbuf->flush();
				stats_seen = 0;
				// ensure we'll flush next time if we didnt now and next is empty
				stats_match = ( ( stats_match >= stats_batch_size / 8 ) ? 1 : 0 );
			}

		} while ( reader->fetch_line() );

		for ( unsigned i = 0 ; i < vals.size() ; ++i )
			regfree( &vals_re[ i ] );
		delete[] vals_re;
	}


	// filter csv, display only lines whose field appears on a file line
	void fgrepcol ( const std::string &colval, const char *filename )
	{
		std::vector<std::string> cols;
		std::vector<std::string> vals;
		if ( ! split_colvalspec( colval, &cols, &vals ) )
			return;

		// merge cols in a colspec
		std::string colspec;
		for ( unsigned i = 0 ; i < cols.size() ; ++i )
		{
			if ( i > 0 )
				colspec.push_back( ',' );

			colspec.append( cols[ i ] );
		}

		std::tr1::unordered_set<std::string> *vals_set = new std::tr1::unordered_set<std::string>[ vals.size() ];

		bool nocase = HAS_FLAG( RE_NOCASE );

		for ( unsigned i = 0 ; i < vals.size() ; ++i )
		{
			std::string line;
			std::ifstream in(vals[ i ].c_str());

			if ( ! in )
			{
				std::cerr << "Cannot open " << vals[ i ] << ": " << strerror( errno ) << std::endl;
				delete[] vals_set;

				return;
			}

			while ( std::getline( in, line ) )
			{
				if ( line.size() > 0 && line[ line.size() - 1 ] == '\n' )
					line.erase( line.size() - 1 );
				if ( line.size() > 0 && line[ line.size() - 1 ] == '\r' )
					line.erase( line.size() - 1 );

				if ( nocase )
					vals_set[ i ].insert( str_downcase( line ) );
				else
					vals_set[ i ].insert( line );
			}
		}

		if ( ! start_reader( colspec, filename ) )
		{
			delete[] vals_set;

			return;
		}

		if ( headers )
		{
			for ( unsigned i = 0 ; i < headers->size() ; ++i )
			{
				if ( i > 0 )
					outbuf->append( sep_out );

				outbuf->append( reader->escape_csv_field( (*headers)[i] ) );
			}

			outbuf->append_nl();
		}

		if ( reader->eos() )
		{
			delete[] vals_set;

			return;
		}

		const unsigned stats_batch_size = 16*1024;
		unsigned stats_seen = 0;
		unsigned stats_match = (headers ? 1 : 0);
		bool invert = HAS_FLAG( RE_INVERT );
		do
		{
			char *line = NULL;
			unsigned f_off = 0, f_len = 0;
			unsigned idx_in = 0;
			bool show = false;

			while ( reader->read_csv_field( &line, &f_off, &f_len ) )
			{
				if ( ( idx_in < inv_indexes.size() ) && inv_indexes[ idx_in ].size() )
				{
					std::string str;
					char *ptr = line + f_off;
					unsigned len = f_len;
					reader->unescape_csv_field( &ptr, &len, &str );

					for ( unsigned i = 0 ; i < inv_indexes[ idx_in ].size() ; ++i )
					{
						unsigned idx_g = inv_indexes[ idx_in ][ i ];
						if ( idx_g >= vals.size() )
							continue;

						if ( nocase )
						{
							if ( vals_set[ idx_g ].count( str_downcase( str ) ) > 0 )
								show = true;
						}
						else
						{
							if ( vals_set[ idx_g ].count( str ) > 0 )
								show = true;
						}
					}
				}
				++idx_in;
			}

			if ( show ^ invert )
			{
				outbuf->append( line, f_off + f_len );
				outbuf->append_nl();
				++stats_match;
			}
			++stats_seen;

			// manually flush if output is sparse
			if ( stats_seen > stats_batch_size )
			{
				if ( ( stats_match > 0 ) && ( stats_match < stats_batch_size / 8 ) )
					outbuf->flush();
				stats_seen = 0;
				// ensure we'll flush next time if we didnt now and next is empty
				stats_match = ( ( stats_match >= stats_batch_size / 8 ) ? 1 : 0 );
			}

		} while ( reader->fetch_line() );

		delete[] vals_set;
	}


	// dump csv rows, prefix each field with its colname
	void inspect ( const char *filename )
	{
		if ( ! start_reader( "", filename ) )
			return;

		if ( reader->eos() )
			return;

		unsigned long lineno = 0;
		do
		{
			outbuf->append( ull_str( lineno++, "%03lu:" ) );

			// parse input row
			char *fld = NULL;
			unsigned f_len = 0;
			unsigned colnum = 0;
			while ( reader->read_csv_field( &fld, &f_len ) )
			{
				// create & populate headers ondemand
				if ( ! headers )
					headers = new std::vector<std::string>;

				if ( colnum >= headers->size() )
					headers->push_back( ull_str( colnum ) );

				if ( colnum > 0 )
					outbuf->append( sep_out );

				outbuf->append( headers->at( colnum ) );
				outbuf->append( '=' );
				outbuf->append( fld, f_len );
				++colnum;
			}
			outbuf->append_nl();

		} while ( reader->fetch_line() );
	}


	// dump a range of csv rows
	// range is [row_start]-[row_end] (included, first row = 0)
	void rows ( const std::string &rowspec, const char *filename )
	{
		if ( ! start_reader( "", filename ) )
			return;

		if ( reader->eos() )
			return;

		if ( headers )
		{
			for ( unsigned i = 0 ; i < headers->size() ; ++i )
			{
				outbuf->append( reader->escape_csv_field( (*headers)[i] ) );

				if ( i + 1 < headers->size() )
					outbuf->append( sep_out );
			}

			outbuf->append_nl();
		}

		// parse rowspec
		unsigned long lineno_min = 1;	// included
		unsigned long lineno_max = 0;	// included
		size_t idx;
		if ( (idx = rowspec.find( '-' )) != std::string::npos )
		{
			// idx = 0 works too
			if ( ! str_ul( rowspec.substr( 0, idx ), &lineno_min ) )
				return;
			if ( idx == rowspec.size() - 1 )
				lineno_max = -1;
			else if ( ! str_ul( rowspec.substr( idx+1 ), &lineno_max ) )
				return;
		}
		else
		{
			if ( ! str_ul( rowspec, &lineno_min ) )
				return;
			lineno_max = lineno_min;
		}

		if ( lineno_min > lineno_max )
			return;

		if ( reader->eos() )
			return;

		unsigned long lineno = 0;

		do
		{
			char *fld = NULL;
			unsigned fld_len = 0;
			unsigned colnum = 0;

			while ( reader->read_csv_field( &fld, &fld_len ) )
			{
				if ( lineno >= lineno_min )
				{
					if ( colnum++ > 0 )
						outbuf->append( sep_out );
					outbuf->append( fld, fld_len );
				}
			}

			if ( lineno >= lineno_min )
				outbuf->append_nl();

			++lineno;

			if ( lineno > lineno_max )
				break;

		} while ( reader->fetch_line() );
	}


	// rename columns, always output a headerline, even with -H
	void rename ( const std::string &colval, const char *filename )
	{
		std::vector<std::string> cols;
		std::vector<std::string> vals;
		if ( ! split_colvalspec( colval, &cols, &vals ) )
			return;

		std::string colspec;
		for ( unsigned i = 0 ; i < cols.size() ; ++i )
		{
			if ( i > 0 )
				colspec.push_back( ',' );

			colspec.append( cols[ i ] );
		}

		if ( ! start_reader( colspec, filename ) )
			return;

		// generate new headerline
		for ( unsigned i = 0 ; i < max_index ; ++i )
		{
			if ( i > 0 )
				outbuf->append( sep );

			if ( i < inv_indexes.size() && inv_indexes[ i ].size() )
			{
				// new colname in colvals
				unsigned j = inv_indexes[ i ][ inv_indexes[ i ].size() - 1 ];
				if ( j < vals.size() )
					outbuf->append( vals[ j ] );
				else
					outbuf->append( ull_str( j ) );
			}
			else if ( headers && (unsigned)i < headers->size() )
			{
				// reuse old header name
				outbuf->append( (*headers)[ i ] );
			}
			else
			{
				// create new header, use the column number
				outbuf->append( ull_str( i ) );
			}
		}
		outbuf->append_nl();

		// copy end of file unchanged
		while ( ! reader->eos() )
		{
			char *ptr = NULL;
			unsigned len = 64*1024;

			reader->read( &ptr, &len );
			outbuf->append( ptr, len );
		}
	}


	// output a csv with a column values converted from hex (0x42) to decimal (66)
	// 64-bit range
	void decimal ( const std::string &colspec, const char *filename )
	{
		if ( ! start_reader( colspec, filename ) )
			return;

		if ( headers )
		{
			for ( unsigned i = 0 ; i < headers->size() ; ++i )
			{
				if ( i > 0 )
					outbuf->append( sep_out );

				outbuf->append( reader->escape_csv_field( (*headers)[ i ] ) );
			}
			outbuf->append_nl();
		}

		if ( reader->eos() )
			return;

		do
		{
			char *fld = NULL;
			unsigned fld_len = 0;
			unsigned colnum = 0;

			while ( reader->read_csv_field( &fld, &fld_len ) )
			{
				if ( colnum > 0 )
					outbuf->append( sep_out );

				if ( inv_indexes[ colnum ].size() )
				{
					char *fld_dup = fld;
					unsigned fld_len_dup = fld_len;
					std::string hex;
					reader->unescape_csv_field( &fld_dup, &fld_len_dup, &hex );

					unsigned long long v;
					int minus = 0;

					if ( hex.size() > 0 && hex[0] == '-' )
					{
						minus = 1;
						hex = hex.substr( 1 );
					}

					if ( ! str_ull( hex, &v ) )
						outbuf->append( fld, fld_len );
					else
					{
						if ( minus )
							outbuf->append( '-' );
						outbuf->append( ull_str( v ) );
					}
				}
				else
					outbuf->append( fld, fld_len );

				++colnum;
			}

			outbuf->append_nl();

		} while ( reader->fetch_line() );
	}
};

static const char *usage =
"Usage: csv [options] <mode>\n"
" Options:\n"
"          -V                 display version information and exit\n"
"          -h                 display help (this text) and exit\n"
"          -o <outfile>       specify output file (default=stdout)\n"
"          -s <separator>     csv field separator (default=',')\n"
"          -S <separator>     output csv field separator (default=sep) - do not use -s after this option ; ignored in rename\n"
"          -q <quote>         csv quote character (default='\"')\n"
"          -L <max line len>  specify maximum line length allowed (default=64k)\n"
"          -H                 csv files have no header line\n"
"                             columns are specified as number (first col is 0)\n"
"          -i                 case insensitive regex (grep mode)\n"
"          -v                 invert regex: show non-matching lines (grep mode)\n"
"          -u                 unique columns: do not include cols specified in colspec when expanding ranges\n"
"                             useful to move cols, eg select -u col3,-,col1\n"
"          -0                 in extract mode, end records with a nul byte\n"
"\n"
"csv addcol <col1>=<val1>,..  prepend a column to the csv with fixed value\n"
"csv extract <column>         extract one column data\n"
"csv grepcol <col1>=<val1>,.. create a csv with only the lines where colX has value X (regexp)\n"
"                             with multiple colval, show line if any one match (c1=~v1 OR c2=~v2)\n"
"csv fgrepcol <col1>=<f1>,..  create a csv with only the lines where colX has a value appearing exactly as a line of file fX\n"
"                             similar to grep -f -F ; options -v and -i work\n"
"csv rename <col1>=<name>,..  rename columns\n"
"csv select <col1>,<col2>,..  create a new csv with a subset/reordered columns\n"
"csv deselect <cols>          create a new csv with the specified columns removed\n"
"csv listcol                  list csv column names, one per line\n"
"csv inspect                  dump csv file, prefix each field with its column name\n"
"csv rows <min>-<max>         dump selected row range from file\n"
"csv stripheader              dump the csv files omitting the header line\n"
"csv decimal <cols>           convert selected columns to decimal int64 representation\n"
;

static const char *version_info =
"CSV tool version " CSV_TOOL_VERSION "\n"
"Copyright (c) 2013 Yoann Guillot\n"
"Licensed under the WtfPLv2, see http://www.wtfpl.net/\n"
;

static char escape_char( const char c )
{
	switch ( c )
	{
	case '0':
		return '\0';

	case 't':
		return '\t';

	case 'f':
		return '\f';

	case '\\':
		return '\\';

	default:
		std::cerr << "Unhandled separator, using coma" << std::endl;
	}

	return ',';
}

int main ( int argc, char * argv[] )
{
	int opt;
	char *outfile = NULL;
	char sep = ',';
	char sep_out = ',';
	char quot = '"';
	unsigned line_max = 64*1024;
	unsigned csv_flags = 0;

	while ( (opt = getopt(argc, argv, "hVo:s:S:q:L:Hivu0")) != -1 )
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

		case 's':
			sep = *optarg;
			if ( sep == '\\' )
				sep = escape_char( optarg[ 1 ] );
			sep_out = sep;
			break;

		case 'S':
			sep_out = *optarg;
			if ( sep_out == '\\' )
				sep_out = escape_char( optarg[ 1 ] );
			break;

		case 'q':
			quot = *optarg;
			if ( quot == '\\' )
				quot = escape_char( optarg[ 1 ] );
			break;

		case 'L':
			line_max = strtoul( optarg, NULL, 0 );
			break;

		case 'H':
			csv_flags |= 1 << NO_HEADERLINE;
			break;

		case 'i':
			csv_flags |= 1 << RE_NOCASE;
			break;

		case 'v':
			csv_flags |= 1 << RE_INVERT;
			break;

		case 'u':
			csv_flags |= 1 << UNIQ_COLS;
			break;

		case '0':
			csv_flags |= 1 << EXTRACT_ZERO;
			break;

		default:
			std::cerr << "Unknwon option: " << opt << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
	}

	if ( optind >= argc )
	{
		std::cerr << "No mode specified" << std::endl << usage << std::endl;
		return EXIT_FAILURE;
	}

	output_buffer outbuf( outfile );
	if ( outbuf.failed_to_open() )
		return EXIT_FAILURE;

	csv_tool csv( &outbuf, sep, sep_out, quot, line_max, csv_flags );

	std::string mode = argv[optind++];

	if ( mode == "extract" || mode == "e" || mode == "x" )
	{
		if ( optind >= argc )
		{
			std::cerr << "No column specified" << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
		std::string colspec = argv[ optind++ ];

		if ( optind >= argc )
			csv.extract( colspec, NULL );
		else
			for ( int i = optind ; i < argc ; ++i )
				csv.extract( colspec, argv[ i ] );
	}
	else if ( mode == "select" || mode == "map" || mode == "s" || mode == "m" )
	{
		if ( optind >= argc )
		{
			std::cerr << "No columns specified" << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
		std::string colspec = argv[ optind++ ];

		if ( optind >= argc )
			csv.select( colspec, NULL, true );
		else
		{
			for ( int i = optind ; i < argc ; ++i )
				colspec = csv.select( colspec, argv[ i ], (i == optind) );
		}
	}
	else if ( mode == "deselect" || mode == "d" )
	{
		if ( optind >= argc )
		{
			std::cerr << "No columns specified" << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
		std::string colspec = argv[ optind++ ];

		if ( optind >= argc )
			csv.deselect( colspec, NULL );
		else
		{
			for ( int i = optind ; i < argc ; ++i )
				csv.deselect( colspec, argv[ i ] );
		}
	}
	else if ( mode == "rename" )
	{
		if ( optind >= argc )
		{
			std::cerr << "No columns specified" << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
		std::string colval = argv[ optind++ ];

		if ( optind >= argc )
			csv.rename( colval, NULL );
		else
		{
			// XXX output headers for every file
			for ( int i = optind ; i < argc ; ++i )
				csv.rename( colval, argv[ i ] );
		}
	}
	else if ( mode == "listcol" || mode == "l" )
	{
		if ( optind >= argc )
			csv.listcol( NULL );
		else
		{
			for ( int i = optind ; i < argc ; ++i )
				csv.listcol( argv[ i ] );
		}
	}
	else if ( mode == "addcol" || mode == "a" )
	{
		if ( optind >= argc )
		{
			std::cerr << "No colval specified" << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
		std::string colval = argv[ optind++ ];

		if ( optind >= argc )
			csv.addcol( colval, NULL );
		else
		{
			for ( int i = optind ; i < argc ; ++i )
				csv.addcol( colval, argv[ i ] );
		}
	}
	else if ( mode == "grepcol" || mode == "grep" || mode == "g" )
	{
		if ( optind >= argc )
		{
			std::cerr << "No colval specified" << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
		std::string colval = argv[ optind++ ];

		if ( optind >= argc )
			csv.grepcol( colval, NULL );
		else
		{
			for ( int i = optind ; i < argc ; ++i )
				csv.grepcol( colval, argv[ i ] );
		}
	}
	else if ( mode == "fgrepcol" || mode == "fgrep" || mode == "f" )
	{
		if ( optind >= argc )
		{
			std::cerr << "No colval specified" << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
		std::string colval = argv[ optind++ ];

		if ( optind >= argc )
			csv.fgrepcol( colval, NULL );
		else
		{
			for ( int i = optind ; i < argc ; ++i )
				csv.fgrepcol( colval, argv[ i ] );
		}
	}
	else if ( mode == "inspect" || mode == "i" )
	{
		if ( optind >= argc )
			csv.inspect( NULL );
		else
		{
			for ( int i = optind ; i < argc ; ++i )
				csv.inspect( argv[ i ] );
		}
	}
	else if ( mode == "rows" || mode == "row" || mode == "r" )
	{
		if ( optind >= argc )
		{
			std::cerr << "No rowspec specified" << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
		std::string rowspec = argv[ optind++ ];

		if ( optind >= argc )
			csv.rows( rowspec, NULL );
		else
		{
			for ( int i = optind ; i < argc ; ++i )
				csv.rows( rowspec, argv[ i ] );
		}
	}
	else if ( mode == "stripheader" || mode == "stripheaders" )
	{
		csv.csv_flags |= 1 << NO_HEADERLINE;

		if ( optind >= argc )
			csv.rows( "1-", NULL );
		else
		{
			for ( int i = optind ; i < argc ; ++i )
				csv.rows( "1-", argv[ i ] );
		}
	}
	else if ( mode == "decimal" || mode == "dec" )
	{
		if ( optind >= argc )
		{
			std::cerr << "No columns specified" << std::endl << usage << std::endl;
			return EXIT_FAILURE;
		}
		std::string colspec = argv[ optind++ ];

		if ( optind >= argc )
			csv.decimal( colspec, NULL );
		else
		{
			for ( int i = optind ; i < argc ; ++i )
				csv.decimal( colspec, argv[ i ] );
		}
	}
	else
	{
		std::cerr << "Unsupported mode " << mode << std::endl << usage << std::endl;
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}
