#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <errno.h>
#ifndef NO_ZLIB
#include <zlib.h>
#endif

#include "csv_reader.h"

// wraps an istream, provide an efficient interface to read lines
// skips UTF-8 BOM
// interprets UTF-16 BOMs, return iso codepoints - out of range characters are converted to '?'
// handles gzip compressed inputs
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

#ifndef NO_ZLIB
	unsigned zbuf_cur;
	unsigned zbuf_end;
	unsigned zbuf_size;
	char *zbuf;
	z_stream zstream;
#endif

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

#ifndef NO_ZLIB
			if (zbuf) {
				// shift & refill compressed data if available
				if ( zbuf_cur > zbuf_size / 2 )
				{
					if ( zbuf_end < zbuf_cur )
						zbuf_end = zbuf_cur;
					memmove( zbuf, zbuf + zbuf_cur, zbuf_end - zbuf_cur );
					zbuf_end -= zbuf_cur;
					zbuf_cur = 0;
				}

				if ( zbuf_size > zbuf_end )
				{
					input->read( zbuf + zbuf_end, zbuf_size - zbuf_end);
					zbuf_end += input->gcount();
				}

				// decompress into buf
				int ret;
				zstream.next_out = (Bytef *)buf + buf_end;
				zstream.avail_out = toread;
				zstream.next_in = (Bytef *)zbuf + zbuf_cur;
				zstream.avail_in = zbuf_end - zbuf_cur;
				ret = inflate( &zstream, Z_NO_FLUSH );
				if ( ret == Z_OK || ret == Z_STREAM_END )
				{
					zbuf_cur = (char *)zstream.next_in - zbuf;
					buf_end = (char *)zstream.next_out - buf;
				}

				if ( ret != Z_OK )
				{
					if ( ret == Z_STREAM_END )
					{
						if ( zbuf_cur != zbuf_end )
							std::cerr << "inflate: trailing data (" << (zbuf_end - zbuf_cur) << " bytes)" << std::endl;
					} else
						std::cerr << "inflate: " << zstream.msg << std::endl;

					inflateEnd( &zstream );
					delete[] zbuf;
					zbuf = NULL;
				}

				if ( input_filter )
					buf_end = filter_input( old_end, buf_end );

				return;
			}
#endif

			input->read( buf + buf_end, toread );
			buf_end += input->gcount();
			if (buf_end > buf_size)
				buf_end = buf_cur;	// just in case

			if ( input_filter )
				buf_end = filter_input( old_end, buf_end );
		}
	}

#ifndef NO_ZLIB
	void init_zstream(void)
	{
		zbuf = buf;
		zbuf_cur = buf_cur;
		zbuf_end = buf_end;
		zbuf_size = buf_size;

		buf = new char[buf_size];
		buf_cur = buf_end = 0;

		int ret;
		zstream.zalloc = Z_NULL;
		zstream.zfree = Z_NULL;
		zstream.opaque = Z_NULL;
		zstream.avail_in = zbuf_end - zbuf_cur;
		zstream.next_in = (Bytef *)zbuf + zbuf_cur;

		ret = inflateInit2( &zstream, 32|15 );

		if (ret != Z_OK)
		{
			std::cerr << "inflateInit: " << zstream.msg << std::endl;
			badfile = true;
			return;
		}

		zbuf_cur = (char *)zstream.next_in - zbuf;

		refill_buffer();
	}
#endif

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

#ifndef NO_ZLIB
		zbuf = NULL;

		if ( buf_end >= 2 && buf[0] == 0x1f && buf[1] == (char)0x8b )
			// zlib header
			init_zstream();
#endif

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
#ifndef NO_ZLIB
		if ( zbuf )
			delete[] zbuf;
#endif
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

		// slide existing buffer, read more from input, and retry
		if ( buf_cur > 0 )
		{
			refill_buffer();

			return read_line( line_start, line_length );
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

// set cur_line_length from cur_line_length_nl, trim \r\n
void csv_reader::trim_newlines ( )
{
	cur_line_length = cur_line_length_nl;

	if ( cur_line_length > 0 && cur_line[ cur_line_length - 1 ] == '\n' )
		cur_line_length--;

	if ( cur_line_length > 0 && cur_line[ cur_line_length - 1 ] == '\r' )
		cur_line_length--;
}

bool csv_reader::failed_to_open ( ) const
{
	return input_lines->failed_to_open();
}

// return true if no more data is available from input_lines
bool csv_reader::eos ( ) const
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
void csv_reader::reset_cur_field_offset ( )
{
	cur_field_offset = 0;
}

// line_max is passed to the line_reader, it is also the limit for a full csv row (that may span many lines)
csv_reader::csv_reader ( const char *filename, const char sep, const char quot, const unsigned line_max ) :
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

csv_reader::~csv_reader ( )
{
	if ( line_copy )
		delete[] line_copy;

	delete input_lines;
}

// read one line from input_lines
// invalidates previous read_csv_field pointers
// return false after EOF
bool csv_reader::fetch_line ( )
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
bool csv_reader::read_csv_field ( char* *line_start, unsigned *field_offset, unsigned *field_length )
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
bool csv_reader::read_csv_field ( char* *field_start, unsigned *field_length )
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
std::string* csv_reader::unescape_csv_field ( char* *field_start, unsigned *field_length, std::string* unescaped ) const
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
std::string csv_reader::escape_csv_string ( const std::string &str, const char quot )
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

std::string csv_reader::escape_csv_field ( const std::string &str ) const
{
	return csv_reader::escape_csv_string( str, quot );
}

// parse the current csv line (after a call to fetch_line())
// return a pointer to a vector of unescaped strings, should be delete by caller
std::vector<std::string>* csv_reader::parse_line ( )
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
void csv_reader::read ( char* *ptr, unsigned *len )
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
