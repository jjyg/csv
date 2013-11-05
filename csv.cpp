#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <iostream>
#include <fstream>
#include <vector>

class line_reader
{
private:
	std::istream &input;

	// maximum line length
	unsigned buf_cur;
	unsigned buf_end;
	unsigned buf_size;
	char *buf;

	// memmove buf_cur to buf_start, fill buf_end..buf_size with freshly read data
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
			input.read( buf + buf_end, buf_size - buf_end );
			buf_end += input.gcount();
			if (buf_end > buf_size)
				buf_end = buf_cur;	// just in case
		}
	}

public:
	// return true if no more data is available from input
	bool eos ( )
	{
		if ( input.good() )
			return false;

		if ( buf_cur < buf_end )
			return false;

		return true;
	}

	explicit line_reader ( std::istream &is, const unsigned line_max=1<<20 ) :
		input(is),
		buf_cur(0),
		buf_end(0),
		buf_size(line_max)
	{
		buf = new char[buf_size];

		input.read( buf, (buf_size > 4096 ? buf_size/16 : buf_size) );
		buf_end = input.gcount();

		// discard utf-8 BOM
		if ( (buf_end >= 3) && buf[0] == '\xef' && buf[1] == '\xbb' && buf[2] == '\xbf' )
			buf_cur += 3;
	}

	~line_reader ( )
	{
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
	bool read_line ( char* &line_start, unsigned &line_length )
	{
		char *nl = NULL;

		if ( buf_cur < buf_end )
			nl = (char*)memchr( (void*)(buf + buf_cur), '\n', buf_end - buf_cur );

		// newline found ?
		if ( nl )
		{
			line_start = buf + buf_cur;
			line_length = nl + 1 - line_start;

			buf_cur += line_length;
			if (buf_cur > buf_end)
				buf_cur = buf_end;	// just in case

			return true;
		}

		// end of file ?
		if ( !input.good() )
		{
			if ( buf_cur < buf_end )
			{
				line_start = buf + buf_cur;
				line_length = buf_end - buf_cur;
				buf_cur = buf_end;

				return true;
			}
			else
			{
				line_start = NULL;
				line_length = 0;

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
		line_start = NULL;
		line_length = 0;

		// slide buffer anyway, to avoid infinite loop in badly written clients
		buf_cur = 0;
		buf_end = 0;
		refill_buffer();

		return false;
	}
};

class csv_reader
{
private:
	line_reader *input_lines;
	unsigned line_max;
	char *line_copy;

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
	// return true if no more data is available from input_lines
	bool eos ( )
	{
		if ( cur_field_offset < cur_line_length )
			return false;

		if ( !input_lines->eos() )
			return false;

		return true;
	}

	// line_max is passed to the line_reader, it is also the limit for a full csv row (that may span many lines)
	explicit csv_reader ( std::istream &is, const char sep=',', const char quot='"', const unsigned line_max=1<<20 ) :
		line_max(line_max),
		line_copy(NULL),
		sep(sep),
		quot(quot),
		cur_line(NULL),
		cur_line_length(0),
		cur_line_length_nl(0),
		cur_field_offset(0)
	{
		input_lines = new line_reader(is, line_max);
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
	bool next_line ( )
	{
		cur_field_offset = 0;

		if ( input_lines->read_line( cur_line, cur_line_length_nl ) )
		{
			trim_newlines();
			
			return true;
		}

		cur_line_length = cur_line_length_nl = 0;

		return false;
	}

	// read one csv field from the current line
	// returns false if no more fields are available in the line, or if there is a syntax error (unterminated quote, end quote followed by neither a quote nor a separator)
	// returns a pointer to the line start, the offset of the current field, and its length
	// the 'field_offset' returned by previous calls for the same line is still valid relative to the new 'line_start' (which may change if one field crosses a line boundary, in that case the lines are copied into an internal buffer)
	bool read_csv_field ( char* &line_start, unsigned &field_offset, unsigned &field_length )
	{
		if ( cur_field_offset >= cur_line_length )
			return false;

		field_offset = cur_field_offset;
		line_start = cur_line;

		if ( cur_line[ cur_field_offset ] != quot )
		{
			// unquoted field
			char *psep = (char*)memchr( (void*)(cur_line + cur_field_offset), sep, cur_line_length - cur_field_offset );

			if ( psep )
			{
				field_length = psep - (cur_line + cur_field_offset);
				cur_field_offset += field_length + 1;
			}
			else
			{
				field_length = cur_line_length - cur_field_offset;
				cur_field_offset = cur_line_length;
			}

			return true;
		}

		// quoted field
		field_length = 1;	// field length until now, including opening quote
		while (1)
		{
			char *pquot = NULL;

			if ( cur_field_offset + field_length < cur_line_length )
				pquot = (char*)memchr( (void*)(cur_line + cur_field_offset + field_length), quot,
						cur_line_length - (cur_field_offset + field_length) );

			if ( pquot )
			{
				// found end quote
				field_length = pquot - (cur_line + cur_field_offset) + 1;

				if ( cur_field_offset + field_length < cur_line_length )
				{
					if ( cur_line[ cur_field_offset + field_length ] == sep )
					{
						// end of field
						cur_field_offset += field_length + 1;
						return true;
					}

					if ( cur_line[ cur_field_offset + field_length ] == quot )
					{
						// escaped quote
						field_length++;
						continue;
					}

					// syntax error
					cur_field_offset += field_length;
					return false;
				}

				// end quote was at end of line
				cur_field_offset = cur_line_length;
				return true;
			}

			// no closing quote on current input_lines line
			if ( cur_line != line_copy )
			{
				// copy current line to internal buffer
				if ( !line_copy )
					line_copy = new char[line_max];

				memcpy( line_copy, cur_line, cur_line_length_nl );

				line_start = cur_line = line_copy;
			}

			char *next_line = NULL;
			unsigned next_line_length_nl = 0;

			input_lines->read_line( next_line, next_line_length_nl );

			if ( next_line && cur_line_length_nl + next_line_length_nl <= line_max )
			{
				// next line fits in internal buffer: append
				memcpy( line_copy + cur_line_length_nl, next_line, next_line_length_nl );

				field_length = cur_line_length_nl - cur_field_offset;

				cur_line_length_nl += next_line_length_nl;

				trim_newlines();
			}
			else
			{
				// reached end of input_lines / line_max with no end quote: return syntax error
				cur_field_offset = cur_line_length;
				return false;
			}
		}
	}

	// same as read_csv_field ( line_start, field_offset, field_length ) with simpler args
	// returned values are only valid until the next call to this function (with the 3-args version, it is valid until next_line())
	bool read_csv_field ( char* &field_start, unsigned &field_length )
	{
		char *line_start = NULL;
		unsigned field_offset = 0;

		if ( !read_csv_field( line_start, field_offset, field_length ) )
			return false;

		field_start = line_start + field_offset;

		return true;
	}

	// return an unescaped csv field
	// if unescaped is not NULL, fill it with unescaped data (data appended, string should be empty on call)
	// if unescaped is NULL, and field has no escaped quote, only update field_start and field_length to reflect unescaped data (zero copy)
	// if unescaped is NULL, and field has escaped quotes, allocate a new string and fill it with unescaped data. Caller should free it.
	// returns a pointer to unescaped
	// on return, if unescaped is not NULL, field_start and field_length are undefined.
	std::string* unescape_csv_field ( char* &field_start, unsigned &field_length, std::string* unescaped )
	{
		if ( field_length <= 0 )
			return unescaped;

		if ( field_start[ 0 ] != quot )
		{
			if ( unescaped )
				unescaped->append( field_start, field_length );

			return unescaped;
		}

		field_start++;
		field_length--;
		field_length--;

		while (1)
		{
			char *pquot = NULL;

			if ( field_length > 0 )
				pquot = (char*)memchr( (void*)field_start, quot, field_length );

			if ( pquot )
			{
				// found quote: must be an escape
				if ( ! unescaped )
					unescaped = new std::string;

				// append string start, including 1 quot
				unescaped->append( field_start, pquot - field_start + 1 );
				field_length -= pquot - field_start + 2;
				field_start = pquot + 2;
			}
			else
			{
				if ( unescaped )
					unescaped->append( field_start, field_length );

				return unescaped;
			}
		}
	}

	// parse the current csv line (after a call to next_line())
	// return a vector of unescaped strings
	std::vector<std::string> *parse_line( )
	{
		std::vector<std::string> *vec = new std::vector<std::string>;
		char *field_start = NULL;
		unsigned field_length = 0;

		while ( read_csv_field( field_start, field_length ) )
		{
			std::string unescaped;
			unescape_csv_field( field_start, field_length, &unescaped );
			vec->push_back( unescaped );
		}

		return vec;
	}

	// parse a colspec string (coma-separated list of column names), return a vector of indexes for those columns
	// colspec is a coma-separated list of column names, or a coma-separated list of column indexes (numeric)
	// colspec may include a range of columns, begin-end
	// with named columns, 1st try is done with '-' as part of the column name, and then as a range separator
	//
	// if a column is not found, its index is set as -1. For ranges, assume a short range.
	// if the csv has a header row, should be called with headers = parse_line()
	// should be called on a new csv_parser, after the 1st call to next_line()
	std::vector<int> *parse_colspec( const std::string &colspec_str, const std::vector<std::string> *headers )
	{
		std::vector<int> *indexes = new std::vector<int>;
		int max_index = -1;

		if ( headers )
		{
			max_index = headers->size();
		}
		else
		{
			// count columns on the 1st row
			char *ptr = NULL;
			unsigned len = 0;
			while ( read_csv_field( ptr, len ) )
				max_index++;

			// reset internal ptr
			cur_field_offset = 0;
		}

		// parse colspec_str: split on comas
		std::vector<std::string> colspec_vec;
		{
			size_t off = 0, idx = -1;
			while ( (idx = colspec_str.find( ',', off )) != std::string::npos )
			{
				colspec_vec.push_back( colspec_str.substr( off, idx-off ) );
				off = idx + 1;
			}
			colspec_vec.push_back( colspec_str.substr( off ) );
		}

		for ( std::vector<std::string>::const_iterator spec_it = colspec_vec.begin() ; spec_it != colspec_vec.end() ; ++spec_it )
		{
			bool found = false;
			if ( headers )
			{
				for ( std::vector<std::string>::const_iterator head_it = headers->begin() ; head_it != headers->end() ; ++head_it )
				{
					if ( ! strcasecmp( spec_it->c_str(), head_it->c_str() ) )
					{
						indexes->push_back( head_it - headers->begin() );
						found = true;
						break;
					}
				}
				// TODO
			}
		}

		return indexes;
	}
};

static char *outfile = NULL;
static char sep = ',';
static char quot = '"';
static bool has_headerline = true;

static void csv_extract( std::string &colname, std::istream &input )
{
	csv_reader reader( input, sep, quot );

	while ( reader.next_line() )
	{
		char *ptr = NULL;
		unsigned off = 0;
		unsigned len = 0;
		int i = 0;

		while ( reader.read_csv_field( ptr, off, len ) )
		{
			fprintf(stdout, "f%d: %.*s\n", i++, len, ptr+off);
		}
		fprintf(stdout, "eol\n");
	}
}

static const char *usage =
"Usage: csv [options] <mode>\n"
" Options:\n"
"          -o <outfile>       default=stdout\n"
"          -s <separator>     default=','\n"
"          -q <quote>         default='\"'\n"
"          -H                 csv has no header line\n"
"                             columns are specified number (starts at 1)\n"
"\n"
"csv extract <column>         extract one column data\n"
"csv select <col1>,<col2>,..  create a new csv with columns reordered\n"
"csv addcol <col1>=<val1>     append an column to the csv with fixed value\n"
"csv grepcol <col1>=<val1>    create a csv with only the lines where colX has value X (regexp)\n"
;

int main ( int argc, char * argv[] )
{
	int opt;

	while ( (opt = getopt(argc, argv, "ho:s:q:H")) != -1 )
	{
		switch (opt)
		{
		case 'h':
			std::cerr << usage;
			exit(EXIT_SUCCESS);

		case 'o':
			outfile = optarg;
			break;

		case 's':
			sep = *optarg;
			break;

		case 'q':
			quot = *optarg;
			break;

		case 'H':
			has_headerline = false;
			break;

		default:
			std::cerr << "Unknwon option: " << opt << std::endl << usage << std::endl;
			exit(EXIT_FAILURE);
		}
	}

	if ( optind >= argc )
	{
		std::cerr << "No mode specified" << std::endl << usage << std::endl;
		exit(EXIT_FAILURE);
	}

	std::string mode = argv[optind++];

	if ( mode == "extract" )
	{
		if ( optind >= argc )
		{
			std::cerr << "No column specified" << std::endl << usage << std::endl;
			exit(EXIT_FAILURE);
		}
		std::string colname = argv[optind++];

		if ( optind >= argc )
		{
			csv_extract( colname, std::cin );
		}
		else
		{
			for ( int i = optind ; i<argc ; i++ )
			{
				std::ifstream in(argv[i]);
				csv_extract( colname, in );
			}
		}
	}
	else
	{
		std::cerr << "Unsupported mode " << mode << std::endl << usage << std::endl;
		exit(EXIT_FAILURE);
	}

	exit(EXIT_SUCCESS);
}
