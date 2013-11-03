#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <iostream>
#include <fstream>
#include <tr1/unordered_map>

using namespace std;
using namespace std::tr1;

class line_reader
{
private:
	istream &input;

	// maximum line length
	unsigned buf_cur;
	unsigned buf_end;
	unsigned buf_size;
	char *buf;

	// move buf_cur to buf_start, fill buf_end..buf_size with freshly read data
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
				buf_end = buf_cur;
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

	explicit line_reader ( istream &is, unsigned line_max=1<<20 ) :
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
	// returns line_start = NULL if no newline is found
	// eof is considered as a newline
	// the returned pointers are only valid until the next call to read_line
	void read_line ( char* &line_start, unsigned &line_length )
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

			return;
		}

		// end of file ?
		if ( !input.good() )
		{
			if ( buf_cur < buf_end )
			{
				line_start = buf + buf_cur;
				line_length = buf_end - buf_cur;
			}
			else
			{
				line_start = NULL;
				line_length = 0;
			}

			buf_cur = buf_end;

			return;
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

		return;
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
	explicit csv_reader ( istream &is, char sep=',', char quot='"', unsigned line_max=1<<20 ) :
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
	bool read_line ( )
	{
		cur_field_offset = 0;

		input_lines->read_line( cur_line, cur_line_length_nl );

		if ( cur_line )
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
			char *esep = (char*)memchr( (void*)(cur_line + cur_field_offset), sep, cur_line_length - cur_field_offset );

			if ( esep )
			{
				field_length = esep - (cur_line + cur_field_offset);
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
			char *equot = NULL;

			if ( cur_field_offset + field_length < cur_line_length )
				equot = (char*)memchr( (void*)(cur_line + cur_field_offset + field_length), quot,
						cur_line_length - (cur_field_offset + field_length) );

			if ( equot )
			{
				// found end quote
				field_length = equot - (cur_line + cur_field_offset) + 1;

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
};

static char *outfile = NULL;
static char sep = ',';
static char quot = '"';
static bool has_headerline = true;

static void csv_extract( string &colname, istream &input )
{
	csv_reader reader( input, sep, quot );

	while ( reader.read_line() )
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
			cout << usage;
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
			cerr << "Unknwon option: " << opt << endl << usage << endl;
			exit(EXIT_FAILURE);
		}
	}

	if ( optind >= argc )
	{
		cerr << "No mode specified" << endl << usage << endl;
		exit(EXIT_FAILURE);
	}

	string mode = argv[optind++];

	if ( mode == "extract" )
	{
		if ( optind >= argc )
		{
			cerr << "No column specified" << endl << usage << endl;
			exit(EXIT_FAILURE);
		}
		string colname = argv[optind++];

		if ( optind >= argc )
		{
			csv_extract( colname, cin );
		}
		else
		{
			for ( int i = optind ; i<argc ; i++ )
			{
				ifstream in(argv[i]);
				csv_extract( colname, in );
			}
		}
	}
	else
	{
		cerr << "Unsupported mode " << mode << endl <<  usage << endl;
		exit(EXIT_FAILURE);
	}

	exit(EXIT_SUCCESS);
}
