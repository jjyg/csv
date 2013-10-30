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
	unsigned char *buf;

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
			input.read( (char*)(buf + buf_end), buf_size - buf_end );
			buf_end += input.gcount();
			if (buf_end > buf_size)
				buf_end = buf_cur;
		}
	}

public:
	bool eos ( )
	{
		if ( input.good() )
			return false;

		if ( buf_cur < buf_end )
			return false;

		return true;
	}

	explicit line_reader ( istream &is, int line_max=1<<24 ) :
		input(is),
		buf_cur(0),
		buf_end(0),
		buf_size(line_max)
	{
		buf = new unsigned char[buf_size];

		input.read( (char*)buf, buf_size );
		buf_end = input.gcount();

		if (buf_end > buf_size)
			buf_end = buf_cur;	// just in case

		// discard utf-8 BOM
		if ( (buf_end >= 3) && buf[0] == 0xef && buf[1] == 0xbb && buf[2] == 0xbf )
			buf_cur += 3;
	}

	~line_reader ( )
	{
		delete[] buf;
	}

	// read one line from input, starting at buf_cur
	// returns the line start in line_start and the line length in line_length
	// line_length does not include the newline character(s)
	// the line is not null-terminated
	// internally, advances buf_cur to the beginning of next line
	// returns line_start = NULL if no newline is found
	// eof is considered as a newline
	void read_line ( unsigned char* &line_start, unsigned &line_length )
	{
		unsigned char *nl = (unsigned char*)memchr( (void*)(buf + buf_cur), '\n', buf_end - buf_cur );

		// newline found ?
		if ( nl )
		{
			line_start = buf + buf_cur;
			line_length = nl - line_start;

			buf_cur += line_length + 1;
			if (buf_cur > buf_end)
				buf_cur = buf_end;	// just in case

			if ( line_length >= 1 && line_start[ line_length - 1 ] == '\r' )
				--line_length;

			return;
		}

		// end of file ?
		if ( !input.good() )
		{
			line_start = buf + buf_cur;
			line_length = buf_end - buf_cur;

			buf_cur = buf_end;

			if ( line_length >= 1 && line_start[ line_length - 1 ] == '\r' )
				--line_length;

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


static char *outfile = NULL;
static char sep = ',';
static char quot = '"';
static bool has_headerline = true;

static void csv_extract( string &colname, istream &input )
{
	line_reader lines(input);

	while ( ! lines.eos() )
	{
		unsigned char *ptr = NULL;
		unsigned len = 0;
		lines.read_line( ptr, len );
		if ( ptr )
			fprintf(stdout, "%3.3s\n", ptr);
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
