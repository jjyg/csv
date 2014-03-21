#include <string.h>
#include <fstream>
#include <errno.h>

#include "output_buffer.h"

bool output_buffer::failed_to_open ( ) const
{
	return badfile;
}

void output_buffer::flush ( )
{
	if ( buf_end > 0 )
	{
		output->write( buf, buf_end );
		buf_end = 0;
	}

	output->flush();
}

void output_buffer::append ( const char *s, const unsigned len )
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

void output_buffer::append ( const std::string *str )
{
	append( str->data(), str->size() );
}

void output_buffer::append ( const std::string &str )
{
	append( &str );
}

void output_buffer::append ( const char c )
{
	if ( buf_end < buf_size - 1 )
		buf[ buf_end++ ] = c;
	else
		append( &c, 1 );
}

void output_buffer::append_nl ( )
{
	append( '\r' );
	append( '\n' );
}

output_buffer::output_buffer ( const char *filename, const unsigned buf_size ) :
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

output_buffer::~output_buffer ( )
{
	flush();

	if ( should_delete_output )
		delete output;

	delete[] buf;
}
