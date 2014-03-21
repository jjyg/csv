#ifndef OUTPUT_BUFFER_H
#define OUTPUT_BUFFER_H

#include <iostream>

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
	bool failed_to_open ( ) const;
	void flush ( );
	void append ( const char *s, const unsigned len );
	void append ( const std::string *str );
	void append ( const std::string &str );
	void append ( const char c );
	void append_nl ( );
	explicit output_buffer ( const char *filename, const unsigned buf_size = 64*1024 );
	~output_buffer ( );

private:
	output_buffer ( const output_buffer& );
	output_buffer& operator=( const output_buffer& );
};

#endif
