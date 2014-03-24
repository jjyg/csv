#ifndef CSVREADER_H
#define CSVREADER_H

class line_reader;

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
	void trim_newlines ( );

public:
	bool failed_to_open ( ) const;

	// return true if no more data is available from input_lines
	bool eos ( ) const;

	// reset cur_field_offset to 0, so that subsequent read_csv_field() re-output the current row fields
	void reset_cur_field_offset ( );

	// line_max is passed to the line_reader, it is also the limit for a full csv row (that may span many lines)
	explicit csv_reader ( const char *filename, const char sep = ',', const char quot = '"', const unsigned line_max = 64*1024 );
	~csv_reader ( );

	// read one line from input_lines
	// invalidates previous read_csv_field pointers
	// return false after EOF
	bool fetch_line ( );

	// read one csv field from the current line
	// returns false if no more fields are available in the line, or if there is a syntax error (unterminated quote, end quote followed by neither a quote nor a separator)
	// returns a pointer to the line start, the offset of the current field, and its length
	// the 'field_offset' returned by previous calls for the same line is still valid relative to the new 'line_start' (which may change if one field crosses a line boundary, in that case the lines are copied into an internal buffer)
	bool read_csv_field ( char* *line_start, unsigned *field_offset, unsigned *field_length );

	// same as read_csv_field ( line_start, field_offset, field_length ) with simpler args
	// returned values are only valid until the next call to this function (with the 3-args version, it is valid until fetch_line())
	bool read_csv_field ( char* *field_start, unsigned *field_length );

	// return an unescaped csv field
	// if unescaped is not NULL, fill it with unescaped data (data appended, string should be empty on call)
	// if unescaped is NULL, and field has no escaped quote, only update field_start and field_length to reflect unescaped data (zero copy)
	// if unescaped is NULL, and field has escaped quotes, allocate a new string and fill it with unescaped data. Caller should free it.
	// returns a pointer to unescaped
	// on return, if unescaped is not NULL, field_start and field_length are undefined.
	std::string* unescape_csv_field ( char* *field_start, unsigned *field_length, std::string* unescaped = NULL ) const;

	static std::string escape_csv_string ( const std::string &str, const char quot = '"' );

	// return the escaped version of an unescaped string
	std::string escape_csv_field ( const std::string &str ) const;

	// parse the current csv line (after a call to fetch_line())
	// return a pointer to a vector of unescaped strings, should be delete by caller
	std::vector<std::string>* parse_line ( );

	// read raw data (dont mix with read_*)
	void read ( char* *ptr, unsigned *len );

private:
	csv_reader ( const csv_reader& );
	csv_reader& operator=( const csv_reader& );
};

#endif
