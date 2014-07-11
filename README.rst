csv is a command-line tool to handle RFC4180 formatted streams.

It is designed to handle very large files with minimal CPU/memory overhead.

The general mode of operation is:

  ./csv [options] <mode> <mode parameters> [input file1 [input file 2 ...]]

Mode parameters usually involve column descriptors ; those can be words (as taken from the 1st "header" row of the file) or integers (1st column = 0).
Column names are case-insensitive.

With no input file, data is read from stdin.


Options
=======

  -V  show program version and exit
  -h  show help message and exit
  -o <outfile>  output to a specified file (default = stdout)
  -s  field separator (default = ',')
  -S  output field separator (default = same as -s) -- should only be used with mode 'select'
  -q  quote character (default = '"')
  -L <len>  maximum line length (default = 64*1024 bytes)
  -H  do not try to parse input first line as a header


Modes
=====

Each mode is described here, with the commandline shortcut in parenthesis.

select (s)
----------

From one or more input files, generate an output file holding only the specified columns. Columns names may be duplicated, in this case the column is duplicated at the specified positions.

  csv select row4,row12 foo.csv

Column ranges are allowed (syntax = start-end) ; start and end are optional (defaults to 0 and last).

  csv s row4,row10-row15

With the special option -u, columns specified explicitely are excluded from the ranges. This allows for reordering of columns:

  # outputs row3,row1,row2,row4,row5

  csv s -u row3,-

This mode can be used to transcode files with different column separators:

  csv select 0- -s '\t' -S ',' foo.tsv -o foo.csv

It is the only mode that will handle changing column separator correctly, by quoting every field ; other modes will leave quoting untouched (or ignore output_separator altogether) and will corrupt the file if one unquoted input field contains the output separator.

Select will reorder multiple input file columns so they are coherent ; eg columns with the same name are moved to be at the same position.

  csv -- select - csv1.csv csv2.csv


deselect (d)
------------

Same as select, but discard the specified columns.

  csv deselect row2


grepcol (grep, g)
-----------------

Generate a CSV containing only rows for which a given field (unescaped) matches a regex.
The regex style is "extended posix", ie regcomp(3) with REG_EXTENDED.

  csv grepcol row2=foo.*bar

With many selectors, show all rows for which any of the fields matches its regex (ie "or"):

  csv grep row2=foo.*bar,row4=^moo

To select lines for which many conditions are true at the same time ("and"), pipe several commands:

  csv g row2=foo.*bar | csv g row4=^moo

Two options may change the behavior of this mode.
To invert the matching (ie display lines where no field match its regex), use -v.
To do case-insensitive matches, use -i.

  csv grep -v -i row2=foo.*bar


fgrepcol (fgrep, f)
-------------------

Generate a CSV containing only rows for which a given field (unescaped) appears in a file.
The target file is read, each line is added to a dictionnary, and each csv row has its field matched against the dictionnary.
The field must match exactly the line of the file ; no regexp / substring / whatever matching is done here.

  csv fgrepcol row1=./some_wordlist_one_per_line

The options -v and -i are available here. The same stuff as grepcol applies, regarding the "or" and "and" boolean operations.

The wordlist is stored in a C++ unordered_set ; so it should have good matching performances, but it may not scale to very large wordlists.


concat (c)
----------

Generate a CSV containing all the original columns, plus a new one labeled 'concat' whose value is, for each row, the concatenation of the specified fields. To add fixed values (eg separator chars), use a chain with addcol.

  csv concat f1,f2 foo.csv
  csv s - filelist.csv | csv addcol sep='/' | csv concat dirname,sep,filename | csv deselect sep


rename
------

Change the name of the columns.

  csv rename row1=foo,row2=bar

Can be used to add a header line to a headerless CSV:

  csv rename -H 0=row1,1=row2,2=blarg


addcol (a)
----------

Prepend one column with a fixed value to each row.

  csv addcol newfirstcol=somevalue

With many column names, keep the order when prepending:

  csv a newcol1=v1,newcol2=v2


extract (e)
-----------

Output the unescaped value of the specified column.

The output is not a CSV. The header line is skipped.

Unquoted values are unchanged, quoted values are shown with the quotes unescaped.

  csv extract row4

With the special option -0, a NUL byte is appended to each field (beware, a field may already contain NUL bytes).

listcol (l)
-----------

List the columns of the input stream. The column names are printed, unescaped, one per line. This mode does not take arguments.

  csv listcol


inspect (i)
-----------

For each csv row, dump the row number (hex) and each row field prepended with the column name. This mode takes no argument.

  csv inspect


rows (r)
--------

Generate a CSV with only a range of the input rows.
The range is inclusive.

  csv rows 12-50 foo.csv
  csv row 4
  csv r 8-


stripheader
-----------

Dump the CSV file without its header line. Shortcut for csv -H rows 1-


decimal (dec)
-------------

Convert a specific field from hexadecimal to decimal.
The conversion is done using 64bit unsigned integers plus the sign, on hexadecimal values starting with '0x'.
Other values, or values where the converslion failed are preserved unchanged.

  csv dec row4

Useful for eg mysql load from file which cannot efficiently convert hexadecimal values.


Input encoding
==============

The program interprets some special UTF BOM markers at the beginning of streams: UTF8, UTF16-BE and UTF16-LE.
The UTF8 BOM is discarded, and when encountering the UTF16 markers, the stream read from this point is transcoded to ASCII-8BIT (0-255). Out of range characters are converted to '?'.
The body of the file is treated as an array of bytes.

The program recognizes the gzip magic (0x1f 0x8b) and handles compressed files accordingly. Compile with -DNO_ZLIB to disable this support.


Limitations
===========

The program does not validate the CSV format of the input files, so that an unquoted field with a quote in the middle does not yield errors. This is a feature.

Only the quote, coma and newline characters are used during parsing, all other characters are passed as-is. This includes NUL bytes.

Most modes of operation do not handle well multiple input files with varied column ordering. One exception is the 'select' mode, that will reorder subsequent inputs to match the 1st file columns.

Additionaly, most modes (except select) will not discard the header line of subsequent files from the input.

The maximum line length is specified when starting the program, it may be overriden with the '-L' switch. It specifies the maximum row length in bytes.


License
=======

Copyright 2013 Yoann Guillot <john-git@ofjj.net>

This work is free. You can redistribute it and/or modify it under the terms of the Do What The Fuck You Want To Public License, Version 2, as published by Sam Hocevar.

See http://www.wtfpl.net/ for more details.


Hacking
=======

The code is designed to be fast and efficient. This impacts readability.

Common code paths are expected to be fast and straightforward, with weird corner-cases handled with slower code.

Modes setup may involve complex structure construction, in order to reduce later per-row CPU cost.

The memory footprint of the program does not depend on the size of the input files, it is designed to handle infinite streams.

Memory allocation / copying are generally avoided: the input is read in big chunks of memory, and from then on only pointers into that chunk are manipulated.

