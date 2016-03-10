csv-aggreg is a command-line tool to handle RFC4180 formatted streams.

It is designed to aggregate multiple CSV files and generate a new one containing aggregated data from the inputs.

Usage:

  ./csv-aggreg [options] <aggregation directives> [input file1 [input file 2 ...]]

The aggregation directives describe the output columns, they may be based on reprocessing of input columns or new column creation from scratch.

The aggregation directives are based on the input file column names (case insensitive). All input file should have the same column structure.

For input encoding, limitation, license and other information, please refer to the main README file.

Options
=======

  -V  show program version and exit
  -h  show help message and exit
  -o <outfile>  output to a specified file (default = stdout)
  -L <len>  maximum line length (default = 64*1024 bytes)
  -m  input files are already outputs of csv-aggreg with the same specification
  -d <dir>  use a directory to store temporary files


The -m mode allows further processing from already processed aggregation tasks, this allows to distribute the work across many machines and then to create a final output based on the intermediary distributed work. In this mode, all input files should be the output of csv-aggreg, no raw input file is allowed. For each invocation of this mode in a batch run, the aggregation string must be identical.

The -d option allows the program to use temporary files on-disk, so that it may handle more data than would fit in available RAM. However this mode of operation is extremely slow. This mode is only needed if the output file is to be larger than approx. 2/3 of the available RAM. If possible, avoid using this option.


Aggregation functions
=====================

Each aggregation function is described here.

Each output column will use one of those functions.

The output column name may be specified with a 'output_col_name=aggreg_func(input_col_name)' syntax ; multiple aggregation directives are separated by comas on the commandline.

The aggregation key is the set of output columns generated through the 'str()' or 'downcase()' functions.
It is applied for each input line. If the resulting key already exists, the associated output line is updated according to the other aggregation functions. If not, a new output line is initialized with this key.


str(col)
--------

Use the value of this column as an aggregation key. It is conserved as-is.

Simply specifying an input column name as aggregation mode is a shortcut for this mode.


downcase(col)
-------------

Use the downcase value of the column as aggregation key. Similar to 'str', plus case-insensitive.


top20(col)
----------

This output column will contain the 20 first different values found in aggregated lines, separated by comas.

After the first 20 are collected, further values are discarded.


min(col), max(col)
------------------

Retain the minimal (maximal) numerical value found for all aggregated lines with the same key.


minstr(col), maxstr(col)
------------------------

Retain the minimal (maximal) value for aggregated lines, in lexicographic order.


count()
-------

Show the number of lines having the same aggregation key.


Exemples
========

input file:

 $ cat in.csv
 "a","b","c"
 "a1","b1","c1"
 "a1","b2","c2"
 "a2","b3","c3"


 $ ./csv-aggreg 'a=str(a),min_b=minstr(b),top_c=top20(c)' in.csv
 "a","min_b","top_c"
 "a1","b1","c1,c2"
 "a2","b3","c3"

 $ ./csv-aggreg 'a,nr=count(),min_b=minstr(b),max_b=maxstr(b)' in.csv -o out.csv
 "a","nr","min_b","max_b"
 "a1",2,"b1","b2"
 "a2",1,"b3","b3"

 $ ./csv-aggreg -m 'a,nr=count(),min_b=minstr(b),max_b=maxstr(b)' out.csv out.csv out.csv
 "a","nr","min_b","max_b"
 "a1",6,"b1","b2"
 "a2",3,"b3","b3"

