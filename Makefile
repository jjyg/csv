all: csv csv-aggreg

csv: csv_tool.o csv_reader.o output_buffer.o
	g++ -W -Wall -O2 -s -o $@ $+ -lz

csv-aggreg: csv_aggreg.o csv_reader.o output_buffer.o
	g++ -W -Wall -O2 -s -o $@ $+ -lz

%.o: %.cpp
	g++ -W -Wall -O2 -o $@ -c $<

clean:
	rm -f *.o
