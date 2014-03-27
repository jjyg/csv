CC=g++
CCOPTS=-W -Wall -O2
LDOPTS=-s -lz

all: csv csv-aggreg

csv: csv_tool.o csv_reader.o output_buffer.o
	$(CC) $(CCOPTS) -o $@ $+ $(LDOPTS)

csv-aggreg: csv_aggreg.o csv_reader.o output_buffer.o
	$(CC) $(CCOPTS) -o $@ $+ $(LDOPTS)

%.o: %.cpp
	$(CC) $(CCOPTS) -o $@ -c $<

clean:
	rm -f *.o
