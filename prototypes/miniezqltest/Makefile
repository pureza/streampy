FS_BIN=/home/pureza/fsharp/bin
FSC=mono $(FS_BIN)/fsc.exe
EZQL_PATH=../ezql
REFS=-r FSharp.PowerPack.dll -r $(EZQL_PATH)/ezql.dll 
FS_FILES=Test.fs Test?*.fs Program.fs

all: EzqlTest

EzqlTest:
	$(FSC) $(FS_FILES) $(REFS)
	cp $(EZQL_PATH)/ezql.dll .

clean:
	rm -f *.exe
