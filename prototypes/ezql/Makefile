FS_BIN=/home/pureza/fsharp/bin
FSC=mono $(FS_BIN)/fsc.exe
FSLEX=mono $(FS_BIN)/fslex.exe
FSYACC=mono $(FS_BIN)/fsyacc.exe
REFS=FSharp.PowerPack.dll

all: Ezql

EzqlLexer.fs:
	$(FSLEX) EzqlLexer.fsl

EzqlParser.fs:
	$(FSYACC) EzqlParser.fsy

Ezql: EzqlLexer.fs EzqlParser.fs
	$(FSC) EzqlAst.fs EzqlParser.fs EzqlLexer.fs Ezql.fs -r $(REFS)

clean:
	rm -f EzqlLexer.fs EzqlParser.fs *.exe
