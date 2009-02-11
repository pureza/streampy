FS_BIN=/home/pureza/fsharp/bin
FSC=mono $(FS_BIN)/fsc.exe
FSLEX=mono $(FS_BIN)/fslex.exe
FSYACC=mono $(FS_BIN)/fsyacc.exe
REFS=FSharp.PowerPack.dll
FS_FILES=EzqlAst.fs EzqlParser.fs EzqlLexer.fs EventQueue.fs
FS_FILES+=Stream.fs Window.fs Types.fs Eval.fs Ezql.fs

all: Ezql

EzqlLexer.fs:
	$(FSLEX) EzqlLexer.fsl

EzqlParser.fs:
	$(FSYACC) EzqlParser.fsy

Ezql: EzqlLexer.fs EzqlParser.fs
	$(FSC) $(FS_FILES) -r $(REFS)

clean:
	rm -f EzqlLexer.fs EzqlParser.fs *.exe
