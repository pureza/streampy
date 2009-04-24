FS_BIN=/home/pureza/fsharp/bin
FSC=mono $(FS_BIN)/fsc.exe
FSLEX=mono $(FS_BIN)/fslex.exe
FSYACC=mono $(FS_BIN)/fsyacc.exe
REFS=-r FSharp.PowerPack.dll 
FS_FILES=Extensions.fs
FS_FILES+=Ast.fs Parser.fs Lexer.fs Program.fs
FS_FILES+=#Types.fs Clock.fs TypeChecker.fs Rewrite.fs
#FS_FILES+=Eval.fs Oper.fs Scheduler.fs
#FS_FILES+=CommonOpers.fs
#FS_FILES+=AggregateOpers.fs DictOpers.fs
#FS_FILES+=Graph.fs Dataflow.fs Engine.fs Adapters.fs
#FS_FILES+=Program.fs

all: Ezql

Lexer.fs:
	$(FSLEX) Lexer.fsl

Parser.fs:
	$(FSYACC) Parser.fsy

Ezql: Lexer.fs Parser.fs
	$(FSC) $(FS_FILES) $(REFS)

clean:
	rm -f Lexer.fs Parser.fs *.exe
