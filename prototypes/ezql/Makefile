FS_BIN=/home/pureza/fsharp/bin
FSC=mono $(FS_BIN)/fsc.exe
FSLEX=mono $(FS_BIN)/fslex.exe
FSYACC=mono $(FS_BIN)/fsyacc.exe
REFS=-r FSharp.PowerPack.dll 
FS_FILES=Extensions.fs
FS_FILES+=Ast.fs Parser.fs Lexer.fs Types.fs Eval.fs Oper.fs MapOper.fs
FS_FILES+=Graph.fs Clock.fs Scheduler.fs Dataflow.fs Engine.fs Adapters.fs
#FS_FILES+=Program.fs

all: Ezql

Lexer.fs:
	$(FSLEX) Lexer.fsl

Parser.fs:
	$(FSYACC) Parser.fsy

Ezql: Lexer.fs Parser.fs
	$(FSC) -a $(FS_FILES) $(REFS) -o ezql.dll

clean:
	rm -f Lexer.fs Parser.fs *.exe
