#light

open System.IO
open System.Text
open EzqlLexer
open EzqlParser
open EzqlAst

let parse file =
    let lexbuf = Lexing.from_text_reader Encoding.ASCII file
    try
        EzqlParser.start EzqlLexer.token lexbuf
    with e ->
        let pos = lexbuf.EndPos
        failwithf "Error near line %d, character %d\n" pos.Line pos.Column

let printTokens file =
    let lexbuf = Lexing.from_text_reader Encoding.ASCII file
    while not lexbuf.IsPastEndOfStream do
        printfn "%A" (EzqlLexer.token lexbuf)

let rec printProg = function
  | Prog classes -> List.iter printClass classes

and printClass = function
  | Class (name, methods) -> printfn "class %s" name;
                             List.iter printMethods methods
and printMethods = function
  | Method (name, args, code) -> printfn " - %s()" name


let file = File.OpenText(Sys.argv.[1])
let result = parse file
printProg result

printTokens(File.OpenText(Sys.argv.[1]))




