#light

open System.IO
open System.Text
open EzqlLexer
open EzqlParser
open EzqlAst

let parse lexbuf =
    try
        EzqlParser.start EzqlLexer.token lexbuf
    with e ->
        let pos = lexbuf.EndPos
        failwithf "Error near line %d, character %d\n" pos.Line pos.Column


let rec printProg = function
  | Prog classes -> List.iter printClass classes

and printClass = function
  | Class (name, methods) -> printfn "class %s" name;
                             List.iter printMethods methods
and printMethods = function
  | Method (name, args, code) -> printfn " - %s()" name


let file = File.OpenText(Sys.argv.[1])
let lexbuf = Lexing.from_text_reader Encoding.ASCII file
let result = parse lexbuf


printProg result




