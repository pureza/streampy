#light

open System.IO
open System.Text
open EzqlLexer
open EzqlParser
open EzqlAst

let parse lexbuf =
    EzqlParser.start EzqlLexer.token lexbuf

let rec printProg = function
  | Prog classes -> List.iter printClass classes

and printClass = function
  | Class (name, methods) -> printfn "class %s" name


let file = File.OpenText(Sys.argv.[1])
let lexbuf = Lexing.from_text_reader Encoding.ASCII file
let result = parse lexbuf


printProg result




