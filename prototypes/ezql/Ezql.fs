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
  | Prog queries -> List.iter printQuery queries

and printQuery = function
  | MethodCall (target, Identifier methodName, args) -> printfn "method %s" methodName
                                                        List.iter printArgs args
  | _ -> printfn "Something else"

and printArgs = function
  | Lambda (parameters, expr) -> printfn "Lambda %A" parameters
  | _ -> printfn "Something else"

let file = File.OpenText(Sys.argv.[1])
let result = parse file
printProg result
printfn "%A" result

printTokens(File.OpenText(Sys.argv.[1]))




