#light

open System.IO
open System.Text
open System.Collections.Generic
open EzqlLexer
open EzqlParser
open EzqlAst
open Eval

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

let file = File.OpenText(Sys.argv.[1])
let ast = parse file

// Initial environment
let env = [("tempreadings", Stream EzQL.Stream.empty)]
let res = 
  match ast with
  | Prog exprs -> List.map (eval env) exprs
  
let tempreadings = 
    match lookup env "tempreadings" with
    | Stream stream -> stream
    | _ -> failwithf "error"

tempreadings.OnUpdate printEvent

let anEvent = Dictionary<string, value>()
anEvent.["timestamp"] <- Integer 0
anEvent.["temperature"] <- Integer 30
tempreadings.add anEvent


System.Console.ReadLine() |> ignore




