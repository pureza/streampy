#light

open System
open System.IO
open System.Text
open System.Collections.Generic
open EzqlLexer
open EzqlParser
open EzqlAst
open Eval
open Types

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
let env = [("tempreadings", value.Stream (Stream.Stream ()))]
let res = 
  match ast with
  | Prog exprs -> List.map (eval env) exprs

List.iter (fun v -> match v with
                    | Stream stream -> stream |> Stream.print
                    | _ -> failwith "The result of a query must be a IStream")
          res
  
let tempreadings = 
    match lookup env "tempreadings" with
    | Stream stream -> stream
    | _ -> failwithf "error"

tempreadings |> Stream.print

let anEvent = (new Types.Event ()) :> IEvent
anEvent.Timestamp <- DateTime.Now
anEvent.["temperature"] <- Integer 30

tempreadings.Add anEvent

System.Console.ReadLine() |> ignore




