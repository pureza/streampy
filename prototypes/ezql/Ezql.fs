#light

open System
open System.IO
open System.Text
open System.Collections.Generic
open System.Threading
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
let env = [("tempreadings", VStream (Stream ()))]
let res = 
  match ast with
  | Prog exprs -> List.map (eval env) exprs

List.iter (fun v -> match v with
                    | VStream stream -> stream |> printStream
                    | VContinuousValue v -> v |> printContValue
                    | _ -> failwith "The result of a query must be a IStream")
          res
  
let tempreadings = 
    match lookup env "tempreadings" with
    | VStream stream -> stream
    | _ -> failwithf "error"

tempreadings |> printStream

let anEvent = (new Types.Event (DateTime.Now, Map.of_list [("temperature", VInteger 30)])) :> IEvent

tempreadings.Add anEvent

Thread.Sleep(1000)

let anotherEvent = (new Types.Event (DateTime.Now, Map.of_list [("temperature", VInteger 50)])) :> IEvent

tempreadings.Add anotherEvent

System.Console.ReadLine() |> ignore




