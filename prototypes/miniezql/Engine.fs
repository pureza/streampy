#light

open Scheduler
open Clock
open Ast
open Types
open TypeChecker
open Rewrite
open Oper
open Clock
open Graph
open Dataflow

let parse code =
    // let lexbuf = Lexing.from_text_reader Encoding.ASCII file
    let lexbuf = Lexing.from_string code
    try
        Parser.TopLevel Lexer.token lexbuf
    with e ->
        let pos = lexbuf.EndPos
        failwithf "Error near line %d, character %d\n" (pos.Line + 1) pos.Column


let typeCheck ast =
  let initialEnv = Map.of_list [ for f in Primitives.primitiveFunctions -> (f.Name, dg (TyPrimitive f.TypeCheck)) ]
  List.fold types initialEnv ast


let compile code =
    let ast = parse code |> rewrite1
    let types = typeCheck ast
    let ast' = rewrite2 ast
    dataflowAnalysis types ast', types


let mainLoop () =
    let virtualClock = Scheduler.clock () :?> VirtualClock
    while virtualClock.HasNext () do
        virtualClock.Step ()

let reset () = Scheduler.reset ()
               theForwardDeps.Reset ()

let now () = Scheduler.clock().Now
