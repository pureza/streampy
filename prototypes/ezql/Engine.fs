#light

open Scheduler
open Clock
open Ast
open Types
open TypeChecker
open Rewrite
open Oper
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
  let initEnv = Map.of_list ["ticks", TyStream (TyRecord (Map.of_list ["timestamp", TyInt]))]
  List.fold types initEnv ast

let compile code =
    let ast = parse code
    let types = typeCheck ast
    let ast' = rewrite types ast
    dataflowAnalysis types ast', types

let mainLoop () =
    let virtualClock = Scheduler.clock () :?> VirtualClock
    while virtualClock.HasNext () do
        virtualClock.Step ()

let reset () = Scheduler.reset ()
               theForwardDeps.Reset ()

let now () = Scheduler.clock().Now
