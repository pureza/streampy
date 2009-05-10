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
        Parser.toplevel Lexer.token lexbuf
    with e ->
        let pos = lexbuf.EndPos
        failwithf "Error near line %d, character %d\n" (pos.Line + 1) pos.Column



let typeCheck ast =
  List.fold_left types Map.empty ast


let dataflowAnalysis types ast =
  let g = Graph.empty
  let env = Map.empty
  let roots = []
  let env', g', roots' = List.fold_left dataflow (env, g, roots) ast

  //Graph.Viewer.display g' (fun v info -> info.Uid)
  let rootUids = List.map (fun name -> env'.[name].Uid) roots'
  let operators = Dataflow.makeOperNetwork g' rootUids id
  Map.fold_left (fun acc k v -> Map.add k operators.[v.Uid] acc) Map.empty env'


let compile code =
    let ast = parse code
    let ast' = rewrite (typeCheck ast) ast
    dataflowAnalysis (typeCheck ast') ast'

let mainLoop () =
    let virtualClock = Scheduler.clock () :?> VirtualClock
    while virtualClock.HasNext () do
        virtualClock.Step ()

let reset () = Scheduler.reset ()

let now () = Scheduler.clock().Now
