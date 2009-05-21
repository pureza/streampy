﻿#light

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
  fst (List.fold_left types (Map.empty, Map.empty) ast)

let dataflowAnalysis types ast =
  let g = Graph.empty
  let env = Map.empty
  let roots = []
  let env', types', g', roots' = List.fold_left dataflow (env, types, g, roots) ast

//  Graph.Viewer.display g' (fun v info -> info.Uid)
  let operators = Dataflow.makeOperNetwork g' (Graph.nodes g') id Map.empty
  Map.fold_left (fun acc k v -> Map.add k operators.[v.Uid] acc) Map.empty env'


let compile code =
    let ast = parse code
    let types = typeCheck ast
    let ast' = rewrite types ast
    dataflowAnalysis types ast'

let mainLoop () =
    let virtualClock = Scheduler.clock () :?> VirtualClock
    while virtualClock.HasNext () do
        virtualClock.Step ()

let reset () = Scheduler.reset ()

let now () = Scheduler.clock().Now
